import os
import wave
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import discord
from discord.ext import commands, voice_recv
from dotenv import load_dotenv
import dave_patch

logging.basicConfig(level=logging.INFO)

dave_patch.apply()

from transcriber import load_model, transcribe_pcm
from batch_generator import generate_jsonl

load_dotenv()

SAMPLE_RATE = 48000
CHANNELS = 2
SAMPLE_WIDTH = 2  # 16-bit PCM
BYTES_PER_SEC = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH  # 192000

# Transcreve chunks de 20s enquanto grava — como Scripty
STREAM_CHUNK_SECS = int(os.getenv("STREAM_CHUNK_SECS", "20"))
STREAM_CHUNK_BYTES = BYTES_PER_SEC * STREAM_CHUNK_SECS

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

RECORDINGS_DIR = Path("recordings")
RECORDINGS_DIR.mkdir(exist_ok=True)

# guild_id -> dict com estado da sessão
active_recordings: dict = {}


@bot.event
async def on_ready():
    print(f"Bot online: {bot.user} (ID: {bot.user.id})")
    # Pré-carrega o modelo no startup — elimina cold start no primeiro !stop
    await asyncio.to_thread(load_model)


@bot.command(name="join")
async def join_channel(ctx: commands.Context):
    if not ctx.author.voice:
        await ctx.send("Você precisa estar em um canal de voz primeiro.")
        return
    if ctx.voice_client and ctx.voice_client.is_connected():
        await ctx.send("Já estou em um canal. Use `!leave` para sair primeiro.")
        return
    if ctx.voice_client:
        await ctx.voice_client.disconnect(force=True)
        await asyncio.sleep(1)  # aguarda o Discord encerrar a sessão antes de reconectar

    channel = ctx.author.voice.channel
    try:
        await channel.connect(cls=voice_recv.VoiceRecvClient, timeout=60.0)
        print(f"[DEBUG] conectado em {channel.name}")
        await ctx.send(f"Entrei em **{channel.name}**. Use `!record` para iniciar a gravação.")
    except asyncio.TimeoutError:
        await ctx.send("Timeout ao conectar ao canal de voz. Tente `!join` novamente em alguns segundos.")
        # Só desconecta se não tiver gravação ativa — timeout pode ter disparado tarde,
        # depois de um !join + !record bem-sucedido em outra tentativa.
        if ctx.guild.id not in active_recordings and ctx.voice_client:
            await ctx.voice_client.disconnect(force=True)
    except Exception as e:
        await ctx.send(f"Não consegui entrar no canal: {e}")
        if ctx.guild.id not in active_recordings and ctx.voice_client:
            await ctx.voice_client.disconnect(force=True)


@bot.command(name="record")
async def start_recording(ctx: commands.Context):
    vc = ctx.voice_client
    print(f"[DEBUG] !record — voice_client={vc}, is_connected={vc.is_connected() if vc else None}")
    if vc is None:
        await ctx.send("Não estou conectado. Use `!join` e aguarde a confirmação.")
        return
    if ctx.guild.id in active_recordings:
        await ctx.send("Já estou gravando. Use `!stop` para parar.")
        return

    session_dir = RECORDINGS_DIR / datetime.now().strftime("session_%Y%m%d_%H%M%S")
    session_dir.mkdir(parents=True, exist_ok=True)

    state = {
        "vc": ctx.voice_client,
        "session_dir": session_dir,
        "session_date": datetime.now(),
        "pcm_buffers": defaultdict(bytearray),     # áudio ainda não transcrito
        "processed": defaultdict(int),              # bytes já transcrito por user
        "transcript_accum": defaultdict(list),      # chunks transcritos
        "stream_task": None,
        "_dbg_packets": defaultdict(int),           # total de pacotes recebidos por user_id
        "_dbg_null_user": 0,                        # pacotes com user=None
    }
    def on_audio(user, data: voice_recv.VoiceData):
        if user is None:
            state["_dbg_null_user"] += 1
            if state["_dbg_null_user"] % 500 == 1:
                print(f"[DEBUG] on_audio: {state['_dbg_null_user']} pacotes com user=None (SSRC não mapeado)")
            return
        state["_dbg_packets"][user.id] += 1
        if state["_dbg_packets"][user.id] % 500 == 1:
            total_bytes = len(state["pcm_buffers"][user.id])
            print(f"[DEBUG] on_audio: user={user.id} pacotes={state['_dbg_packets'][user.id]} buffer={total_bytes/1024:.1f}kB")
        state["pcm_buffers"][user.id] += data.pcm

    if not vc.is_connected():
        try:
            await asyncio.wait_for(vc._connected.wait(), timeout=15.0)
        except asyncio.TimeoutError:
            await ctx.send("Timeout aguardando conexão de voz. Use `!join` novamente.")
            return

    try:
        vc.listen(voice_recv.BasicSink(on_audio))
    except Exception as e:
        await ctx.send(f"Erro ao iniciar captura de áudio: {e}")
        print(f"[ERROR] listen() falhou: {e}")
        return

    active_recordings[ctx.guild.id] = state

    # Inicia task de streaming em background
    state["stream_task"] = asyncio.create_task(
        _streaming_task(ctx.guild.id, ctx)
    )

    await ctx.send("Gravação iniciada. Transcrevendo em tempo real. Use `!stop` para finalizar.")


async def _streaming_task(guild_id: int, ctx: commands.Context):
    """Transcreve chunks de STREAM_CHUNK_SECS enquanto a gravação está ativa."""
    while guild_id in active_recordings:
        await asyncio.sleep(STREAM_CHUNK_SECS)
        if guild_id not in active_recordings:
            break

        state = active_recordings[guild_id]
        await _process_new_chunks(state)


async def _process_new_chunks(state: dict) -> None:
    """Transcreve qualquer chunk de STREAM_CHUNK_BYTES ainda não processado."""
    for user_id, pcm_data in list(state["pcm_buffers"].items()):
        unprocessed_start = state["processed"][user_id]
        unprocessed = pcm_data[unprocessed_start:]

        if len(unprocessed) < STREAM_CHUNK_BYTES:
            continue

        chunk = bytes(unprocessed[:STREAM_CHUNK_BYTES])
        state["processed"][user_id] += STREAM_CHUNK_BYTES

        text = await asyncio.to_thread(transcribe_pcm, chunk)
        text = text.strip()

        if text:
            state["transcript_accum"][user_id].append(text)
            print(f"[STREAM] user={user_id} chunk transcrito: {text[:60]}...")


@bot.command(name="stop")
async def stop_recording(ctx: commands.Context):
    if ctx.guild.id not in active_recordings:
        await ctx.send("Não há gravação em andamento.")
        return

    state = active_recordings.pop(ctx.guild.id)

    # Para o streaming e o listener
    if state["stream_task"]:
        state["stream_task"].cancel()
    state["vc"].stop_listening()

    null_pkts = state["_dbg_null_user"]
    named_pkts = {uid: cnt for uid, cnt in state["_dbg_packets"].items()}
    print(f"[DEBUG] sessão encerrada — pacotes user=None: {null_pkts}, por usuário: {named_pkts}")
    for uid, buf in state["pcm_buffers"].items():
        print(f"[DEBUG] buffer user={uid}: {len(buf)/1024:.1f}kB ({len(buf)/192000:.1f}s de áudio)")

    await ctx.send("Finalizando transcrição...")

    # Transcreve o restante (áudio que não chegou a 20s)
    for user_id, pcm_data in state["pcm_buffers"].items():
        remaining_start = state["processed"][user_id]
        remaining = bytes(pcm_data[remaining_start:])
        if len(remaining) < 960:
            continue

        text = await asyncio.to_thread(transcribe_pcm, remaining)
        text = text.strip()
        if text:
            state["transcript_accum"][user_id].append(text)

    # Resolve nomes dos usuários
    save_wav = os.getenv("SAVE_WAV", "false").lower() == "true"
    transcript_parts: list[tuple[str, str]] = []

    for user_id, chunks in state["transcript_accum"].items():
        if not chunks:
            continue
        try:
            user = bot.get_user(user_id) or await bot.fetch_user(user_id)
            username = user.display_name if user else f"usuario_{user_id}"
        except discord.NotFound:
            username = f"usuario_{user_id}"

        full_text = " ".join(chunks)
        transcript_parts.append((username, full_text))

        if save_wav:
            wav_path = state["session_dir"] / f"{username}_{user_id}.wav"
            _write_wav(wav_path, bytes(state["pcm_buffers"][user_id]))

    if not transcript_parts:
        await ctx.send("Nenhum áudio detectado.")
        return

    # Salva transcript bruto
    raw = "\n\n".join(f"[{u}]\n{t}" for u, t in transcript_parts)
    (state["session_dir"] / "transcript.txt").write_text(raw, encoding="utf-8")

    # Gera JSONL para Claude Code
    jsonl_filename = f"meeting_{state['session_date'].strftime('%Y%m%d_%H%M%S')}.jsonl"
    jsonl_path = state["session_dir"] / jsonl_filename
    await asyncio.to_thread(
        generate_jsonl, transcript_parts, state["session_date"], str(jsonl_path)
    )

    # Posta no canal configurado
    channel_id = os.getenv("SUMMARY_CHANNEL_ID")
    target = bot.get_channel(int(channel_id)) if channel_id else ctx.channel

    await target.send(
        f"**Reunião gravada** — {state['session_date'].strftime('%d/%m/%Y %H:%M')} "
        f"| {len(transcript_parts)} participante(s)\n"
        f"Para gerar o resumo:\n"
        f"```\nclaude -p \"$(jq -r '.params.messages[0].content' {jsonl_filename})\"\n```",
        file=discord.File(str(jsonl_path), filename=jsonl_filename),
    )


@bot.command(name="leave")
async def leave_channel(ctx: commands.Context):
    if ctx.guild.id in active_recordings:
        state = active_recordings.pop(ctx.guild.id)
        if state["stream_task"]:
            state["stream_task"].cancel()
        state["vc"].stop_listening()

    if ctx.voice_client:
        await ctx.voice_client.disconnect()
        await ctx.send("Até mais!")
    else:
        await ctx.send("Não estou em nenhum canal de voz.")


def _write_wav(path: Path, pcm_data: bytes) -> None:
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm_data)


bot.run(os.environ["DISCORD_TOKEN"])
