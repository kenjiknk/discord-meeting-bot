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
SAMPLE_WIDTH = 2
BYTES_PER_SEC = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH  # 192000

STREAM_CHUNK_SECS = int(os.getenv("STREAM_CHUNK_SECS", "20"))
STREAM_CHUNK_BYTES = BYTES_PER_SEC * STREAM_CHUNK_SECS

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

RECORDINGS_DIR = Path("recordings")
RECORDINGS_DIR.mkdir(exist_ok=True)

active_recordings: dict = {}


@bot.event
async def on_ready():
    print(f"Bot online: {bot.user} (ID: {bot.user.id})")
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
        await asyncio.sleep(1)

    channel = ctx.author.voice.channel
    try:
        await channel.connect(cls=voice_recv.VoiceRecvClient, timeout=60.0)
        print(f"[DEBUG] conectado em {channel.name}")
        await ctx.send(f"Entrei em **{channel.name}**. Use `!record` para iniciar a gravação.")
    except asyncio.TimeoutError:
        await ctx.send("Timeout ao conectar ao canal de voz. Tente `!join` novamente em alguns segundos.")
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

    channel_id = os.getenv("SUMMARY_CHANNEL_ID")
    transcript_channel = bot.get_channel(int(channel_id)) if channel_id else ctx.channel

    state = {
        "vc": ctx.voice_client,
        "session_dir": session_dir,
        "session_date": datetime.now(),
        "pcm_buffers": defaultdict(bytearray),
        "processed": defaultdict(int),
        "transcript_accum": defaultdict(list),
        "username_cache": {},
        "transcript_channel": transcript_channel,
        "stream_task": None,
        "_dbg_packets": defaultdict(int),
        "_dbg_null_user": 0,
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

    state["stream_task"] = asyncio.create_task(
        _streaming_task(ctx.guild.id)
    )

    await transcript_channel.send(
        f"🔴 **Gravação iniciada** — {state['session_date'].strftime('%d/%m/%Y %H:%M')}\n"
        f"Canal de voz: **{ctx.author.voice.channel.name}**\n"
        f"Transcrição em tempo real abaixo:"
    )
    if transcript_channel != ctx.channel:
        await ctx.send("Gravação iniciada. Transcrevendo em tempo real. Use `!stop` para finalizar.")


async def _streaming_task(guild_id: int):
    while guild_id in active_recordings:
        await asyncio.sleep(STREAM_CHUNK_SECS)
        if guild_id not in active_recordings:
            break
        state = active_recordings[guild_id]
        await _process_new_chunks(state)


async def _resolve_username(state: dict, user_id: int) -> str:
    if user_id not in state["username_cache"]:
        try:
            user = bot.get_user(user_id) or await bot.fetch_user(user_id)
            state["username_cache"][user_id] = user.display_name if user else f"usuario_{user_id}"
        except discord.NotFound:
            state["username_cache"][user_id] = f"usuario_{user_id}"
    return state["username_cache"][user_id]


async def _process_new_chunks(state: dict, post_to_channel: bool = True) -> None:
    lines = []
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
            username = await _resolve_username(state, user_id)
            lines.append(f"🎙️ **{username}**: {text}")
            print(f"[STREAM] user={user_id} chunk transcrito: {text[:60]}...")

    if lines and post_to_channel:
        channel = state["transcript_channel"]
        msg = "\n".join(lines)
        try:
            await channel.send(msg)
        except discord.HTTPException as e:
            print(f"[WARN] Falha ao postar transcrição: {e}")


async def _transcribe_remaining(state: dict) -> None:
    """Transcreve áudio restante (< STREAM_CHUNK_BYTES) e posta no canal."""
    lines = []
    for user_id, pcm_data in state["pcm_buffers"].items():
        remaining_start = state["processed"][user_id]
        remaining = bytes(pcm_data[remaining_start:])
        if len(remaining) < 960:
            continue

        text = await asyncio.to_thread(transcribe_pcm, remaining)
        text = text.strip()
        if text:
            state["transcript_accum"][user_id].append(text)
            username = await _resolve_username(state, user_id)
            lines.append(f"🎙️ **{username}**: {text}")

    if lines:
        channel = state["transcript_channel"]
        try:
            await channel.send("\n".join(lines))
        except discord.HTTPException as e:
            print(f"[WARN] Falha ao postar transcrição final: {e}")


@bot.command(name="stop")
async def stop_recording(ctx: commands.Context):
    if ctx.guild.id not in active_recordings:
        await ctx.send("Não há gravação em andamento.")
        return

    state = active_recordings.pop(ctx.guild.id)

    if state["stream_task"]:
        state["stream_task"].cancel()
    state["vc"].stop_listening()

    null_pkts = state["_dbg_null_user"]
    named_pkts = {uid: cnt for uid, cnt in state["_dbg_packets"].items()}
    print(f"[DEBUG] sessão encerrada — pacotes user=None: {null_pkts}, por usuário: {named_pkts}")
    for uid, buf in state["pcm_buffers"].items():
        print(f"[DEBUG] buffer user={uid}: {len(buf)/1024:.1f}kB ({len(buf)/192000:.1f}s de áudio)")

    await ctx.send("Finalizando transcrição...")

    await _transcribe_remaining(state)

    save_wav = os.getenv("SAVE_WAV", "false").lower() == "true"
    transcript_parts: list[tuple[str, str]] = []

    for user_id, chunks in state["transcript_accum"].items():
        if not chunks:
            continue
        username = await _resolve_username(state, user_id)
        full_text = " ".join(chunks)
        transcript_parts.append((username, full_text))

        if save_wav:
            wav_path = state["session_dir"] / f"{username}_{user_id}.wav"
            _write_wav(wav_path, bytes(state["pcm_buffers"][user_id]))

    if not transcript_parts:
        await state["transcript_channel"].send("⚫ Gravação encerrada. Nenhum áudio detectado.")
        await ctx.send("Nenhum áudio detectado.")
        return

    raw = "\n\n".join(f"[{u}]\n{t}" for u, t in transcript_parts)
    (state["session_dir"] / "transcript.txt").write_text(raw, encoding="utf-8")

    jsonl_filename = f"meeting_{state['session_date'].strftime('%Y%m%d_%H%M%S')}.jsonl"
    jsonl_path = state["session_dir"] / jsonl_filename
    await asyncio.to_thread(
        generate_jsonl, transcript_parts, state["session_date"], str(jsonl_path)
    )

    target = state["transcript_channel"]
    await target.send(
        f"⚫ **Gravação encerrada** — {state['session_date'].strftime('%d/%m/%Y %H:%M')}"
        f" | {len(transcript_parts)} participante(s)\n\n"
        f"Para gerar o resumo via Claude Code:\n"
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
