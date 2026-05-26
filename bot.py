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
BYTES_PER_SEC = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH

STREAM_CHUNK_SECS = int(os.getenv("STREAM_CHUNK_SECS", "20"))
STREAM_CHUNK_BYTES = BYTES_PER_SEC * STREAM_CHUNK_SECS

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

RECORDINGS_DIR = Path("recordings")
RECORDINGS_DIR.mkdir(exist_ok=True)

active_recordings: dict = {}


@bot.event
async def on_ready():
    print(f"Bot online: {bot.user} (ID: {bot.user.id})")
    await asyncio.to_thread(load_model)

    # Sync instantâneo no guild de teste; global sync pode levar até 1h
    test_guild_id = os.getenv("TEST_GUILD_ID")
    if test_guild_id:
        guild = discord.Object(id=int(test_guild_id))
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
    else:
        synced = await bot.tree.sync()
    print(f"Slash commands sincronizados: {len(synced)}")


@bot.tree.command(name="record", description="Entra no seu canal de voz e inicia a gravação")
async def record_command(interaction: discord.Interaction):
    await interaction.response.defer()

    if not interaction.user.voice:
        await interaction.followup.send("Você precisa estar em um canal de voz primeiro.")
        return

    guild_id = interaction.guild_id
    if guild_id in active_recordings:
        await interaction.followup.send("Já estou gravando. Use `/stop` para parar.")
        return

    voice_channel = interaction.user.voice.channel
    vc = interaction.guild.voice_client

    if vc and vc.is_connected():
        if vc.channel != voice_channel:
            await vc.move_to(voice_channel)
    else:
        if vc:
            await vc.disconnect(force=True)
            await asyncio.sleep(1)
        try:
            vc = await voice_channel.connect(cls=voice_recv.VoiceRecvClient, timeout=60.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("Timeout ao conectar. Tente novamente.")
            return
        except Exception as e:
            await interaction.followup.send(f"Não consegui entrar no canal: {e}")
            return

    session_dir = RECORDINGS_DIR / datetime.now().strftime("session_%Y%m%d_%H%M%S")
    session_dir.mkdir(parents=True, exist_ok=True)

    channel_id = os.getenv("SUMMARY_CHANNEL_ID")
    transcript_channel = bot.get_channel(int(channel_id)) if channel_id else interaction.channel

    state = {
        "vc": vc,
        "session_dir": session_dir,
        "session_date": datetime.now(),
        "voice_channel": voice_channel,
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
            return
        state["_dbg_packets"][user.id] += 1
        if state["_dbg_packets"][user.id] % 500 == 1:
            total_bytes = len(state["pcm_buffers"][user.id])
            print(f"[DEBUG] user={user.id} pacotes={state['_dbg_packets'][user.id]} buffer={total_bytes/1024:.1f}kB")
        state["pcm_buffers"][user.id] += data.pcm

    if not vc.is_connected():
        try:
            await asyncio.wait_for(vc._connected.wait(), timeout=15.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("Timeout aguardando conexão. Use `/record` novamente.")
            return

    try:
        vc.listen(voice_recv.BasicSink(on_audio))
    except Exception as e:
        await interaction.followup.send(f"Erro ao iniciar captura de áudio: {e}")
        return

    active_recordings[guild_id] = state
    state["stream_task"] = asyncio.create_task(_streaming_task(guild_id))

    await transcript_channel.send(
        f"🔴 **Gravação iniciada** — {state['session_date'].strftime('%d/%m/%Y %H:%M')}\n"
        f"Canal de voz: **{voice_channel.name}**\n"
        f"Transcrição em tempo real abaixo:"
    )

    if transcript_channel.id != interaction.channel_id:
        await interaction.followup.send(
            f"Gravação iniciada em **{voice_channel.name}**. "
            f"Transcrição em <#{transcript_channel.id}>."
        )
    else:
        await interaction.followup.send(f"Gravação iniciada em **{voice_channel.name}**.")


@bot.tree.command(name="stop", description="Para a gravação e gera o JSONL para resumo")
async def stop_command(interaction: discord.Interaction):
    await interaction.response.defer()

    if interaction.guild_id not in active_recordings:
        await interaction.followup.send("Não há gravação em andamento.")
        return

    await interaction.followup.send("Finalizando transcrição...")
    await _do_stop(interaction.guild_id)


@bot.tree.command(name="status", description="Mostra o status da gravação atual")
async def status_command(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    if guild_id not in active_recordings:
        await interaction.response.send_message("Nenhuma gravação em andamento.", ephemeral=True)
        return

    state = active_recordings[guild_id]
    duration = datetime.now() - state["session_date"]
    mins = int(duration.total_seconds() // 60)
    secs = int(duration.total_seconds() % 60)
    total_speakers = len([uid for uid, chunks in state["transcript_accum"].items() if chunks])
    total_chunks = sum(len(chunks) for chunks in state["transcript_accum"].values())

    await interaction.response.send_message(
        f"🔴 **Gravando** — {mins:02d}:{secs:02d}\n"
        f"Canal: **{state['voice_channel'].name}**\n"
        f"Participantes com fala: **{total_speakers}** | Chunks transcritos: **{total_chunks}**",
        ephemeral=True,
    )


@bot.tree.command(name="leave", description="Para a gravação (se ativa) e sai do canal de voz")
async def leave_command(interaction: discord.Interaction):
    await interaction.response.defer()

    if interaction.guild_id in active_recordings:
        await _do_stop(interaction.guild_id)

    vc = interaction.guild.voice_client
    if vc:
        await vc.disconnect()
        await interaction.followup.send("Até mais!")
    else:
        await interaction.followup.send("Não estou em nenhum canal de voz.")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    guild_id = member.guild.id
    if guild_id not in active_recordings:
        return

    state = active_recordings[guild_id]
    vc = state["vc"]
    if vc is None or vc.channel is None:
        return

    human_members = [m for m in vc.channel.members if not m.bot]
    if len(human_members) == 0:
        await state["transcript_channel"].send("👤 Canal esvaziou — parando gravação automaticamente.")
        await _do_stop(guild_id)
        if vc.is_connected():
            await vc.disconnect()


async def _do_stop(guild_id: int) -> None:
    if guild_id not in active_recordings:
        return

    state = active_recordings.pop(guild_id)

    if state["stream_task"]:
        state["stream_task"].cancel()
    state["vc"].stop_listening()

    null_pkts = state["_dbg_null_user"]
    named_pkts = {uid: cnt for uid, cnt in state["_dbg_packets"].items()}
    print(f"[DEBUG] sessão encerrada — user=None: {null_pkts}, por usuário: {named_pkts}")

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


async def _streaming_task(guild_id: int):
    while guild_id in active_recordings:
        await asyncio.sleep(STREAM_CHUNK_SECS)
        if guild_id not in active_recordings:
            break
        await _process_new_chunks(active_recordings[guild_id])


async def _resolve_username(state: dict, user_id: int) -> str:
    if user_id not in state["username_cache"]:
        try:
            user = bot.get_user(user_id) or await bot.fetch_user(user_id)
            state["username_cache"][user_id] = user.display_name if user else f"usuario_{user_id}"
        except discord.NotFound:
            state["username_cache"][user_id] = f"usuario_{user_id}"
    return state["username_cache"][user_id]


async def _process_new_chunks(state: dict) -> None:
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
            print(f"[STREAM] user={user_id}: {text[:60]}...")

    if lines:
        try:
            await state["transcript_channel"].send("\n".join(lines))
        except discord.HTTPException as e:
            print(f"[WARN] Falha ao postar transcrição: {e}")


async def _transcribe_remaining(state: dict) -> None:
    lines = []
    channel = state["transcript_channel"]

    async with channel.typing():
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
        try:
            await channel.send("\n".join(lines))
        except discord.HTTPException as e:
            print(f"[WARN] Falha ao postar transcrição final: {e}")


def _write_wav(path: Path, pcm_data: bytes) -> None:
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm_data)


bot.run(os.environ["DISCORD_TOKEN"])
