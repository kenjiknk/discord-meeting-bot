import io
import os
import re
import wave
from faster_whisper import WhisperModel, BatchedInferencePipeline

_model: WhisperModel | None = None
_pipeline: BatchedInferencePipeline | None = None

_SPECIAL_TOKENS = re.compile(r'\[[\w\s*]+\]|\([\w\s]+\)|<[\w|]+>', re.IGNORECASE)

SAMPLE_RATE = 48000
CHANNELS = 2
SAMPLE_WIDTH = 2


def load_model() -> BatchedInferencePipeline:
    global _model, _pipeline
    if _pipeline is None:
        name = os.getenv("WHISPER_MODEL", "large-v3")
        threads = int(os.getenv("WHISPER_THREADS", "8"))
        print(f"[Whisper] carregando '{name}' — {threads} threads, int8...")
        _model = WhisperModel(name, device="cpu", compute_type="int8", cpu_threads=threads)
        _pipeline = BatchedInferencePipeline(model=_model)
        print(f"[Whisper] '{name}' pronto.")
    return _pipeline


def _pcm_to_wav_buffer(pcm_data: bytes) -> io.BytesIO:
    """Encapsula PCM bruto num WAV em memória — sem tocar o disco."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm_data)
    buf.seek(0)
    return buf


def transcribe_pcm(pcm_data: bytes) -> str:
    """Transcreve PCM bruto diretamente da memória — sem arquivo intermediário."""
    pipeline = load_model()
    language = os.getenv("WHISPER_LANGUAGE", "pt")

    audio_buf = _pcm_to_wav_buffer(pcm_data)

    segments, info = pipeline.transcribe(
        audio_buf,
        language=language,
        task="transcribe",
        batch_size=int(os.getenv("WHISPER_BATCH_SIZE", "8")),
        initial_prompt="Transcrição de reunião em Português Brasileiro.",
        vad_filter=True,
        vad_parameters={
            "min_silence_duration_ms": 500,
            "speech_pad_ms": 200,
            "threshold": 0.4,
        },
        suppress_blank=True,
        no_speech_threshold=0.6,
        log_prob_threshold=-1.0,
        temperature=0,
    )

    print(f"[Whisper] idioma={info.language} prob={info.language_probability:.2f}")

    parts = []
    for seg in segments:
        text = _SPECIAL_TOKENS.sub("", seg.text).strip()
        if text:
            parts.append(text)

    return " ".join(parts)
