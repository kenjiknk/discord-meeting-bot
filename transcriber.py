import os
import re
import numpy as np
from faster_whisper import WhisperModel

_model: WhisperModel | None = None

_SPECIAL_TOKENS = re.compile(r'\[[\w\s*]+\]|\([\w\s]+\)|<[\w|]+>', re.IGNORECASE)

SAMPLE_RATE = 48000
CHANNELS = 2
SAMPLE_WIDTH = 2


def load_model() -> WhisperModel:
    global _model
    if _model is None:
        name = os.getenv("WHISPER_MODEL", "large-v3")
        threads = int(os.getenv("WHISPER_THREADS", "8"))
        print(f"[Whisper] carregando '{name}' — {threads} threads, int8...")
        _model = WhisperModel(name, device="cpu", compute_type="int8", cpu_threads=threads)
        print(f"[Whisper] '{name}' pronto.")
    return _model


def _pcm_to_float_mono(pcm_data: bytes) -> np.ndarray:
    # Stereo int16 48kHz → mono float32 16kHz
    # 48kHz / 16kHz = 3 → average every 3 samples (box filter, prevents aliasing)
    samples = np.frombuffer(pcm_data, dtype=np.int16).reshape(-1, CHANNELS)
    mono = samples.mean(axis=1).astype(np.float32) / 32768.0
    n = len(mono) // 3 * 3
    return mono[:n].reshape(-1, 3).mean(axis=1)


_RMS_THRESHOLD = 0.003


def _has_speech(audio: np.ndarray) -> bool:
    return float(np.sqrt(np.mean(audio ** 2))) >= _RMS_THRESHOLD


def transcribe_pcm(pcm_data: bytes, context: str = "") -> str:
    model = load_model()
    language = os.getenv("WHISPER_LANGUAGE", "pt")

    audio = _pcm_to_float_mono(pcm_data)

    if not _has_speech(audio):
        return ""

    prompt = context if context else "Transcrição de reunião em Português Brasileiro."

    segments, info = model.transcribe(
        audio,
        language=language,
        task="transcribe",
        initial_prompt=prompt,
        vad_filter=True,
        vad_parameters={
            "min_silence_duration_ms": 300,
            "speech_pad_ms": 100,
            "threshold": 0.5,
        },
        suppress_blank=True,
        no_speech_threshold=0.6,
        log_prob_threshold=-0.5,
        compression_ratio_threshold=2.0,
        condition_on_previous_text=False,
        temperature=0,
        beam_size=5,
    )

    print(f"[Whisper] lang={info.language} prob={info.language_probability:.2f}")

    parts = []
    for seg in segments:
        text = _SPECIAL_TOKENS.sub("", seg.text).strip()
        if text:
            parts.append(text)

    return " ".join(parts)
