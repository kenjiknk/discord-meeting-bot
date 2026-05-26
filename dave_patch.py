"""
Patches discord-ext-voice-recv to apply DAVE (E2E encryption) decryption
before passing audio data to the Opus decoder.

discord-ext-voice-recv decrypts the RTP transport layer but doesn't know
about the DAVE MLS layer on top. This patch adds that second decryption step.

When decryption fails (DAVE session not ready, user SSRC not mapped yet, or
any other error), returns a silence frame instead of crashing the router thread.
"""
import logging
import davey
from discord.ext.voice_recv import opus as vr_opus

log = logging.getLogger(__name__)

# 20ms of silence: 48000 Hz * 2 channels * 2 bytes * 0.02s
_SILENCE = b'\x00' * 3840


_dbg_counts = {"passthrough": 0, "no_ssrc": 0, "ok": 0, "fail": 0}
_DBG_INTERVAL = 500


def _dave_decrypt(self, data: bytes) -> bytes | None:
    """
    Returns DAVE-decrypted bytes, original bytes (if DAVE not active),
    or None if the packet should be dropped (decryption failed).
    """
    try:
        vc = self.sink.voice_client
        dave_session = vc._connection.dave_session

        if dave_session is None or not dave_session.ready:
            _dbg_counts["passthrough"] += 1
            if _dbg_counts["passthrough"] % _DBG_INTERVAL == 1:
                log.info("DAVE decrypt stats (DAVE inativo): %s", _dbg_counts)
            return data  # DAVE não ativo — dados já são Opus válido

        user_id = vc._get_id_from_ssrc(self.ssrc)
        if user_id is None:
            _dbg_counts["no_ssrc"] += 1
            if _dbg_counts["no_ssrc"] % _DBG_INTERVAL == 1:
                log.info("DAVE decrypt stats (sem SSRC): %s", _dbg_counts)
            return None

        result = dave_session.decrypt(user_id, davey.MediaType.audio, data)
        _dbg_counts["ok"] += 1
        if _dbg_counts["ok"] % _DBG_INTERVAL == 1:
            log.info("DAVE decrypt stats (ok): %s", _dbg_counts)
        return result

    except Exception as e:
        _dbg_counts["fail"] += 1
        if _dbg_counts["fail"] % _DBG_INTERVAL == 1:
            log.info("DAVE decrypt stats (falha): %s", _dbg_counts)
        log.debug("DAVE decrypt failed ssrc=%s: %s", self.ssrc, e)
        return None


def _patched_decode_packet(self, packet):
    assert self._decoder is not None

    if packet:
        data = _dave_decrypt(self, packet.decrypted_data)

        if data is None:
            return packet, _SILENCE

        try:
            pcm = self._decoder.decode(data, fec=False)
        except Exception as e:
            log.debug("Opus decode failed ssrc=%s: %s — dropping packet", self.ssrc, e)
            return packet, _SILENCE

        return packet, pcm

    # Fake packet — usa FEC do próximo pacote
    next_packet = self._buffer.peek_next()

    if next_packet is not None:
        nextdata = _dave_decrypt(self, next_packet.decrypted_data)

        if nextdata is None:
            return packet, _SILENCE

        try:
            log.debug(
                "Generating fec packet: fake=%s, fec=%s",
                packet.sequence,
                next_packet.sequence,
            )
            pcm = self._decoder.decode(nextdata, fec=True)
        except Exception as e:
            log.debug("Opus FEC decode failed ssrc=%s: %s", self.ssrc, e)
            return packet, _SILENCE
    else:
        pcm = self._decoder.decode(None, fec=False)

    return packet, pcm


def apply():
    vr_opus.PacketDecoder._decode_packet = _patched_decode_packet
    print("[DAVE patch] aplicado — decriptação MLS + silence fallback ativo")
