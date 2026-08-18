"""UnAI Discord Workspace implementation.

Subclasses Workspace from UnAI SDK. Interacts natively with Discord REST API v10
using user or bot tokens. Provides full capability for reading servers, channels,
messages with attachments, sending messages, replies, member lookup, and notifications.

Follows ADR-0004 for one-shot login tool state management.
"""

import asyncio
import base64
import collections
from datetime import datetime
import io
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import wave
import aiohttp
import speech_recognition as sr

try:
    import audioop
except ImportError:
    import audioop_lts as audioop

try:
    import webrtcvad
except ImportError:
    webrtcvad = None

from unai.sdk import Workspace, tool

try:
    from voice_engine import VoiceEngine
except ImportError:
    try:
        from .voice_engine import VoiceEngine
    except ImportError:
        VoiceEngine = None


DISCORD_API_BASE = "https://discord.com/api/v10"


def _file_to_base64_data_uri(file_path: str) -> str:
    p = Path(file_path)
    if not p.exists():
        raise RuntimeError(f"Avatar/Banner file not found: {file_path}")
    data = p.read_bytes()
    b64 = base64.b64encode(data).decode("utf-8")
    ext = p.suffix.lower()
    mime = "image/png"
    if ext in [".jpg", ".jpeg"]:
        mime = "image/jpeg"
    elif ext == ".gif":
        mime = "image/gif"
    elif ext == ".webp":
        mime = "image/webp"
    return f"data:{mime};base64,{b64}"


def _get_data_dir() -> Path:
    d = Path.home() / ".unai" / "data" / "discord"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _resolve_gateway_properties(platform: str, is_bot: bool = False) -> Dict[str, str]:
    if is_bot:
        return {"os": "Linux", "browser": "UnAI-Discord-Bot", "device": "UnAI-Discord-Bot"}
    p = platform.lower().strip()
    if p in ["mobile", "phone", "android"]:
        return {"os": "Android", "browser": "Discord Android", "device": "Android"}
    elif p in ["ios", "iphone"]:
        return {"os": "iOS", "browser": "Discord iOS", "device": "iPhone"}
    elif p in ["desktop", "pc", "app"]:
        return {"os": "Linux", "browser": "Discord Client", "device": "discord-desktop"}
    elif p in ["console", "embedded"]:
        return {"os": "Linux", "browser": "Discord Embedded", "device": "Console"}
    else:  # web / browser
        return {"os": "Linux", "browser": "Chrome", "device": ""}


def _get_token_file() -> Path:
    return _get_data_dir() / "session.json"


try:
    import discord.opus
    from ctypes.util import find_library
    if not discord.opus.is_loaded():
        lib = find_library("opus")
        if lib:
            discord.opus.load_opus(lib)
except Exception:
    pass


try:
    from discord.ext import voice_recv
    from discord.ext.voice_recv import reader as _vr_reader
    from discord.ext.voice_recv import router as _vr_router
    import davey

    _vr_reader.UDPKeepAlive.delay = 5

    def _fixed_udp_keepalive_run(self) -> None:
        self.voice_client.wait_until_connected()
        while not self._end_thread.is_set():
            vc = self.voice_client
            try:
                packet = self.counter.to_bytes(8, 'big')
            except OverflowError:
                self.counter = 0
                continue
            try:
                if hasattr(vc, '_connection') and vc._connection and hasattr(vc._connection, 'socket') and vc._connection.socket:
                    vc._connection.socket.send(packet)
            except Exception:
                pass
            self.counter += 1
            time.sleep(self.delay)

    _vr_reader.UDPKeepAlive.run = _fixed_udp_keepalive_run

    # Crash-proof PacketDecoder._decode_packet (with DAVE MLS support and Opus error resilience)
    def _safe_decode_packet(self, packet):
        assert self._decoder is not None
        try:
            if packet and hasattr(packet, 'decrypted_data') and packet.decrypted_data:
                vc = getattr(self.sink, 'voice_client', None)
                if vc and hasattr(vc, '_connection') and vc._connection:
                    dave_session = getattr(vc._connection, 'dave_session', None)
                    user_id = self._cached_id or 0
                    if dave_session and user_id:
                        try:
                            if not dave_session.can_passthrough(user_id):
                                decrypted = dave_session.decrypt(user_id, davey.MediaType.audio, packet.decrypted_data)
                                if decrypted:
                                    packet.decrypted_data = decrypted
                        except Exception:
                            pass
                pcm = self._decoder.decode(packet.decrypted_data, fec=False)
                return packet, pcm
            elif packet:
                pcm = self._decoder.decode(packet.decrypted_data, fec=False)
                return packet, pcm
            else:
                next_packet = self._buffer.peek_next()
                if next_packet is not None:
                    pcm = self._decoder.decode(getattr(next_packet, 'decrypted_data', None), fec=True)
                else:
                    pcm = self._decoder.decode(None, fec=False)
                return packet, pcm
        except Exception:
            # Return silence on malformed/corrupted frames instead of crashing the PacketRouter thread
            return packet, b'\x00' * 3840

    _vr_router.PacketDecoder._decode_packet = _safe_decode_packet

    # Crash-proof PacketRouter._do_run (ensures reader never calls stop_listening() on transient errors)
    def _safe_packet_router_do_run(self) -> None:
        while not self._end_thread.is_set():
            try:
                self.waiter.wait()
                with self._lock:
                    for decoder in list(self.waiter.items):
                        try:
                            data = decoder.pop_data()
                            if data is not None:
                                self.sink.write(data.source, data)
                        except Exception:
                            pass
            except Exception:
                pass

    _vr_router.PacketRouter._do_run = _safe_packet_router_do_run

    # Prevent single packet or decrypt glitches from stopping the AudioReader listener thread
    _orig_reader_callback = _vr_reader.AudioReader.callback

    def _safe_reader_callback(self, packet_data: bytes) -> None:
        try:
            _orig_reader_callback(self, packet_data)
        except Exception:
            pass
        finally:
            self.error = None

    _vr_reader.AudioReader.callback = _safe_reader_callback

    AudioSinkBase = voice_recv.AudioSink


except Exception:
    try:
        from discord.ext import voice_recv
        AudioSinkBase = voice_recv.AudioSink
    except Exception:
        class AudioSinkBase:
            pass



def _clean_transcript_text(text: str) -> str:
    """Normalize transcript casing and deduplicate Whisper repetition loops."""
    if not text or not text.strip():
        return ""
    t = text.strip().strip("\"'`«»")

    # 1. Deduplicate repetitive sentence/clause patterns (e.g. 'А, да. А, да. А, да.' -> 'А, да.')
    for _ in range(3):
        m = re.match(r'^(.{2,30}?[.!?,\s])(?:\s*\1){2,}', t, re.IGNORECASE)
        if m:
            t = m.group(1).strip()
            break
        words = t.split()
        if len(words) >= 4 and len(set(w.lower().rstrip('.!?') for w in words)) == 1:
            t = words[0].rstrip('.!?') + '.'
            break

    # 2. Normalize ALL CAPS shouting (e.g. 'КАК ТОПИТ?' -> 'Как топит?')
    letters = [c for c in t if c.isalpha()]
    if len(letters) >= 4 and sum(1 for c in letters if c.isupper()) / len(letters) >= 0.8:
        sentences = [s.strip() for s in re.split(r'([.!?]+)', t) if s.strip()]
        rebuilt = []
        for i in range(0, len(sentences), 2):
            s = sentences[i].lower().capitalize()
            punc = sentences[i+1] if i+1 < len(sentences) else ''
            rebuilt.append(s + punc)
        t = " ".join(rebuilt)

    return t.strip()


def _pcm_to_clean_wav(raw_pcm: bytes) -> bytes:
    """Convert raw 48000Hz 16-bit stereo PCM from Discord to clean, normalized 16kHz mono WAV."""
    try:
        mono_48k = audioop.tomono(raw_pcm, 2, 0.5, 0.5)
        mono_16k, _ = audioop.ratecv(mono_48k, 2, 1, 48000, 16000, None)

        # DC offset removal
        try:
            avg = audioop.avg(mono_16k, 2)
            mono_16k = audioop.bias(mono_16k, 2, -avg)
        except Exception:
            pass

        # Safe volume normalization: only normalize if within healthy speech range
        # Avoid blindly boosting quiet hiss/static 3x
        try:
            cur_rms = audioop.rms(mono_16k, 2)
            if 1200 <= cur_rms <= 15000:
                factor = min(1.8, max(0.6, 4500.0 / cur_rms))
                mono_16k = audioop.mul(mono_16k, 2, factor)
        except Exception:
            pass

        wav_io = io.BytesIO()
        with wave.open(wav_io, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(mono_16k)
        return wav_io.getvalue()
    except Exception:
        pass

    wav_io = io.BytesIO()
    with wave.open(wav_io, "wb") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(48000)
        wf.writeframes(raw_pcm)
    return wav_io.getvalue()


class UserVoiceBuffer:
    """Per-speaker Discrete Voice Activity Detector (VAD) & Phrase Accumulator.
    
    Combines:
    - WebRTC VAD (GMM spectral model across 6 frequency bands)
    - Adaptive Noise Floor Tracking (EMA per-speaker baseline)
    - Zero-Crossing Rate (ZCR) filtering (rejects high-frequency hiss / fans)
    - Pre-speech circular buffer (~400ms) to ensure phrase onsets are preserved
    """

    def __init__(self, uid: int, uname: str, vad_mode: int = 0, min_energy: int = 150):
        self.uid = uid
        self.uname = uname
        self.vad_mode = vad_mode
        self._vad = webrtcvad.Vad(vad_mode) if webrtcvad else None
        self.noise_floor_rms: float = 150.0
        self.min_energy = min_energy
        self.pre_buffer: collections.deque = collections.deque(maxlen=20)  # ~400ms pre-speech ring buffer
        self.phrase_frames: List[bytes] = []
        self.is_speaking: bool = False
        self.consecutive_speech_frames: int = 0

        self.silence_frame_count: int = 0
        self.voiced_frame_count: int = 0
        self.voiced_rms_sum: float = 0.0
        self.last_packet_time: float = time.time()
        self.phrase_start_time: float = 0.0

    def _is_frame_speech(self, pcm_bytes: bytes) -> Tuple[bool, float, float]:
        """Check if 20ms frame contains speech using WebRTC VAD + Adaptive RMS Gate."""
        try:
            frame_rms = float(audioop.rms(pcm_bytes, 2))
        except Exception:
            frame_rms = 0.0

        try:
            mono_48k = audioop.tomono(pcm_bytes, 2, 0.5, 0.5)
            mono_16k, _ = audioop.ratecv(mono_48k, 2, 1, 48000, 16000, None)
        except Exception:
            mono_16k = b""

        # Zero-crossing rate on 16kHz mono (320 samples in 20ms)
        zcr = 0.0
        if mono_16k and len(mono_16k) == 640:
            try:
                crossings = audioop.cross(mono_16k, 2)
                zcr = crossings / 320.0
            except Exception:
                zcr = 0.0

        is_vad_active = False
        if self._vad and mono_16k and len(mono_16k) == 640:
            try:
                is_vad_active = self._vad.is_speech(mono_16k, 16000)
            except Exception:
                is_vad_active = False

        # Dynamic speech threshold: trigger if WebRTC VAD detects speech OR energy is above baseline
        dynamic_thresh = max(self.min_energy, self.noise_floor_rms * 1.10)
        has_enough_energy = frame_rms >= dynamic_thresh

        # Speech condition: WebRTC VAD active or energy spike with reasonable ZCR
        if self._vad:
            is_speech = is_vad_active or (has_enough_energy and zcr <= 0.55)
        else:
            is_speech = has_enough_energy and zcr <= 0.55

        if not is_speech and 30 < frame_rms < 1000:
            # Update ambient noise floor with slow exponential moving average
            self.noise_floor_rms = 0.96 * self.noise_floor_rms + 0.04 * frame_rms

        if not hasattr(self, '_total_frame_cnt'):
            self._total_frame_cnt = 0
        self._total_frame_cnt += 1
        if self._total_frame_cnt <= 3 or self._total_frame_cnt % 100 == 0:
            try:
                dbg_log = _get_data_dir() / "voice_debug.log"
                with open(dbg_log, "a") as f:
                    f.write(f"[{datetime.now()}] 📊 [VAD FRAME #{self._total_frame_cnt}] {self.uname}: RMS={int(frame_rms)}, thresh={int(dynamic_thresh)}, vad={is_vad_active}, zcr={zcr:.2f}, speech={is_speech}\n")
            except Exception:
                pass

        return is_speech, frame_rms, zcr


    def process_frame(self, pcm_bytes: bytes) -> Optional[Tuple[bytes, float, float]]:
        """Process incoming 20ms frame. Returns (raw_pcm, duration_sec, avg_rms) if phrase completed."""
        now = time.time()
        self.last_packet_time = now

        is_speech, frame_rms, zcr = self._is_frame_speech(pcm_bytes)

        if not self.is_speaking:
            if is_speech:
                self.consecutive_speech_frames += 1
                # Require 2 consecutive speech frames (~40ms) to trigger phrase start
                if self.consecutive_speech_frames >= 2:
                    self.is_speaking = True
                    self.phrase_start_time = now
                    self.phrase_frames = list(self.pre_buffer) + [pcm_bytes]
                    self.pre_buffer.clear()
                    self.silence_frame_count = 0
                    self.voiced_frame_count = self.consecutive_speech_frames
                    self.voiced_rms_sum = frame_rms * self.consecutive_speech_frames
                    try:
                        dbg_log = _get_data_dir() / "voice_debug.log"
                        with open(dbg_log, "a") as f:
                            f.write(f"[{datetime.now()}] 🎙️ [VAD START] {self.uname} speech detected (RMS {int(frame_rms)}, noise floor {int(self.noise_floor_rms)}, ZCR {zcr:.2f})\n")
                    except Exception:
                        pass
            else:
                self.consecutive_speech_frames = 0
                self.pre_buffer.append(pcm_bytes)
            return None

        # Already speaking
        self.phrase_frames.append(pcm_bytes)
        if is_speech:
            self.voiced_frame_count += 1
            self.voiced_rms_sum += frame_rms
            self.silence_frame_count = 0
        else:
            self.silence_frame_count += 1

        # End of phrase condition 1: ~400ms (20 frames) of natural pause silence
        if self.silence_frame_count >= 20:
            return self._flush_phrase(continued=False)

        # End of phrase condition 2: natural sentence break (micro-pause >= 160ms) if phrase is already >= 10.0s
        if len(self.phrase_frames) >= 500 and self.silence_frame_count >= 8:
            return self._flush_phrase(continued=False)

        # End of phrase condition 3: max phrase duration 15.0s (750 frames) for continuous non-stop speech
        if len(self.phrase_frames) >= 750:
            return self._flush_phrase(continued=True)

        return None

    def flush_if_timed_out(self, now: float, timeout: float = 0.35) -> Optional[Tuple[bytes, float, float]]:
        """Called by periodic inactivity monitor when Discord stops sending UDP packets."""
        if self.is_speaking and (now - self.last_packet_time >= timeout):
            return self._flush_phrase(continued=False)
        return None

    def _flush_phrase(self, continued: bool = False) -> Optional[Tuple[bytes, float, float]]:
        """Finalize current speech phrase and validate before dispatching."""
        frames = self.phrase_frames
        voiced_cnt = self.voiced_frame_count
        voiced_rms = self.voiced_rms_sum

        # Reset state with smooth rollover if user continues speaking non-stop
        self.is_speaking = continued
        self.consecutive_speech_frames = 2 if continued else 0
        self.silence_frame_count = 0
        self.voiced_frame_count = 0
        self.voiced_rms_sum = 0.0
        self.phrase_frames = []

        if not frames:
            return None

        total_frames = len(frames)
        duration_sec = total_frames * 0.02
        avg_rms = voiced_rms / max(1, voiced_cnt)

        # Allow short genuine speech phrases
        if duration_sec < 0.18 or voiced_cnt < 2:
            return None

        raw_pcm = b"".join(frames)
        return (raw_pcm, duration_sec, avg_rms)





class STTVoiceSink(AudioSinkBase):
    """Real-time Voice Receiver and Speech-to-Text Sink with WebRTC VAD & Whisper Confidence Gating."""

    def __init__(self, callback: Any, language: str = "ru-RU", stt_config: Optional[Dict[str, Any]] = None, loop: Optional[Any] = None):
        super().__init__()
        self.callback = callback
        self.language = language
        self.stt_config = stt_config or {}
        try:
            self.loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            try:
                self.loop = loop or asyncio.get_event_loop()
            except RuntimeError:
                self.loop = None

        self._user_buffers: Dict[int, UserVoiceBuffer] = {}
        self._inactivity_task: Optional[asyncio.Task] = None
        self.user_info: Dict[int, Dict[str, Any]] = {}
        self.packets_received: int = 0
        self.bytes_received: int = 0
        self.transcripts_count: int = 0
        self.last_packet_time: float = 0.0
        self.speakers: Set[str] = set()
        self._seen_speakers: Set[int] = set()
        self._running = True

        if self.loop and self.loop.is_running():
            self._inactivity_task = self.loop.create_task(self._inactivity_loop())

    def wants_opus(self) -> bool:
        return False

    def write(self, user: Optional[Any], data: Any) -> None:
        if not self._running:
            return

        pcm_bytes = getattr(data, "pcm", None)
        if not pcm_bytes:
            return

        ssrc = (getattr(data, "packet", None) and getattr(data.packet, "ssrc", 0)) or 0
        user_id = getattr(user, "id", None)
        uid = user_id or ssrc or 0
        uname = getattr(user, "display_name", None) or getattr(user, "name", None) or "Speaker"

        now = time.time()
        self.packets_received += 1
        self.bytes_received += len(pcm_bytes)
        self.last_packet_time = now
        self.speakers.add(uname)

        if uid not in self._seen_speakers:
            self._seen_speakers.add(uid)
            try:
                dbg_log = _get_data_dir() / "voice_debug.log"
                with open(dbg_log, "a") as f:
                    f.write(f"[{datetime.now()}] 📡 [VOICE RECV] Audio packets arriving from {uname} (UID: {uid}, SSRC: {ssrc})\n")
            except Exception:
                pass


        uinfo = {
            "id": str(user_id or uid),
            "username": uname,
            "avatar": getattr(user, "display_avatar", None) and str(user.display_avatar.url),
        }
        self.user_info[uid] = uinfo

        # Lazy-init inactivity loop if loop wasn't ready at init
        if self._inactivity_task is None and self.loop and self.loop.is_running() and self._running:
            self._inactivity_task = self.loop.create_task(self._inactivity_loop())

        try:
            if uid not in self._user_buffers:
                vad_mode = int(self.stt_config.get("vad_mode", 1))
                min_energy = int(self.stt_config.get("min_energy", 250))
                self._user_buffers[uid] = UserVoiceBuffer(uid=uid, uname=uname, vad_mode=vad_mode, min_energy=min_energy)

            user_buf = self._user_buffers[uid]
            res = user_buf.process_frame(pcm_bytes)
            if res:
                raw_pcm, dur, avg_rms = res
                if self.loop and self.loop.is_running():
                    asyncio.run_coroutine_threadsafe(self._process_stt_raw_pcm(uid, uinfo, raw_pcm, dur, avg_rms), self.loop)
        except Exception as write_err:
            try:
                dbg_log = _get_data_dir() / "voice_debug.log"
                with open(dbg_log, "a") as f:
                    f.write(f"[{datetime.now()}] ⚠️ [WRITE ERR] {write_err}\n")
            except Exception:
                pass


    async def _inactivity_loop(self) -> None:
        """Check for speakers who paused and Discord stopped sending UDP packets (DTX)."""
        while self._running:
            try:
                await asyncio.sleep(0.12)
                now = time.time()
                for uid, user_buf in list(self._user_buffers.items()):
                    res = user_buf.flush_if_timed_out(now, timeout=0.50)
                    if res:
                        raw_pcm, dur, avg_rms = res
                        uinfo = self.user_info.get(uid, {"id": str(uid), "username": user_buf.uname})
                        if self.loop and self.loop.is_running():
                            self.loop.create_task(self._process_stt_raw_pcm(uid, uinfo, raw_pcm, dur, avg_rms))
            except asyncio.CancelledError:
                break
            except Exception:
                pass


    def _is_whisper_hallucination(self, text: str) -> bool:
        """Filter out obvious Whisper subtitle/bracket artifacts only."""
        if not text or not text.strip():
            return True
        cleaned = text.strip()
        lower = cleaned.lower().strip("\"'«»`").rstrip(".!?,;:").strip()
        if not lower:
            return True

        # 1. Bracket artifacts: [музыка], (тишина), *аплодисменты*, etc.
        if (lower.startswith("[") and lower.endswith("]")) or (lower.startswith("(") and lower.endswith(")")) or (lower.startswith("*") and lower.endswith("*")):
            return True

        # 2. Only blatant subtitle artifacts
        blatant = {
            "продолжение следует",
            "субтитры",
            "dimatorzok",
            "диматорзок",
        }
        if lower in blatant:
            return True

        for pat in [r"продолжение следует", r"dimatorzok", r"диматорзок", r"^субтитры"]:
            if re.search(pat, lower):
                return True

        return False


    async def _process_stt_raw_pcm(self, uid: int, uinfo: Dict[str, Any], raw_pcm: bytes, duration_sec: float, avg_rms: float) -> None:
        dbg_log = _get_data_dir() / "voice_debug.log"
        username = uinfo.get("username") or "Speaker"

        # Preprocess raw PCM to clean 16kHz mono WAV
        wav_bytes = _pcm_to_clean_wav(raw_pcm)
        if not wav_bytes or len(wav_bytes) < 100:
            return

        # Save debug wav for local inspection
        try:
            with open("/tmp/unai_last_voice.wav", "wb") as wf_out:
                wf_out.write(wav_bytes)
        except Exception:
            pass

        try:
            with open(dbg_log, "a") as f:
                f.write(f"[{datetime.now()}] 🚀 [STT SEND] Transcribing {duration_sec:.1f}s audio (avg RMS {int(avg_rms)}) for {username}...\n")
        except Exception:
            pass

        text = ""
        cfg = self.stt_config or {}
        provider = cfg.get("provider", "omniroute")
        api_base = cfg.get("api_base", "http://localhost:20128/v1").rstrip("/")
        api_key = cfg.get("api_key", "omniroute")
        lang = cfg.get("language", "ru" if "ru" in self.language.lower() else "en")
        preferred_model = cfg.get("model", "groq/whisper-large-v3-turbo")

        models_to_try = [preferred_model]
        candidates = [
            "groq/whisper-large-v3-turbo",
            "groq/whisper-large-v3",
            "whisper-large-v3-turbo",
            "whisper-large-v3",
            "whisper-1",
            "openai/whisper-1",
            "cloudflare/@cf/openai/whisper",
        ]
        for c in candidates:
            if c not in models_to_try:
                models_to_try.append(c)

        is_rejected_hallucination = False

        if provider in ["omniroute", "openai_compatible", "groq", "whisper"]:
            for model in models_to_try:
                try:
                    form = aiohttp.FormData()
                    form.add_field("file", wav_bytes, filename="audio.wav", content_type="audio/wav")
                    form.add_field("model", model)
                    form.add_field("language", lang)
                    form.add_field("temperature", "0.0")
                    form.add_field("response_format", "verbose_json")
                    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

                    async with aiohttp.ClientSession() as session:
                        async with session.post(f"{api_base}/audio/transcriptions", headers=headers, data=form, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                raw_text = data.get("text", "")
                                segments = data.get("segments") or []

                                # Confidence & Hallucination Gating on Whisper verbose metadata
                                is_hallucination = False
                                reject_reasons = []

                                if segments:
                                    avg_no_speech = sum(s.get("no_speech_prob", 0.0) for s in segments) / len(segments)
                                    avg_logprob = sum(s.get("avg_logprob", 0.0) for s in segments) / len(segments)

                                    if avg_no_speech > 0.85 and avg_logprob < -1.5:
                                        is_hallucination = True
                                        reject_reasons.append(f"no_speech_prob ({avg_no_speech:.2f})")

                                candidate = _clean_transcript_text(raw_text)
                                if not candidate or self._is_whisper_hallucination(candidate):
                                    is_hallucination = True
                                    reject_reasons.append("pattern/loop filter")


                                if is_hallucination:
                                    is_rejected_hallucination = True
                                    with open(dbg_log, "a") as f:
                                        f.write(f"[{datetime.now()}] 🛡️ [STT REJECT] Filtered hallucination '{raw_text.strip()}' for {username} ({', '.join(reject_reasons)})\n")
                                else:
                                    text = candidate
                                    with open(dbg_log, "a") as f:
                                        f.write(f"[{datetime.now()}] 🎯 [STT RESULT] Whisper ({model}) for {username}: '{text}'\n")
                                break
                            else:
                                err_body = await resp.text()
                                with open(dbg_log, "a") as f:
                                    f.write(f"[{datetime.now()}] ⚠️ [STT FAILOVER] Whisper ({model}) returned status {resp.status}: {err_body[:100]}, failing over to next model...\n")
                except Exception as e:
                    with open(dbg_log, "a") as f:
                        f.write(f"[{datetime.now()}] ⚠️ [STT FAILOVER] Whisper ({model}) request error: {e}, failing over to next model...\n")


        # Fallback to Google Web Speech API only if Whisper errored out (not when rejected as noise)
        if not text and not is_rejected_hallucination:
            def _google_transcribe():
                try:
                    r = sr.Recognizer()
                    with sr.AudioFile(io.BytesIO(wav_bytes)) as source:
                        audio_data = r.record(source)
                    return r.recognize_google(audio_data, language=self.language)
                except Exception:
                    return ""

            loop = asyncio.get_event_loop()
            candidate = await loop.run_in_executor(None, _google_transcribe)
            if candidate:
                candidate = _clean_transcript_text(candidate)
            if candidate and not self._is_whisper_hallucination(candidate):
                text = candidate
                with open(dbg_log, "a") as f:
                    f.write(f"[{datetime.now()}] 🎯 [STT RESULT] Google STT for {username}: '{text}'\n")

        if text and text.strip():
            self.transcripts_count += 1
            notif = {
                "id": str(int(time.time() * 1000)),
                "type": "voice_transcript",
                "user_id": uinfo.get("id"),
                "username": uinfo.get("username"),
                "text": text.strip(),
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "language": self.language,
                "read": False,
            }
            if self.callback:
                try:
                    self.callback(notif)
                except Exception as cb_err:
                    with open(dbg_log, "a") as f:
                        f.write(f"[{datetime.now()}] ❌ Transcript callback error: {cb_err}\n")


    def _drop_user(self, uid: int) -> None:
        self._user_buffers.pop(uid, None)
        self.user_info.pop(uid, None)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "packets_received": self.packets_received,
            "bytes_received": self.bytes_received,
            "transcripts_count": self.transcripts_count,
            "speakers": list(self.speakers),
            "last_packet_seconds_ago": round(time.time() - self.last_packet_time, 1) if self.last_packet_time else None,
            "active_buffers": len(self._user_buffers),
            "running": self._running,
        }

    def cleanup(self) -> None:
        self._running = False
        if self._inactivity_task and not self._inactivity_task.done():
            self._inactivity_task.cancel()
        self._user_buffers.clear()


class VoiceManager:
    """Voice Connection & Media Streaming Manager for Discord channels."""

    def __init__(self):
        self._py_client: Optional[Any] = None
        self._active_sinks: Dict[int, Any] = {}

    async def get_py_client(self, token: str) -> Any:
        import discord
        from ctypes.util import find_library

        if not discord.opus.is_loaded():
            lib = find_library("opus")
            if lib:
                discord.opus.load_opus(lib)

        if self._py_client is None or self._py_client.is_closed():
            if hasattr(discord, "Intents"):
                intents = discord.Intents.default()
                if hasattr(intents, "voice_states"):
                    intents.voice_states = True
                if hasattr(intents, "guilds"):
                    intents.guilds = True
                client = discord.Client(intents=intents)
            else:
                client = discord.Client()

            clean_token = token.replace("Bot ", "").strip()
            await client.login(clean_token)
            asyncio.create_task(client.connect())
            try:
                await asyncio.wait_for(client.wait_until_ready(), timeout=12.0)
            except Exception:
                pass
            self._py_client = client

        return self._py_client

    async def get_or_connect(self, token: str, channel_id_str: str) -> Any:
        from discord.ext import voice_recv

        client = await self.get_py_client(token)
        cid = int(channel_id_str)

        channel = client.get_channel(cid)
        if channel is None:
            for g in client.guilds:
                ch = g.get_channel(cid)
                if ch:
                    channel = ch
                    break

        if channel is None:
            try:
                channel = await client.fetch_channel(cid)
            except Exception:
                pass

        if channel is None:
            raise RuntimeError(f"Voice channel or Guild '{channel_id_str}' not found.")

        guild = getattr(channel, "guild", None)
        if guild and guild.voice_client and guild.voice_client.is_connected():
            if guild.voice_client.channel.id != channel.id:
                await guild.voice_client.move_to(channel)
            return guild.voice_client
        elif guild and guild.voice_client:
            try:
                await guild.voice_client.disconnect(force=True)
            except Exception:
                pass

        vc = await channel.connect(cls=voice_recv.VoiceRecvClient, reconnect=True, timeout=20.0)
        return vc

    async def start_listening(self, token: str, channel_id_str: str, on_transcript_cb: Any, language: str = "ru-RU", stt_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        vc = await self.get_or_connect(token, channel_id_str)
        gid = vc.guild.id

        if hasattr(vc, "stop_listening"):
            try:
                vc.stop_listening()
            except Exception:
                pass

        if gid in self._active_sinks:
            try:
                self._active_sinks[gid].cleanup()
            except Exception:
                pass

        client_loop = getattr(self._py_client, "loop", None) or asyncio.get_event_loop()
        sink = STTVoiceSink(callback=on_transcript_cb, language=language, stt_config=stt_config, loop=client_loop)
        if hasattr(vc, "listen"):
            try:
                vc.listen(sink)
            except Exception as listen_err:
                try:
                    dbg_log = _get_data_dir() / "voice_debug.log"
                    with open(dbg_log, "a") as f:
                        f.write(f"[{datetime.now()}] ❌ [LISTEN ERROR] Failed to attach sink: {listen_err}\n")
                except Exception:
                    pass
                raise

        self._active_sinks[gid] = sink
        try:
            dbg_log = _get_data_dir() / "voice_debug.log"
            with open(dbg_log, "a") as f:
                f.write(f"[{datetime.now()}] 🎧 [LISTEN START] Voice listener attached to '{vc.channel.name}' in '{vc.guild.name}'. STT Model: {stt_config.get('model', 'default') if stt_config else 'default'}\n")
        except Exception:
            pass


        return {
            "channel_id": str(vc.channel.id),
            "channel_name": vc.channel.name,
            "guild_id": str(gid),
            "guild_name": vc.guild.name,
            "listening": True,
            "language": language,
            "info": f"Started Voice Listener in Voice Channel '{vc.channel.name}' on '{vc.guild.name}'. Incoming speech is captured, transcribed via STT, and piped to transcripts feed."
        }

    async def stop_listening(self, token: str, channel_id_str: str) -> str:
        client = await self.get_py_client(token)
        cid = int(channel_id_str) if channel_id_str.isdigit() else 0
        for gid, sink in list(self._active_sinks.items()):
            g = client.get_guild(gid)
            if cid == 0 or gid == cid or (g and g.get_channel(cid)):
                try:
                    if g and g.voice_client and hasattr(g.voice_client, "stop_listening"):
                        g.voice_client.stop_listening()
                except Exception:
                    pass
                sink.cleanup()
                self._active_sinks.pop(gid, None)
                return f"Voice Listener stopped for guild {gid}."
        return "No active Voice Listener was found for this channel/guild."

    async def get_status(self, token: str, channel_id_str: str) -> Dict[str, Any]:
        client = await self.get_py_client(token)
        cid = int(channel_id_str) if channel_id_str.isdigit() else 0
        for gid, sink in list(self._active_sinks.items()):
            g = client.get_guild(gid)
            if cid == 0 or gid == cid or (g and g.get_channel(cid)):
                stats = sink.get_stats()
                return {
                    "listening": True,
                    "guild_id": str(gid),
                    "guild_name": g.name if g else "Unknown",
                    "channel_id": str(g.voice_client.channel.id) if g and g.voice_client and g.voice_client.channel else channel_id_str,
                    "channel_name": g.voice_client.channel.name if g and g.voice_client and g.voice_client.channel else "Unknown",
                    "stats": stats,
                    "info": f"Listening is active. Received {stats.get('packets_received', 0)} packets ({stats.get('bytes_received', 0)} PCM bytes). Transcripts generated: {stats.get('transcripts_count', 0)}.",
                }
        return {"listening": False, "stats": None, "info": "No active voice listener is running on this channel/guild."}

    def is_listening(self, guild_id: int) -> bool:
        return guild_id in self._active_sinks


class DiscordWorkspace(Workspace):
    """Native Discord Workspace for autonomous AI agents."""

    def __init__(self, runtime_id: str = "discord", bus: Optional[Any] = None, **kwargs: Any):
        super().__init__(runtime_id=runtime_id, bus=bus, **kwargs)
        self._token: Optional[str] = None
        self._user_info: Optional[Dict[str, Any]] = None
        self._gateway_ws: Optional[Any] = None
        self._gateway_task: Optional[Any] = None
        self._gateway_connected: bool = False
        self._gateway_last_seq: Optional[int] = None
        self._notifications_cache: List[Dict[str, Any]] = []
        self._current_presence: Optional[Dict[str, Any]] = None
        self._platform: str = "desktop"
        self._voice_manager: VoiceManager = VoiceManager()
        self._voice_transcripts_cache: List[Dict[str, Any]] = []
        self._music_config: Dict[str, Any] = {}
        self._stt_config: Dict[str, Any] = {}
        self._active_tracks: Dict[int, Dict[str, Any]] = {}
        self._hermes_webhook_url: Optional[str] = None
        self._hermes_wake_words: List[str] = ["диром", "dirom"]
        self._hermes_always_trigger_dm: bool = True
        self._hermes_trigger_on_reply: bool = True
        self._hermes_trigger_on_mention: bool = True
        self._hermes_max_unread_history: int = 10
        self._voice_engine: Optional[Any] = None
        self._load_saved_token()
        self._load_notifications_cache()
        self._load_voice_transcripts_cache()
        self._load_music_config()
        self._load_stt_config()

    def _get_voice_engine(self) -> Any:
        if self._voice_engine is None:
            if VoiceEngine is None:
                raise RuntimeError("VoiceEngine could not be loaded. Ensure voice_engine.py is present.")
            self._voice_engine = VoiceEngine()
        return self._voice_engine

    async def _send_voice_note(
        self,
        channel_id: str,
        ogg_path: Path,
        duration_secs: float,
        waveform_b64: str,
        reply_to: str = "",
    ) -> Dict[str, Any]:
        """Sends an Opus .ogg voice message as a native Discord Voice Note (гска) with waveform."""
        payload: Dict[str, Any] = {
            "flags": 8192,
            "attachments": [
                {
                    "id": 0,
                    "filename": ogg_path.name,
                    "duration_secs": round(duration_secs, 2),
                    "waveform": waveform_b64,
                }
            ],
        }
        if reply_to:
            payload["message_reference"] = {"message_id": str(reply_to).strip()}

        # Primary method: Direct multipart upload
        try:
            data = aiohttp.FormData()
            data.add_field("payload_json", json.dumps(payload))
            data.add_field(
                "files[0]",
                ogg_path.read_bytes(),
                filename=ogg_path.name,
                content_type="audio/ogg",
            )
            return await self._api_request("POST", f"/channels/{channel_id}/messages", data=data)
        except Exception as direct_err:
            # Fallback method: /attachments endpoint upload URL
            try:
                raw_data = ogg_path.read_bytes()
                upload_req = {
                    "files": [
                        {
                            "filename": ogg_path.name,
                            "file_size": len(raw_data),
                            "id": 0,
                        }
                    ]
                }
                att_resp = await self._api_request("POST", f"/channels/{channel_id}/attachments", json_data=upload_req)
                attachments_meta = att_resp.get("attachments", [])
                if not attachments_meta:
                    raise direct_err

                upload_info = attachments_meta[0]
                upload_url = upload_info["upload_url"]
                upload_filename = upload_info["upload_filename"]

                headers = {"Content-Type": "audio/ogg"}
                async with aiohttp.ClientSession() as session:
                    async with session.put(upload_url, data=raw_data, headers=headers) as put_resp:
                        if put_resp.status not in (200, 201, 204):
                            raise RuntimeError(f"Voice upload failed with status {put_resp.status}")

                msg_payload = {
                    "flags": 8192,
                    "attachments": [
                        {
                            "id": "0",
                            "filename": ogg_path.name,
                            "uploaded_filename": upload_filename,
                            "duration_secs": round(duration_secs, 2),
                            "waveform": waveform_b64,
                        }
                    ],
                }
                if reply_to:
                    msg_payload["message_reference"] = {"message_id": str(reply_to).strip()}

                return await self._api_request("POST", f"/channels/{channel_id}/messages", json_data=msg_payload)
            except Exception:
                raise direct_err

    def _load_music_config(self) -> None:
        mf = _get_data_dir() / "music_config.json"
        if mf.exists():
            try:
                self._music_config = json.loads(mf.read_text())
            except Exception:
                self._music_config = {}
        else:
            self._music_config = {}

    def _save_music_config(self) -> None:
        mf = _get_data_dir() / "music_config.json"
        try:
            mf.write_text(json.dumps(self._music_config, ensure_ascii=False, indent=2))
        except Exception:
            pass

    def _load_stt_config(self) -> None:
        sf = _get_data_dir() / "stt_config.json"
        if sf.exists():
            try:
                self._stt_config = json.loads(sf.read_text())
            except Exception:
                self._stt_config = {}
        else:
            self._stt_config = {
                "provider": "omniroute",
                "api_base": "http://localhost:20128/v1",
                "api_key": "omniroute",
                "model": "groq/whisper-large-v3-turbo",
                "language": "ru",
                "vad_mode": 2,
                "min_energy": 600
            }
            self._save_stt_config()

    def _save_stt_config(self) -> None:
        sf = _get_data_dir() / "stt_config.json"
        try:
            sf.write_text(json.dumps(self._stt_config, ensure_ascii=False, indent=2))
        except Exception:
            pass

    def _trigger_hermes_webhook(self, notif: Dict[str, Any]) -> None:
        if not self._hermes_webhook_url:
            return

        channel_id = str(notif.get("channel_id") or "")
        guild_id = notif.get("guild_id") or ""
        is_dm = not bool(guild_id)
        msg_id = str(notif.get("id") or notif.get("message_id") or "")
        content = str(notif.get("content") or notif.get("text") or "")
        
        author_obj = notif.get("author") or {}
        author_name = author_obj.get("username") if isinstance(author_obj, dict) else str(author_obj)
        author_username = author_name
        author_id = str(notif.get("author_id") or notif.get("user_id") or "")
        reply_to_is_self = bool(notif.get("reply_to_is_self", False))

        should_wake = False
        trigger_reason = ""

        if not self._hermes_wake_words:
            should_wake = True
            trigger_reason = "Любое входящее сообщение (wake_words отключены)"
        else:
            if self._hermes_always_trigger_dm and is_dm:
                should_wake = True
                trigger_reason = "Личное сообщение (DM)"
            elif self._hermes_trigger_on_reply and reply_to_is_self:
                should_wake = True
                trigger_reason = "Ответ на сообщение бота"
            else:
                my_id = str((self._user_info or {}).get("id") or "")
                my_username = (self._user_info or {}).get("username", "").lower()
                if self._hermes_trigger_on_mention and my_id and f"<@{my_id}>" in content:
                    should_wake = True
                    trigger_reason = f"Упоминание <@{my_id}>"
                elif self._hermes_trigger_on_mention and my_username and f"@{my_username}" in content.lower():
                    should_wake = True
                    trigger_reason = f"Упоминание @{my_username}"

            if not should_wake:
                content_lower = content.lower()
                for w in self._hermes_wake_words:
                    if w and w in content_lower:
                        should_wake = True
                        trigger_reason = f"Слово активации '{w}'"
                        break

        # If condition not met, leave in unread cache and do not wake Hermes
        if not should_wake:
            return

        # Collect unread previous messages for this channel as missed context
        unread_items = [
            n for n in self._notifications_cache
            if str(n.get("channel_id")) == channel_id and not n.get("read") and str(n.get("id")) != str(msg_id)
        ]
        unread_items = unread_items[:self._hermes_max_unread_history]
        unread_items.reverse()

        missed_context_formatted = ""
        if unread_items:
            missed_lines = []
            for item in unread_items:
                u_author = item.get("author") or item.get("username") or "user"
                u_text = item.get("content") or item.get("text") or ""
                u_mid = item.get("id") or item.get("message_id") or ""
                missed_lines.append(f"- @{u_author} (id: {u_mid}): {u_text}")
            missed_context_formatted = "\n[Пропущенные непрочитанные сообщения в этом канале]:\n" + "\n".join(missed_lines)

        # Mark processed notifications as read
        for item in unread_items:
            item["read"] = True
        notif["read"] = True
        self._save_notifications_cache()

        # Format reply context cleanly
        reply_context = "Нет"
        reply_info = notif.get("reply_to")
        if isinstance(reply_info, dict) and reply_info:
            r_user = reply_info.get("author_username") or "кто-то"
            r_text = (reply_info.get("content") or "")[:120]
            is_self_tag = " (это сообщение бота)" if reply_info.get("is_self") else ""
            reply_context = f"id={reply_info.get('message_id')}, от @{r_user}{is_self_tag}: '{r_text}'"
        elif notif.get("reply_to_content"):
            reply_context = f"id={notif.get('reply_to_message_id')}, от @{notif.get('reply_to_author')}: '{str(notif.get('reply_to_content'))[:120]}'"

        # Format attachments info
        att_count = notif.get("attachments_count") or len(notif.get("attachments", []))
        att_info = f"{att_count} шт." if att_count else "нет"

        async def _send():
            try:
                payload = {
                    "event_type": notif.get("type", "message"),
                    "guild_id": guild_id,
                    "channel_id": channel_id,
                    "message_id": msg_id,
                    "author": author_name,
                    "author_name": author_name,
                    "author_username": author_username,
                    "author_id": author_id,
                    "trigger_reason": trigger_reason,
                    "reply_context": reply_context,
                    "content": content,
                    "text": content,
                    "attachments_info": att_info,
                    "missed_context_formatted": missed_context_formatted,
                    "missed_messages_count": len(unread_items),
                    "payload": notif,
                }
                async with aiohttp.ClientSession() as session:
                    await session.post(self._hermes_webhook_url, json=payload, timeout=5)
            except Exception:
                pass

        asyncio.create_task(_send())

    def _load_voice_transcripts_cache(self) -> None:
        vf = _get_data_dir() / "voice_transcripts_cache.json"
        if vf.exists():
            try:
                self._voice_transcripts_cache = json.loads(vf.read_text())
            except Exception:
                self._voice_transcripts_cache = []

    def _save_voice_transcripts_cache(self) -> None:
        vf = _get_data_dir() / "voice_transcripts_cache.json"
        try:
            vf.write_text(json.dumps(self._voice_transcripts_cache[:200], ensure_ascii=False, indent=2))
        except Exception:
            pass

    def _add_voice_transcript(self, item: Dict[str, Any]) -> None:
        self._voice_transcripts_cache.insert(0, item)
        self._save_voice_transcripts_cache()
        self._trigger_hermes_webhook(item)
        if self.bus:
            try:
                self.bus.emit("discord.voice_transcript", item)
            except Exception:
                pass

    def _build_presence_payload(self) -> Dict[str, Any]:
        p = self._current_presence or {}
        status = p.get("status", "online")
        act_type_str = p.get("activity_type", "custom").lower()
        act_name = p.get("activity_name", "")
        emoji_name = p.get("emoji", "")

        type_map = {
            "playing": 0,
            "streaming": 1,
            "listening": 2,
            "watching": 3,
            "custom": 4,
            "competing": 5,
        }
        act_type = type_map.get(act_type_str, 4)

        activities = []
        if act_name or emoji_name or act_type_str == "custom":
            act_obj: Dict[str, Any] = {
                "name": "Custom Status" if act_type == 4 else act_name,
                "type": act_type,
            }
            if act_type == 4:
                if act_name:
                    act_obj["state"] = act_name
                if emoji_name:
                    act_obj["emoji"] = {"name": emoji_name}
            else:
                if act_name:
                    act_obj["state"] = act_name

            activities.append(act_obj)

        return {
            "since": None,
            "activities": activities,
            "status": status,
            "afk": status == "idle",
        }

    def _load_notifications_cache(self) -> None:
        nf = _get_data_dir() / "notifications_cache.json"
        if nf.exists():
            try:
                self._notifications_cache = json.loads(nf.read_text())
            except Exception:
                self._notifications_cache = []

    def _save_notifications_cache(self) -> None:
        nf = _get_data_dir() / "notifications_cache.json"
        try:
            # Keep max 200 notifications in persistent cache
            nf.write_text(json.dumps(self._notifications_cache[:200], ensure_ascii=False, indent=2))
        except Exception:
            pass

    def _add_notification(self, notif: Dict[str, Any]) -> None:
        self._notifications_cache.insert(0, notif)
        self._save_notifications_cache()
        self._trigger_hermes_webhook(notif)
        if self.bus:
            try:
                self.bus.emit("discord.notification", notif)
            except Exception:
                pass

    def _load_saved_token(self) -> None:
        tf = _get_token_file()
        if tf.exists():
            try:
                data = json.loads(tf.read_text())
                self._token = data.get("token")
                self._user_info = data.get("user")
            except Exception:
                self._token = None

    @property
    def is_logged_in(self) -> bool:
        return bool(self._token)

    @property
    def is_bot(self) -> bool:
        if self._token and self._token.strip().startswith("Bot "):
            return True
        if self._user_info and self._user_info.get("bot"):
            return True
        return False

    def _get_headers(self) -> Dict[str, str]:
        if not self._token:
            raise RuntimeError("Discord token is not set. Please call discord.login(token) first.")
        token = self._token.strip()
        auth_header = token if token.startswith("Bot ") or token.startswith("Bearer ") else token
        if self.is_bot:
            ua = "DiscordBot (https://github.com/kasper-studios/UnAI, 1.0)"
        else:
            ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 UnAI-Discord/1.0"
        return {
            "Authorization": auth_header,
            "User-Agent": ua,
        }

    async def _api_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
    ) -> Any:
        url = f"{DISCORD_API_BASE}{endpoint}"
        headers = self._get_headers()
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, url, headers=headers, params=params, json=json_data, data=data
            ) as response:
                if response.status == 204:
                    return None
                resp_text = await response.text()
                if response.status >= 400:
                    try:
                        err_json = json.loads(resp_text)
                        msg = err_json.get("message", resp_text)
                    except Exception:
                        msg = resp_text
                    raise RuntimeError(f"Discord API error ({response.status}): {msg}")
                try:
                    return json.loads(resp_text)
                except Exception:
                    return resp_text

    async def _gateway_listener(self) -> None:
        """Background Gateway listener loop receiving live events from Discord Gateway v10."""
        import datetime
        ws_url = "wss://gateway.discord.gg/?v=10&encoding=json"
        token = self._token.strip() if self._token else ""
        if not token:
            self._gateway_connected = False
            return

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        self._gateway_ws = ws
                        self._gateway_connected = True
                        heartbeat_task = None

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                op = data.get("op")
                                t = data.get("t")
                                d = data.get("d", {})
                                s = data.get("s")
                                if s is not None:
                                    self._gateway_last_seq = s

                                # OP 10 Hello -> Send Identify and start heartbeat
                                if op == 10:
                                    interval_ms = d.get("heartbeat_interval", 41250)

                                    async def heartbeat_loop(interval: float):
                                        while True:
                                            await asyncio.sleep(interval)
                                            try:
                                                await ws.send_json({"op": 1, "d": self._gateway_last_seq})
                                            except Exception:
                                                break

                                    heartbeat_task = asyncio.create_task(heartbeat_loop(interval_ms / 1000.0))

                                    identify_payload = {
                                        "op": 2,
                                        "d": {
                                            "token": token,
                                            "intents": 3276799,
                                            "presence": self._build_presence_payload(),
                                            "properties": _resolve_gateway_properties(getattr(self, "_platform", "desktop"), is_bot=self.is_bot)
                                        }
                                    }
                                    await ws.send_json(identify_payload)

                                # OP 1 Heartbeat Request from Gateway -> Respond immediately
                                elif op == 1:
                                    await ws.send_json({"op": 1, "d": self._gateway_last_seq})

                                # OP 0 Dispatch Event
                                elif op == 0:
                                    if t == "READY":
                                        if d.get("user"):
                                            self._user_info = d.get("user")
                                        # Send presence update right after READY
                                        p_payload = {"op": 3, "d": self._build_presence_payload()}
                                        await ws.send_json(p_payload)

                                    my_id = self._user_info.get("id") if self._user_info else None

                                    if t == "MESSAGE_CREATE":
                                        author = d.get("author", {})
                                        author_id = author.get("id")
                                        if my_id and author_id == my_id:
                                            continue  # Ignore self messages

                                        guild_id = d.get("guild_id")
                                        mentions = d.get("mentions", [])
                                        is_dm = guild_id is None
                                        is_mentioned = any(m.get("id") == my_id for m in mentions) if my_id else False

                                        notif_type = "dm" if is_dm else ("mention" if is_mentioned else "message")

                                        referenced_msg = d.get("referenced_message")
                                        reply_info = None
                                        if isinstance(referenced_msg, dict):
                                            ref_author = referenced_msg.get("author", {})
                                            ref_author_id = str(ref_author.get("id") or "")
                                            reply_info = {
                                                "message_id": referenced_msg.get("id"),
                                                "author_id": ref_author_id,
                                                "author_username": ref_author.get("username"),
                                                "author_global_name": ref_author.get("global_name"),
                                                "content": referenced_msg.get("content", ""),
                                                "is_bot": ref_author.get("bot", False),
                                                "is_self": bool(my_id and str(my_id) == ref_author_id),
                                            }

                                        notif = {
                                            "id": d.get("id"),
                                            "type": notif_type,
                                            "channel_id": d.get("channel_id"),
                                            "guild_id": guild_id,
                                            "author": author.get("username"),
                                            "author_global_name": author.get("global_name"),
                                            "author_id": author_id,
                                            "content": d.get("content", ""),
                                            "reply_to": reply_info,
                                            "reply_to_message_id": reply_info.get("message_id") if reply_info else None,
                                            "reply_to_author": reply_info.get("author_username") if reply_info else None,
                                            "reply_to_content": reply_info.get("content") if reply_info else None,
                                            "reply_to_is_self": reply_info.get("is_self") if reply_info else False,
                                            "timestamp": d.get("timestamp"),
                                            "attachments_count": len(d.get("attachments", [])),
                                            "read": False,
                                        }
                                        self._add_notification(notif)

                                    elif t == "VOICE_STATE_UPDATE":
                                        user_id = d.get("user_id")
                                        if my_id and user_id == my_id:
                                            continue
                                        channel_id = d.get("channel_id")
                                        member = d.get("member", {}).get("user", {})
                                        notif = {
                                            "id": f"voice_{user_id}_{channel_id}_{datetime.datetime.now().timestamp()}",
                                            "type": "voice_state",
                                            "user_id": user_id,
                                            "username": member.get("username"),
                                            "channel_id": channel_id,
                                            "guild_id": d.get("guild_id"),
                                            "mute": d.get("mute", False),
                                            "deaf": d.get("deaf", False),
                                            "self_mute": d.get("self_mute", False),
                                            "self_deaf": d.get("self_deaf", False),
                                            "timestamp": datetime.datetime.now().isoformat(),
                                            "read": False,
                                        }
                                        self._add_notification(notif)

                                    elif t == "MESSAGE_REACTION_ADD":
                                        user_id = d.get("user_id")
                                        if my_id and user_id == my_id:
                                            continue
                                        emoji = d.get("emoji", {}).get("name")
                                        notif = {
                                            "id": f"react_{user_id}_{d.get('message_id')}_{emoji}",
                                            "type": "reaction",
                                            "user_id": user_id,
                                            "channel_id": d.get("channel_id"),
                                            "guild_id": d.get("guild_id"),
                                            "message_id": d.get("message_id"),
                                            "emoji": emoji,
                                            "timestamp": datetime.datetime.now().isoformat(),
                                            "read": False,
                                        }
                                        self._add_notification(notif)

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                        if heartbeat_task:
                            heartbeat_task.cancel()

            except Exception:
                pass
            finally:
                self._gateway_connected = False
                self._gateway_ws = None
                await asyncio.sleep(5.0)  # Reconnect delay

    # ====================================================================
    # Auth Tools (ADR-0004)
    # ====================================================================

    @tool(
        "discord.login",
        description="Login to Discord account or bot using a token",
        arguments={
            "token": {"type": "string", "description": "Discord user token or Bot token"}
        },
        enabled_if=lambda ws: not ws.is_logged_in,
    )
    async def login(self, token: str, reason: Optional[str] = None) -> str:
        clean_token = token.strip()
        is_bot_token = clean_token.startswith("Bot ")
        auth_header = clean_token if is_bot_token or clean_token.startswith("Bearer ") else clean_token
        ua = "DiscordBot (https://github.com/kasper-studios/UnAI, 1.0)" if is_bot_token else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 UnAI-Discord/1.0"
        headers = {
            "Authorization": auth_header,
            "User-Agent": ua,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{DISCORD_API_BASE}/users/@me", headers=headers) as resp:
                if resp.status == 401 and not (clean_token.startswith("Bot ") or clean_token.startswith("Bearer ")):
                    bot_auth_header = f"Bot {clean_token}"
                    headers["Authorization"] = bot_auth_header
                    headers["User-Agent"] = "DiscordBot (https://github.com/kasper-studios/UnAI, 1.0)"
                    async with session.get(f"{DISCORD_API_BASE}/users/@me", headers=headers) as resp2:
                        if resp2.status == 200:
                            clean_token = bot_auth_header
                            user_info = await resp2.json()
                        else:
                            txt = await resp.text()
                            raise RuntimeError(f"Failed to authenticate Discord token ({resp.status}): {txt}")
                elif resp.status >= 400:
                    txt = await resp.text()
                    raise RuntimeError(f"Failed to authenticate Discord token ({resp.status}): {txt}")
                else:
                    user_info = await resp.json()

        self._token = clean_token
        self._user_info = user_info
        tf = _get_token_file()
        tf.write_text(json.dumps({"token": clean_token, "user": user_info}))

        username = user_info.get("username", "user")
        discrim = user_info.get("discriminator", "0")
        tag = username if discrim == "0" else f"{username}#{discrim}"
        return f"Successfully logged into Discord as {tag} (ID: {user_info.get('id')})"

    @tool(
        "discord.logout",
        description="Logout from Discord account and clear saved token",
        enabled_if=lambda ws: ws.is_logged_in,
    )
    async def logout(self, reason: Optional[str] = None) -> str:
        self._token = None
        self._user_info = None
        tf = _get_token_file()
        if tf.exists():
            tf.unlink()
        return "Logged out from Discord successfully."

    # ====================================================================
    # Core Discord Tools
    # ====================================================================

    @tool(
        "discord.status",
        description="Get connection status and current logged in Discord user profile",
    )
    async def status(self, reason: Optional[str] = None) -> Dict[str, Any]:
        if not self._token:
            return {"connected": False, "user": None, "info": "Not logged in. Call discord.login(token)"}
        try:
            me = await self._api_request("GET", "/users/@me")
            self._user_info = me
            return {
                "connected": True,
                "user": {
                    "id": me.get("id"),
                    "username": me.get("username"),
                    "global_name": me.get("global_name"),
                    "bot": me.get("bot", False),
                    "email": me.get("email"),
                    "avatar": me.get("avatar"),
                },
                "info": "Connected to Discord API",
            }
        except Exception as e:
            return {"connected": False, "user": None, "error": str(e)}

    @tool(
        "discord.servers.list",
        description="List joined Discord servers (guilds) for the account",
    )
    async def servers_list(self, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        guilds = await self._api_request("GET", "/users/@me/guilds")
        out = []
        for g in guilds:
            out.append({
                "id": g.get("id"),
                "name": g.get("name"),
                "owner": g.get("owner", False),
                "permissions": g.get("permissions"),
                "icon": g.get("icon"),
            })
        return out

    @tool(
        "discord.servers.my_member",
        description="Get your own member profile, nickname, roles, joined_at date, and permissions on a specific server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def servers_my_member(self, guild_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        member = await self._api_request("GET", f"/guilds/{guild_id}/members/@me")
        user = member.get("user", {})
        return {
            "guild_id": guild_id,
            "id": user.get("id"),
            "username": user.get("username"),
            "global_name": user.get("global_name"),
            "nickname": member.get("nick"),
            "roles": member.get("roles", []),
            "joined_at": member.get("joined_at"),
            "premium_since": member.get("premium_since"),
            "permissions": member.get("permissions"),
            "avatar": member.get("avatar"),
            "deaf": member.get("deaf", False),
            "mute": member.get("mute", False),
            "pending": member.get("pending", False),
        }

    @tool(
        "discord.servers.get",
        description="Get detailed information about a Discord server (guild) including owner, description, roles, icon, and member counts",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def servers_get(self, guild_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        g = await self._api_request("GET", f"/guilds/{guild_id}?with_counts=true")
        roles = [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "color": r.get("color"),
                "position": r.get("position"),
                "permissions": r.get("permissions"),
            }
            for r in g.get("roles", [])
        ]
        return {
            "id": g.get("id"),
            "name": g.get("name"),
            "description": g.get("description"),
            "owner_id": g.get("owner_id"),
            "icon": g.get("icon"),
            "banner": g.get("banner"),
            "splash": g.get("splash"),
            "approximate_member_count": g.get("approximate_member_count"),
            "approximate_presence_count": g.get("approximate_presence_count"),
            "preferred_locale": g.get("preferred_locale"),
            "system_channel_id": g.get("system_channel_id"),
            "afk_channel_id": g.get("afk_channel_id"),
            "afk_timeout": g.get("afk_timeout"),
            "roles": roles,
        }

    @tool(
        "discord.servers.update",
        description="Update server (guild) settings such as name, description, icon, banner, system_channel_id, or afk settings (requires MANAGE_GUILD permission)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "name": {"type": "string", "description": "New server name", "default": ""},
            "description": {"type": "string", "description": "New server description", "default": ""},
            "icon_path": {"type": "string", "description": "Local image file path to upload as server icon", "default": ""},
            "banner_path": {"type": "string", "description": "Local image file path to upload as server banner", "default": ""},
            "system_channel_id": {"type": "string", "description": "Channel ID for system messages", "default": ""},
            "afk_channel_id": {"type": "string", "description": "Voice channel ID for AFK users", "default": ""},
            "afk_timeout": {"type": "integer", "description": "AFK timeout in seconds (60, 300, 900, 1800, 3600)", "default": 0}
        },
    )
    async def servers_update(
        self,
        guild_id: str,
        name: str = "",
        description: str = "",
        icon_path: str = "",
        banner_path: str = "",
        system_channel_id: str = "",
        afk_channel_id: str = "",
        afk_timeout: int = 0,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if name:
            payload["name"] = name
        if description:
            payload["description"] = description
        if icon_path:
            payload["icon"] = _file_to_base64_data_uri(icon_path)
        if banner_path:
            payload["banner"] = _file_to_base64_data_uri(banner_path)
        if system_channel_id:
            payload["system_channel_id"] = system_channel_id
        if afk_channel_id:
            payload["afk_channel_id"] = afk_channel_id
        if afk_timeout > 0:
            payload["afk_timeout"] = afk_timeout

        if not payload:
            raise RuntimeError("At least one setting parameter must be specified to update server.")

        g = await self._api_request("PATCH", f"/guilds/{guild_id}", json_data=payload)
        return {
            "id": g.get("id"),
            "name": g.get("name"),
            "description": g.get("description"),
            "icon": g.get("icon"),
            "banner": g.get("banner"),
            "info": "Server settings updated successfully.",
        }

    @tool(
        "discord.channels.create",
        description="Create a new text, voice, or category channel in a server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "name": {"type": "string", "description": "Channel name"},
            "type": {"type": "string", "description": "Channel type: 'text', 'voice', 'category', 'news', 'forum'", "default": "text"},
            "topic": {"type": "string", "description": "Optional channel topic", "default": ""},
            "parent_id": {"type": "string", "description": "Optional category ID to place channel in", "default": ""},
            "nsfw": {"type": "boolean", "description": "Whether channel is NSFW", "default": False}
        },
    )
    async def channels_create(
        self,
        guild_id: str,
        name: str,
        type: str = "text",
        topic: str = "",
        parent_id: str = "",
        nsfw: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        type_str_map = {"text": 0, "voice": 2, "category": 4, "news": 5, "forum": 15}
        ctype = type_str_map.get(type.lower(), 0)

        payload: Dict[str, Any] = {"name": name, "type": ctype}
        if topic:
            payload["topic"] = topic
        if parent_id:
            payload["parent_id"] = parent_id
        if nsfw:
            payload["nsfw"] = nsfw

        c = await self._api_request("POST", f"/guilds/{guild_id}/channels", json_data=payload)
        return {
            "id": c.get("id"),
            "name": c.get("name"),
            "type": type,
            "guild_id": c.get("guild_id"),
            "parent_id": c.get("parent_id"),
            "info": f"Channel '{name}' created successfully in guild '{guild_id}'.",
        }

    @tool(
        "discord.channels.update",
        description="Update channel properties (name, topic, parent_id/category, position, or nsfw)",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID to update"},
            "name": {"type": "string", "description": "New channel name", "default": ""},
            "topic": {"type": "string", "description": "New channel topic", "default": ""},
            "parent_id": {"type": "string", "description": "New category ID", "default": ""},
            "position": {"type": "integer", "description": "New position integer", "default": -1},
            "nsfw": {"type": "boolean", "description": "Set NSFW flag", "default": False}
        },
    )
    async def channels_update(
        self,
        channel_id: str,
        name: str = "",
        topic: str = "",
        parent_id: str = "",
        position: int = -1,
        nsfw: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if name:
            payload["name"] = name
        if topic:
            payload["topic"] = topic
        if parent_id:
            payload["parent_id"] = parent_id
        if position >= 0:
            payload["position"] = position
        if nsfw:
            payload["nsfw"] = nsfw

        if not payload:
            raise RuntimeError("At least one parameter must be specified to update channel.")

        c = await self._api_request("PATCH", f"/channels/{channel_id}", json_data=payload)
        return {
            "id": c.get("id"),
            "name": c.get("name"),
            "topic": c.get("topic"),
            "parent_id": c.get("parent_id"),
            "info": f"Channel '{channel_id}' updated successfully.",
        }

    @tool(
        "discord.channels.delete",
        description="Delete a channel or category from a Discord server",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID to delete"}
        },
    )
    async def channels_delete(self, channel_id: str, reason: Optional[str] = None) -> str:
        await self._api_request("DELETE", f"/channels/{channel_id}")
        return f"Channel '{channel_id}' deleted successfully."

    @tool(
        "discord.channels.list",
        description="List text/voice channels of a specific server (guild) by guild_id, or list all accessible channels across all servers and DMs if guild_id is omitted",
        arguments={
            "guild_id": {"type": "string", "description": "Optional server (guild) ID. If omitted, returns channels from all joined servers and open DMs.", "default": ""}
        },
    )
    async def channels_list(self, guild_id: str = "", reason: Optional[str] = None) -> List[Dict[str, Any]]:
        type_map = {0: "text", 1: "dm", 2: "voice", 3: "group_dm", 4: "category", 5: "news", 11: "public_thread", 12: "private_thread"}
        out = []

        if guild_id:
            channels = await self._api_request("GET", f"/guilds/{guild_id}/channels")
            for c in channels:
                ctype = type_map.get(c.get("type"), str(c.get("type")))
                out.append({
                    "id": c.get("id"),
                    "name": c.get("name", "channel"),
                    "type": ctype,
                    "guild_id": guild_id,
                    "position": c.get("position"),
                    "parent_id": c.get("parent_id"),
                    "topic": c.get("topic"),
                })
            return out

        # If guild_id is omitted, check DMs first
        try:
            dms = await self._api_request("GET", "/users/@me/channels")
            for c in dms:
                ctype = type_map.get(c.get("type"), str(c.get("type")))
                recipients = []
                if "recipients" in c:
                    for r in c["recipients"]:
                        recipients.append({"id": r.get("id"), "username": r.get("username"), "global_name": r.get("global_name")})
                out.append({
                    "id": c.get("id"),
                    "name": c.get("name") or (", ".join(r["username"] for r in recipients) if recipients else "DM"),
                    "type": ctype,
                    "guild_id": None,
                    "guild_name": "Direct Messages",
                    "position": c.get("position"),
                    "parent_id": c.get("parent_id"),
                    "topic": c.get("topic"),
                    "recipients": recipients if recipients else None,
                })
        except Exception:
            pass

        # Then fetch all joined servers and their channels
        try:
            guilds = await self._api_request("GET", "/users/@me/guilds")
            for g in guilds:
                gid = g.get("id")
                gname = g.get("name", "Server")
                try:
                    g_channels = await self._api_request("GET", f"/guilds/{gid}/channels")
                    for c in g_channels:
                        ctype = type_map.get(c.get("type"), str(c.get("type")))
                        out.append({
                            "id": c.get("id"),
                            "name": c.get("name", "channel"),
                            "type": ctype,
                            "guild_id": gid,
                            "guild_name": gname,
                            "position": c.get("position"),
                            "parent_id": c.get("parent_id"),
                            "topic": c.get("topic"),
                        })
                except Exception:
                    pass
        except Exception:
            pass

        return out

    @tool(
        "discord.messages.history",
        description="Get recent message history from a Discord channel with attachments, embeds, and author info",
        arguments={
            "channel_id": {"type": "string", "description": "Channel or DM ID to read messages from"},
            "limit": {"type": "integer", "description": "Number of messages to fetch (max 100)", "default": 50},
            "before": {"type": "string", "description": "Message ID to fetch messages before (for pagination)", "default": ""}
        },
    )
    async def messages_history(
        self, channel_id: str, limit: int = 50, before: str = "", reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params = {"limit": min(limit, 100)}
        if before:
            params["before"] = before

        messages = await self._api_request("GET", f"/channels/{channel_id}/messages", params=params)
        out = []
        for m in messages:
            author = m.get("author", {})
            attachments = []
            for a in m.get("attachments", []):
                attachments.append({
                    "id": a.get("id"),
                    "filename": a.get("filename"),
                    "url": a.get("url"),
                    "size_bytes": a.get("size"),
                    "content_type": a.get("content_type"),
                })
            embeds = []
            for e in m.get("embeds", []):
                embeds.append({
                    "title": e.get("title"),
                    "description": e.get("description"),
                    "url": e.get("url"),
                })
            out.append({
                "id": m.get("id"),
                "channel_id": m.get("channel_id"),
                "author": {
                    "id": author.get("id"),
                    "username": author.get("username"),
                    "global_name": author.get("global_name"),
                    "bot": author.get("bot", False),
                },
                "content": m.get("content", ""),
                "timestamp": m.get("timestamp"),
                "edited_timestamp": m.get("edited_timestamp"),
                "attachments": attachments,
                "embeds": embeds,
                "reply_to_id": m.get("referenced_message", {}).get("id") if m.get("referenced_message") else None,
            })
        return out

    @tool(
        "discord.messages.send",
        description="Send a message to a Discord channel. Supports text, replies, and file attachments.",
        arguments={
            "channel_id": {"type": "string", "description": "Target channel or DM ID"},
            "content": {"type": "string", "description": "Message text content", "default": ""},
            "reply_to": {"type": "string", "description": "Optional message ID to reply to", "default": ""},
            "file_path": {"type": "string", "description": "Optional local file path to attach and upload", "default": ""}
        },
    )
    async def messages_send(
        self,
        channel_id: str,
        content: str = "",
        reply_to: str = "",
        file_path: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if content:
            payload["content"] = content
        if reply_to:
            payload["message_reference"] = {"message_id": reply_to}

        if file_path:
            fp = Path(file_path)
            if not fp.exists():
                raise RuntimeError(f"Attachment file not found: {file_path}")
            data = aiohttp.FormData()
            data.add_field("payload_json", json.dumps(payload))
            data.add_field(
                "files[0]",
                fp.read_bytes(),
                filename=fp.name,
                content_type="application/octet-stream",
            )
            res = await self._api_request("POST", f"/channels/{channel_id}/messages", data=data)
        else:
            if not content:
                raise RuntimeError("Either content or file_path must be provided to send a message.")
            res = await self._api_request("POST", f"/channels/{channel_id}/messages", json_data=payload)

        return {
            "id": res.get("id"),
            "channel_id": res.get("channel_id"),
            "content": res.get("content"),
            "timestamp": res.get("timestamp"),
            "info": "Message sent successfully",
        }

    @tool(
        "discord.files.send_voice",
        description="Upload and send an audio file as a native Discord Voice Note (гска) with waveform to a channel or DM",
        arguments={
            "channel_id": {"type": "string", "description": "Target text channel or DM ID"},
            "file_path": {"type": "string", "description": "Local path to audio file (.ogg, .mp3, .wav, .flac, .m4a)"},
            "reply_to": {"type": "string", "description": "Optional message ID to reply to", "default": ""}
        },
    )
    async def files_send_voice(
        self, channel_id: str, file_path: str, reply_to: str = "", reason: Optional[str] = None
    ) -> Dict[str, Any]:
        p = Path(file_path)
        if not p.exists():
            raise RuntimeError(f"Voice audio file not found: {file_path}")

        engine = self._get_voice_engine()
        ogg_path, duration, waveform = engine.convert_file_to_voice_ogg(p)
        res = await self._send_voice_note(
            channel_id=channel_id,
            ogg_path=ogg_path,
            duration_secs=duration,
            waveform_b64=waveform,
            reply_to=reply_to,
        )
        return {
            "id": res.get("id"),
            "channel_id": res.get("channel_id"),
            "duration_seconds": round(duration, 2),
            "file_path": str(ogg_path),
            "info": f"Voice Note (гска) '{p.name}' sent successfully ({duration:.1f}s).",
        }

    @tool(
        "discord.messages.get",
        description="Get detailed info of a single Discord message by ID",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID"}
        },
    )
    async def messages_get(self, channel_id: str, message_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        try:
            m = await self._api_request("GET", f"/channels/{channel_id}/messages/{message_id}")
        except RuntimeError as e:
            if "Only bots" in str(e) or "403" in str(e):
                msgs = await self._api_request("GET", f"/channels/{channel_id}/messages", params={"limit": 1, "around": message_id})
                if msgs and isinstance(msgs, list):
                    m = msgs[0]
                else:
                    raise e
            else:
                raise e

        author = m.get("author", {})
        return {
            "id": m.get("id"),
            "channel_id": m.get("channel_id"),
            "author": {
                "id": author.get("id"),
                "username": author.get("username"),
                "global_name": author.get("global_name"),
                "bot": author.get("bot", False),
            },
            "content": m.get("content", ""),
            "timestamp": m.get("timestamp"),
            "attachments": m.get("attachments", []),
            "embeds": m.get("embeds", []),
            "reactions": m.get("reactions", []),
        }

    @tool(
        "discord.notifications.list",
        description="List active unread DM channels or recent conversation updates",
    )
    async def notifications_list(self, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        channels = await self._api_request("GET", "/users/@me/channels")
        out = []
        for c in channels:
            last_msg_id = c.get("last_message_id")
            if last_msg_id:
                recipients = c.get("recipients", [])
                out.append({
                    "channel_id": c.get("id"),
                    "name": ", ".join(r.get("username", "") for r in recipients) if recipients else "DM",
                    "last_message_id": last_msg_id,
                    "recipients": [r.get("username") for r in recipients],
                })
        return out[:20]

    @tool(
        "discord.members.list",
        description="Search or list members of a Discord server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Guild (server) ID"},
            "query": {"type": "string", "description": "Optional search term for username/nickname", "default": ""},
            "limit": {"type": "integer", "description": "Max members to return (max 100)", "default": 50}
        },
    )
    async def members_list(
        self, guild_id: str, query: str = "", limit: int = 50, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params = {"limit": min(limit, 100)}
        if query:
            params["query"] = query
            members = await self._api_request("GET", f"/guilds/{guild_id}/members/search", params=params)
        else:
            members = await self._api_request("GET", f"/guilds/{guild_id}/members", params=params)

        out = []
        for m in members:
            u = m.get("user", {})
            out.append({
                "id": u.get("id"),
                "username": u.get("username"),
                "global_name": u.get("global_name"),
                "nickname": m.get("nick"),
                "roles": m.get("roles", []),
                "joined_at": m.get("joined_at"),
            })
        return out

    @tool(
        "discord.members.get",
        description="Get detailed profile of a Discord user by ID",
        arguments={
            "user_id": {"type": "string", "description": "Discord User ID"}
        },
    )
    async def members_get(self, user_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        u = await self._api_request("GET", f"/users/{user_id}")
        return {
            "id": u.get("id"),
            "username": u.get("username"),
            "global_name": u.get("global_name"),
            "discriminator": u.get("discriminator"),
            "avatar": u.get("avatar"),
            "bot": u.get("bot", False),
            "accent_color": u.get("accent_color"),
        }

    @tool(
        "discord.messages.edit",
        description="Edit content of a previously sent Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to edit"},
            "content": {"type": "string", "description": "New message content"}
        },
    )
    async def messages_edit(
        self, channel_id: str, message_id: str, content: str, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        res = await self._api_request("PATCH", f"/channels/{channel_id}/messages/{message_id}", json_data={"content": content})
        return {
            "id": res.get("id"),
            "channel_id": res.get("channel_id"),
            "content": res.get("content"),
            "edited_timestamp": res.get("edited_timestamp"),
            "info": "Message edited successfully",
        }

    @tool(
        "discord.messages.delete",
        description="Delete a Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to delete"}
        },
    )
    async def messages_delete(
        self, channel_id: str, message_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("DELETE", f"/channels/{channel_id}/messages/{message_id}")
        return f"Message '{message_id}' deleted successfully from channel '{channel_id}'."

    @tool(
        "discord.messages.react",
        description="Add an emoji reaction to a Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID"},
            "emoji": {"type": "string", "description": "Emoji to react with (e.g. '👍', '❤️', '🔥', or 'name:id')"}
        },
    )
    async def messages_react(
        self, channel_id: str, message_id: str, emoji: str, reason: Optional[str] = None
    ) -> str:
        import urllib.parse
        encoded_emoji = urllib.parse.quote(emoji)
        await self._api_request("PUT", f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me")
        return f"Reacted with '{emoji}' to message '{message_id}' successfully."

    @tool(
        "discord.messages.unreact",
        description="Remove your emoji reaction from a Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID"},
            "emoji": {"type": "string", "description": "Emoji reaction to remove"}
        },
    )
    async def messages_unreact(
        self, channel_id: str, message_id: str, emoji: str, reason: Optional[str] = None
    ) -> str:
        import urllib.parse
        encoded_emoji = urllib.parse.quote(emoji)
        await self._api_request("DELETE", f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me")
        return f"Removed reaction '{emoji}' from message '{message_id}' successfully."

    @tool(
        "discord.messages.pins",
        description="List pinned messages in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"}
        },
    )
    async def messages_pins(
        self, channel_id: str, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        pins = await self._api_request("GET", f"/channels/{channel_id}/pins")
        out = []
        for m in pins:
            author = m.get("author", {})
            out.append({
                "id": m.get("id"),
                "author": author.get("username"),
                "content": m.get("content", ""),
                "timestamp": m.get("timestamp"),
            })
        return out

    @tool(
        "discord.messages.pin",
        description="Pin a message in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to pin"}
        },
    )
    async def messages_pin(
        self, channel_id: str, message_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("PUT", f"/channels/{channel_id}/pins/{message_id}")
        return f"Message '{message_id}' pinned successfully."

    @tool(
        "discord.messages.unpin",
        description="Unpin a message in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to unpin"}
        },
    )
    async def messages_unpin(
        self, channel_id: str, message_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("DELETE", f"/channels/{channel_id}/pins/{message_id}")
        return f"Message '{message_id}' unpinned successfully."

    @tool(
        "discord.typing",
        description="Trigger typing indicator in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"}
        },
    )
    async def typing(
        self, channel_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("POST", f"/channels/{channel_id}/typing")
        return f"Typing indicator triggered in channel '{channel_id}'."

    @tool(
        "discord.messages.search",
        description="Search messages matching a query in a Discord server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Guild (server) ID"},
            "content": {"type": "string", "description": "Search keyword or text pattern"}
        },
    )
    async def messages_search(
        self, guild_id: str, content: str, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        res = await self._api_request("GET", f"/guilds/{guild_id}/messages/search", params={"content": content})
        messages = res.get("messages", [])
        out = []
        for group in messages:
            for m in group:
                author = m.get("author", {})
                out.append({
                    "id": m.get("id"),
                    "channel_id": m.get("channel_id"),
                    "author": author.get("username"),
                    "content": m.get("content", ""),
                    "timestamp": m.get("timestamp"),
                })
        return out

    @tool(
        "discord.profile.update",
        description="Update account profile details including Display Name (global_name), Bio/About Me, Avatar image, Banner image, or accent color",
        arguments={
            "global_name": {"type": "string", "description": "New Display Name", "default": ""},
            "bio": {"type": "string", "description": "New About Me / Bio text", "default": ""},
            "avatar_path": {"type": "string", "description": "Local image file path to upload as avatar", "default": ""},
            "banner_path": {"type": "string", "description": "Local image file path to upload as banner", "default": ""},
            "accent_color": {"type": "integer", "description": "Integer color code (e.g. 16711680 for red)", "default": 0}
        },
    )
    async def profile_update(
        self,
        global_name: str = "",
        bio: str = "",
        avatar_path: str = "",
        banner_path: str = "",
        accent_color: int = 0,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if global_name:
            payload["global_name"] = global_name
        if bio:
            payload["bio"] = bio
        if avatar_path:
            payload["avatar"] = _file_to_base64_data_uri(avatar_path)
        if banner_path:
            payload["banner"] = _file_to_base64_data_uri(banner_path)
        if accent_color > 0:
            payload["accent_color"] = accent_color

        if not payload:
            raise RuntimeError("At least one profile parameter must be specified to update profile.")

        res = await self._api_request("PATCH", "/users/@me", json_data=payload)
        return {
            "id": res.get("id"),
            "username": res.get("username"),
            "global_name": res.get("global_name"),
            "bio": res.get("bio"),
            "avatar": res.get("avatar"),
            "banner": res.get("banner"),
            "info": "Profile updated successfully.",
        }

    @tool(
        "discord.members.set_nickname",
        description="Change your nickname in a specific Discord server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Guild (server) ID"},
            "nickname": {"type": "string", "description": "New nickname for the server (empty string to reset)"}
        },
    )
    async def members_set_nickname(
        self, guild_id: str, nickname: str = "", reason: Optional[str] = None
    ) -> str:
        await self._api_request("PATCH", f"/guilds/{guild_id}/members/@me", json_data={"nick": nickname})
        new_name = nickname if nickname else "default"
        return f"Server nickname in guild '{guild_id}' changed to '{new_name}' successfully."

    @tool(
        "discord.dm.list",
        description="List all active Direct Message (DM) and Group DM conversations with recipient profiles and last message IDs",
    )
    async def dm_list(self, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        channels = await self._api_request("GET", "/users/@me/channels")
        out = []
        for c in channels:
            ctype = "group_dm" if c.get("type") == 3 else "dm"
            recipients = [
                {
                    "id": r.get("id"),
                    "username": r.get("username"),
                    "global_name": r.get("global_name"),
                    "avatar": r.get("avatar"),
                }
                for r in c.get("recipients", [])
            ]
            out.append({
                "channel_id": c.get("id"),
                "type": ctype,
                "name": c.get("name") or (", ".join(r["username"] for r in recipients) if recipients else "DM"),
                "last_message_id": c.get("last_message_id"),
                "recipients": recipients,
            })
        return out

    @tool(
        "discord.dm.open",
        description="Open or get a Direct Message (DM) channel with a target user by User ID",
        arguments={
            "user_id": {"type": "string", "description": "Target Discord User ID"}
        },
    )
    async def dm_open(self, user_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        dm = await self._api_request("POST", "/users/@me/channels", json_data={"recipient_id": user_id})
        recipients = [
            {"id": r.get("id"), "username": r.get("username"), "global_name": r.get("global_name")}
            for r in dm.get("recipients", [])
        ]
        return {
            "channel_id": dm.get("id"),
            "recipient": recipients[0] if recipients else None,
            "info": f"DM channel opened with user '{user_id}'. Use channel_id to read/send messages.",
        }

    @tool(
        "discord.dm.send",
        description="Send a Direct Message (DM) directly to a target User ID or DM Channel ID",
        arguments={
            "recipient_id": {"type": "string", "description": "Target Discord User ID or DM Channel ID"},
            "content": {"type": "string", "description": "Message text content", "default": ""},
            "reply_to": {"type": "string", "description": "Optional message ID to reply to", "default": ""},
            "file_path": {"type": "string", "description": "Optional local file path to attach and upload", "default": ""}
        },
    )
    async def dm_send(
        self,
        recipient_id: str,
        content: str = "",
        reply_to: str = "",
        file_path: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        target_channel_id = recipient_id
        try:
            dm = await self._api_request("POST", "/users/@me/channels", json_data={"recipient_id": recipient_id})
            if dm and "id" in dm:
                target_channel_id = dm["id"]
        except Exception:
            target_channel_id = recipient_id

        return await self.messages_send(channel_id=target_channel_id, content=content, reply_to=reply_to, file_path=file_path)

    @tool(
        "discord.gateway.connect",
        description="Start real-time WebSocket connection to Discord Gateway v10. Platform selection ('desktop', 'mobile', 'web') applies to user accounts (bots use standard bot properties).",
        arguments={
            "platform": {"type": "string", "description": "Optional client platform for user accounts: 'desktop', 'mobile', 'web', 'console'", "default": "desktop"}
        },
    )
    async def gateway_connect(self, platform: str = "desktop", reason: Optional[str] = None) -> str:
        if not self._token:
            raise RuntimeError("Not logged in. Call discord.login(token) first.")
        self._platform = platform
        props = _resolve_gateway_properties(platform, is_bot=self.is_bot)
        if self._gateway_task and not self._gateway_task.done():
            if self._gateway_ws and not self._gateway_ws.closed:
                payload = {
                    "op": 2,
                    "d": {
                        "token": self._token.strip(),
                        "intents": 3276799,
                        "presence": self._build_presence_payload(),
                        "properties": props,
                    },
                }
                await self._gateway_ws.send_json(payload)
            return "Discord Gateway WebSocket updated for Bot account." if self.is_bot else f"Discord Gateway WebSocket updated to platform '{platform}'."

        self._gateway_task = asyncio.create_task(self._gateway_listener())
        await asyncio.sleep(1.0)
        return "Connected to Discord Gateway WebSocket successfully as Bot." if self.is_bot else f"Connected to Discord Gateway WebSocket successfully with platform '{platform}'."

    @tool(
        "discord.gateway.disconnect",
        description="Disconnect from Discord Gateway WebSocket listener",
    )
    async def gateway_disconnect(self, reason: Optional[str] = None) -> str:
        if self._gateway_task:
            self._gateway_task.cancel()
            self._gateway_task = None
        self._gateway_connected = False
        self._gateway_ws = None
        return "Disconnected from Discord Gateway WebSocket."

    @tool(
        "discord.gateway.status",
        description="Get current status of Discord Gateway live connection and notification cache size",
    )
    async def gateway_status(self, reason: Optional[str] = None) -> Dict[str, Any]:
        unread_count = sum(1 for n in self._notifications_cache if not n.get("read"))
        return {
            "connected": self._gateway_connected,
            "listening": bool(self._gateway_task and not self._gateway_task.done()),
            "cached_notifications_total": len(self._notifications_cache),
            "unread_notifications_count": unread_count,
            "info": "Gateway status active" if self._gateway_connected else "Gateway disconnected. Use discord.gateway.connect to start live stream.",
        }

    @tool(
        "discord.notifications.feed",
        description="Read cached real-time notifications (DMs, mentions, reactions, voice_state events). Automatically marks returned items as read.",
        arguments={
            "unread_only": {"type": "boolean", "description": "Filter to return only unread notifications", "default": True},
            "type_filter": {"type": "string", "description": "Optional type filter: 'dm', 'mention', 'message', 'voice_state', 'reaction'", "default": ""},
            "limit": {"type": "integer", "description": "Max notifications to return (default 20)", "default": 20}
        },
    )
    async def notifications_feed(
        self, unread_only: bool = True, type_filter: str = "", limit: int = 20, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        result = []
        for n in self._notifications_cache:
            if unread_only and n.get("read"):
                continue
            if type_filter and n.get("type") != type_filter.lower():
                continue
            result.append(n)
            n["read"] = True
            if len(result) >= limit:
                break

        self._save_notifications_cache()
        return result

    @tool(
        "discord.notifications.clear",
        description="Clear cached notifications list or delete a specific notification by ID",
        arguments={
            "notification_id": {"type": "string", "description": "Optional specific notification ID to remove. If empty, clears all notifications.", "default": ""}
        },
    )
    async def notifications_clear(self, notification_id: str = "", reason: Optional[str] = None) -> str:
        if notification_id:
            self._notifications_cache = [n for n in self._notifications_cache if n.get("id") != notification_id]
            msg = f"Notification '{notification_id}' cleared."
        else:
            self._notifications_cache.clear()
            msg = "All cached notifications cleared."

        self._save_notifications_cache()
        return msg

    @tool(
        "discord.presence.update",
        description="Update your Discord online status (online, dnd, idle, invisible) and custom activity/status with optional emoji and text",
        arguments={
            "status": {"type": "string", "description": "Online status: 'online', 'dnd' (Do Not Disturb), 'idle', 'invisible'", "default": "online"},
            "activity_type": {"type": "string", "description": "Activity type: 'custom', 'playing', 'listening', 'watching', 'streaming', 'competing'", "default": "custom"},
            "activity_name": {"type": "string", "description": "Custom status text or game/stream name", "default": ""},
            "emoji": {"type": "string", "description": "Optional emoji for custom status (e.g. '🤖', '🔥', '⚡')", "default": ""}
        },
    )
    async def presence_update(
        self,
        status: str = "online",
        activity_type: str = "custom",
        activity_name: str = "",
        emoji: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        valid_statuses = ["online", "dnd", "idle", "invisible"]
        status_clean = status.lower().strip()
        if status_clean not in valid_statuses:
            raise RuntimeError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

        self._current_presence = {
            "status": status_clean,
            "activity_type": activity_type.lower().strip(),
            "activity_name": activity_name,
            "emoji": emoji,
        }

        if self._gateway_ws and not self._gateway_ws.closed:
            payload = {"op": 3, "d": self._build_presence_payload()}
            await self._gateway_ws.send_json(payload)
            info_str = f"Presence updated to '{status_clean}' and broadcasted live over Discord Gateway WebSocket."
        else:
            info_str = f"Presence saved as '{status_clean}'. Will be broadcasted live as soon as discord.gateway.connect is active."

        return {
            "status": status_clean,
            "activity_type": activity_type,
            "activity_name": activity_name,
            "emoji": emoji,
            "gateway_connected": self._gateway_connected,
            "info": info_str,
        }

    @tool(
        "discord.emojis.list",
        description="List custom emojis available on a specific Discord server (guild) with formatted strings for use in messages and reactions",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def emojis_list(self, guild_id: str, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        emojis = await self._api_request("GET", f"/guilds/{guild_id}/emojis")
        out = []
        for e in emojis:
            eid = e.get("id")
            ename = e.get("name")
            animated = e.get("animated", False)
            tag = f"a" if animated else ""
            formatted = f"<{tag}:{ename}:{eid}>"
            reaction_fmt = f"{ename}:{eid}"
            out.append({
                "id": eid,
                "name": ename,
                "animated": animated,
                "available": e.get("available", True),
                "formatted": formatted,
                "reaction_format": reaction_fmt,
                "user": e.get("user", {}).get("username") if e.get("user") else None,
            })
        return out

    @tool(
        "discord.emojis.create",
        description="Create a new custom emoji on a server (guild) by uploading a local image file (requires MANAGE_EMOJIS_AND_STICKERS permission)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "name": {"type": "string", "description": "Name for the custom emoji (alphanumeric and underscores)"},
            "image_path": {"type": "string", "description": "Local image file path (.png, .jpg, .gif)"}
        },
    )
    async def emojis_create(
        self, guild_id: str, name: str, image_path: str, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        b64_image = _file_to_base64_data_uri(image_path)
        payload = {"name": name, "image": b64_image}
        e = await self._api_request("POST", f"/guilds/{guild_id}/emojis", json_data=payload)
        eid = e.get("id")
        ename = e.get("name")
        animated = e.get("animated", False)
        tag = f"a" if animated else ""
        return {
            "id": eid,
            "name": ename,
            "animated": animated,
            "formatted": f"<{tag}:{ename}:{eid}>",
            "info": f"Custom emoji '{ename}' created successfully.",
        }

    @tool(
        "discord.emojis.delete",
        description="Delete a custom emoji from a server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "emoji_id": {"type": "string", "description": "Emoji ID to delete"}
        },
    )
    async def emojis_delete(
        self, guild_id: str, emoji_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("DELETE", f"/guilds/{guild_id}/emojis/{emoji_id}")
        return f"Custom emoji '{emoji_id}' deleted successfully from guild '{guild_id}'."

    # ====================================================================
    # Voice Channel & Neural TTS Tools
    # ====================================================================

    @tool(
        "discord.voice.voices_list",
        description="List available neural TTS voices for speech synthesis (Russian, English, etc.)",
        arguments={
            "language": {"type": "string", "description": "Optional language code filter (e.g. 'ru', 'en', 'de', 'fr', 'ja')", "default": ""}
        },
    )
    async def voice_voices_list(self, language: str = "", reason: Optional[str] = None) -> List[Dict[str, Any]]:
        import edge_tts
        all_voices = await edge_tts.list_voices()
        out = []
        lang_clean = language.lower().strip()
        for v in all_voices:
            short_name = v.get("ShortName", "")
            locale = v.get("Locale", "").lower()
            if lang_clean and not locale.startswith(lang_clean):
                continue
            out.append({
                "short_name": short_name,
                "gender": v.get("Gender"),
                "locale": v.get("Locale"),
                "friendly_name": v.get("FriendlyName"),
            })
        return out

    @tool(
        "discord.voice.join",
        description="Connect to a Discord Voice Channel",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID"},
            "mute": {"type": "boolean", "description": "Connect self-muted", "default": False},
            "deaf": {"type": "boolean", "description": "Connect self-deafened", "default": False}
        },
    )
    async def voice_join(self, channel_id: str, mute: bool = False, deaf: bool = False, reason: Optional[str] = None) -> Dict[str, Any]:
        vc = await self._voice_manager.get_or_connect(self._token, channel_id)
        return {
            "channel_id": str(vc.channel.id),
            "guild_id": str(vc.guild.id),
            "connected": vc.is_connected(),
            "info": f"Connected to Voice Channel '{vc.channel.name}' (ID: {vc.channel.id}).",
        }

    @tool(
        "discord.voice.leave",
        description="Disconnect from a Discord Voice Channel",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID or Server ID"}
        },
    )
    async def voice_leave(self, channel_id: str, reason: Optional[str] = None) -> str:
        client = await self._voice_manager.get_py_client(self._token)
        cid = int(channel_id)
        for g in client.guilds:
            if g.voice_client and (g.id == cid or g.voice_client.channel.id == cid):
                vchannel_name = g.voice_client.channel.name
                await g.voice_client.disconnect(force=True)
                return f"Disconnected from Voice Channel '{vchannel_name}' in guild '{g.name}'."

        return f"No active voice connection found for ID '{channel_id}'."

    @tool(
        "discord.voice.play_file",
        description="Play any local audio or sound file (.mp3, .wav, .ogg, .flac, .m4a) in a Discord Voice Channel",
        arguments={
            "channel_id": {"type": "string", "description": "Target Voice Channel ID or Guild ID"},
            "file_path": {"type": "string", "description": "Local path to audio file"},
            "volume": {"type": "number", "description": "Volume level (0.1 to 2.0)", "default": 1.0}
        },
    )
    async def voice_play_file(
        self,
        channel_id: str,
        file_path: str,
        volume: float = 1.0,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        p = Path(file_path)
        if not p.exists():
            raise RuntimeError(f"Audio file not found: {file_path}")

        vc = await self._voice_manager.get_or_connect(self._token, channel_id)
        if vc.is_playing():
            vc.stop()

        import discord
        source = discord.PCMVolumeTransformer(discord.FFmpegPCMAudio(str(p)), volume=volume)
        vc.play(source)

        return {
            "channel_id": str(vc.channel.id),
            "guild_id": str(vc.guild.id),
            "file_path": str(p),
            "volume": volume,
            "info": f"Playing audio file '{p.name}' in voice channel '{vc.channel.name}'.",
        }

    @tool(
        "discord.voice.tts",
        description="Speak text in a Discord Voice Channel using Voice Micro-DSL (SFX slices from ~/Media/Music, background music, audio effects, pitch, speed) or standard neural TTS",
        arguments={
            "channel_id": {"type": "string", "description": "Target Voice Channel ID or Guild ID"},
            "text": {"type": "string", "description": "Text to synthesize and speak, supports Micro-DSL tags like '{bg:phonk} {sfx:boom} {effect:robot} Привет!'"},
            "voice": {"type": "string", "description": "Neural voice name (e.g. 'ru-RU-DmitryNeural', 'ru-RU-SvetlanaNeural', 'en-US-GuyNeural')", "default": "ru-RU-DmitryNeural"},
            "rate": {"type": "string", "description": "Speed rate adjustment (e.g. '+0%', '+25%', '-15%')", "default": "+0%"},
            "pitch": {"type": "string", "description": "Pitch adjustment (e.g. '+0Hz', '+15Hz', '-20Hz')", "default": "+0Hz"},
            "volume": {"type": "string", "description": "Volume adjustment (e.g. '+0%', '+30%')", "default": "+0%"},
            "auto_mix": {"type": "boolean", "description": "Automatically mix background music and random SFX", "default": False}
        },
    )
    async def voice_tts(
        self,
        channel_id: str,
        text: str,
        voice: str = "ru-RU-DmitryNeural",
        rate: str = "+0%",
        pitch: str = "+0Hz",
        volume: str = "+0%",
        auto_mix: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not text:
            raise RuntimeError("Text parameter is required for TTS synthesis.")

        has_tags = bool(re.search(r"[\{\[][a-zA-Z0-9_\-]+(?::[^\]\}]+)?[\}\]]", text))
        if has_tags or auto_mix:
            engine = self._get_voice_engine()
            ogg_path, duration, meta = await engine.render_to_ogg(
                text=text, default_voice=voice, auto_mix=auto_mix
            )
            res = await self.voice_play_file(channel_id=channel_id, file_path=str(ogg_path))
            res["meta"] = meta
            res["duration_seconds"] = duration
            return res
        else:
            import edge_tts
            import tempfile

            temp_audio = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
            temp_audio_path = temp_audio.name
            temp_audio.close()

            communicate = edge_tts.Communicate(text, voice, rate=rate, pitch=pitch, volume=volume)
            await communicate.save(temp_audio_path)

            res = await self.voice_play_file(channel_id=channel_id, file_path=temp_audio_path)
            res["tts_info"] = {
                "text": text,
                "voice": voice,
                "rate": rate,
                "pitch": pitch,
                "volume": volume,
            }
            return res

    # Voice Synthesis & Micro-DSL Voice Note (ГСки) Tools

    @tool(
        "discord.voice.send",
        description="Synthesize speech with Voice Micro-DSL (SFX timing slices from ~/Media/Music, background music ducking, speed/pitch/effects) and send directly as a native Discord Voice Note (гска) with waveform to a channel or DM",
        arguments={
            "channel_id": {"type": "string", "description": "Target text channel or DM ID"},
            "text": {"type": "string", "description": "Text with Micro-DSL tags (e.g. '{bg:phonk} {sfx:boom} Привет {rate:+30%} мир!')"},
            "voice": {"type": "string", "description": "Default TTS narrator voice (default 'ru-RU-DmitryNeural')", "default": "ru-RU-DmitryNeural"},
            "auto_mix": {"type": "boolean", "description": "Automatically pick and mix background music and sound effects", "default": False},
            "reply_to": {"type": "string", "description": "Optional message ID to reply to", "default": ""}
        },
    )
    async def voice_send(
        self,
        channel_id: str,
        text: str,
        voice: str = "ru-RU-DmitryNeural",
        auto_mix: bool = False,
        reply_to: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not text:
            raise RuntimeError("Text parameter is required for voice synthesis.")

        engine = self._get_voice_engine()
        ogg_path, duration, meta = await engine.render_to_ogg(
            text=text, default_voice=voice, auto_mix=auto_mix
        )

        res = await self._send_voice_note(
            channel_id=channel_id,
            ogg_path=ogg_path,
            duration_secs=duration,
            waveform_b64=meta.get("waveform", ""),
            reply_to=reply_to,
        )

        return {
            "id": res.get("id"),
            "channel_id": res.get("channel_id"),
            "duration_seconds": duration,
            "file_path": str(ogg_path),
            "meta": meta,
            "info": f"Voice note (гска) synthesized and sent successfully ({meta['duration_seconds']}s, bg: {meta['bg_music']}, sfx: {meta['sfx_used']}).",
        }

    @tool(
        "discord.voice.render",
        description="Synthesize Voice Micro-DSL to a local .ogg Opus file with waveform without sending to Discord (returns file path, duration, and metadata)",
        arguments={
            "text": {"type": "string", "description": "Text with Voice Micro-DSL tags to synthesize"},
            "voice": {"type": "string", "description": "Default TTS voice (default 'ru-RU-DmitryNeural')", "default": "ru-RU-DmitryNeural"},
            "auto_mix": {"type": "boolean", "description": "Automatically mix background music and random SFX", "default": False}
        },
    )
    async def voice_render(
        self, text: str, voice: str = "ru-RU-DmitryNeural", auto_mix: bool = False, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        engine = self._get_voice_engine()
        ogg_path, duration, meta = await engine.render_to_ogg(
            text=text, default_voice=voice, auto_mix=auto_mix
        )
        return {
            "file_path": str(ogg_path),
            "duration_seconds": duration,
            "meta": meta,
            "info": f"Rendered {duration}s voice audio file to {ogg_path}",
        }

    @tool(
        "discord.voice.dsl_docs",
        description="Get full reference documentation for the Voice Micro-DSL (tags, timing slice syntax, available voices, effects, and examples)",
    )
    async def voice_dsl_docs(self, reason: Optional[str] = None) -> Dict[str, Any]:
        engine = self._get_voice_engine()
        return engine.get_dsl_documentation()

    @tool(
        "discord.voice.list_media",
        description="List and search sound effects and background music tracks indexed in ~/Media/Music",
        arguments={
            "query": {"type": "string", "description": "Optional search term to filter media", "default": ""},
            "category": {"type": "string", "description": "Optional category filter: 'sfx', 'bg', or 'all'", "default": "all"},
            "force_refresh": {"type": "boolean", "description": "Force rescanning ~/Media/Music from disk", "default": False}
        },
    )
    async def voice_list_media(
        self, query: str = "", category: str = "all", force_refresh: bool = False, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        engine = self._get_voice_engine()
        items = engine.media_lib.refresh(force=force_refresh)
        if query:
            clean_q = engine.media_lib._clean_str(query)
            items = [i for i in items if clean_q in engine.media_lib._clean_str(i["name"])]
        if category and category != "all":
            items = [i for i in items if i["category"] == category]

        return {
            "total": len(items),
            "media_dir": str(engine.media_lib.media_dir),
            "items": items[:50],
        }

    @tool(
        "discord.voice.stop",
        description="Stop current active voice audio/music playback in Voice Channel",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID or Guild ID"}
        },
    )
    async def voice_stop(self, channel_id: str, reason: Optional[str] = None) -> str:
        client = await self._voice_manager.get_py_client(self._token)
        cid = int(channel_id)
        for g in client.guilds:
            if g.voice_client and (g.id == cid or g.voice_client.channel.id == cid):
                self._active_tracks.pop(g.id, None)
                if g.voice_client.is_playing():
                    g.voice_client.stop()
                    return f"Audio playback stopped in Voice Channel '{g.voice_client.channel.name}'."
                return f"No audio is currently playing in Voice Channel '{g.voice_client.channel.name}'."

        return f"No active voice connection found for ID '{channel_id}'."

    @tool(
        "discord.voice.music_status",
        description="Get current playing track metadata, elapsed time, and status in Voice Channel",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID or Guild ID"}
        },
    )
    async def voice_music_status(self, channel_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        client = await self._voice_manager.get_py_client(self._token)
        cid = int(channel_id)
        for g in client.guilds:
            if g.voice_client and (g.id == cid or g.voice_client.channel.id == cid):
                vc = g.voice_client
                is_playing = vc.is_playing()
                track = self._active_tracks.get(g.id, {}) if is_playing else {}
                elapsed = int(time.time() - track.get("start_time", time.time())) if track else 0
                return {
                    "connected": vc.is_connected(),
                    "channel_id": str(vc.channel.id),
                    "channel_name": vc.channel.name,
                    "guild_id": str(g.id),
                    "guild_name": g.name,
                    "is_playing": is_playing,
                    "elapsed_seconds": elapsed,
                    "track": track if is_playing else None,
                    "info": f"Track '{track.get('title', 'Unknown')}' is currently playing ({elapsed}s elapsed)" if is_playing else "No track is currently playing.",
                }
        return {"connected": False, "is_playing": False, "info": f"No active voice connection found for ID '{channel_id}'."}

    # ====================================================================
    # Roles, Server Moderation, Invites & Threads Tools
    # ====================================================================

    @tool(
        "discord.roles.list",
        description="List all roles on a server (guild) with IDs, colors, and permissions",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def roles_list(self, guild_id: str, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        roles = await self._api_request("GET", f"/guilds/{guild_id}/roles")
        return [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "color": r.get("color"),
                "position": r.get("position"),
                "permissions": r.get("permissions"),
                "hoist": r.get("hoist", False),
                "managed": r.get("managed", False),
                "mentionable": r.get("mentionable", False),
            }
            for r in roles
        ]

    @tool(
        "discord.roles.add_to_member",
        description="Assign a role to a server member by User ID and Role ID",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "user_id": {"type": "string", "description": "Member User ID"},
            "role_id": {"type": "string", "description": "Role ID to add"}
        },
    )
    async def roles_add_to_member(self, guild_id: str, user_id: str, role_id: str, reason: Optional[str] = None) -> str:
        await self._api_request("PUT", f"/guilds/{guild_id}/members/{user_id}/roles/{role_id}")
        return f"Role '{role_id}' added to member '{user_id}' in guild '{guild_id}'."

    @tool(
        "discord.roles.remove_from_member",
        description="Remove a role from a server member by User ID and Role ID",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "user_id": {"type": "string", "description": "Member User ID"},
            "role_id": {"type": "string", "description": "Role ID to remove"}
        },
    )
    async def roles_remove_from_member(self, guild_id: str, user_id: str, role_id: str, reason: Optional[str] = None) -> str:
        await self._api_request("DELETE", f"/guilds/{guild_id}/members/{user_id}/roles/{role_id}")
        return f"Role '{role_id}' removed from member '{user_id}' in guild '{guild_id}'."

    @tool(
        "discord.members.kick",
        description="Kick a member from a server (guild) by User ID (requires KICK_MEMBERS permission)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "user_id": {"type": "string", "description": "User ID to kick"},
            "reason": {"type": "string", "description": "Reason for kick", "default": ""}
        },
    )
    async def members_kick(self, guild_id: str, user_id: str, reason: str = "", reason_arg: Optional[str] = None) -> str:
        params = {"reason": reason} if reason else None
        await self._api_request("DELETE", f"/guilds/{guild_id}/members/{user_id}", params=params)
        return f"Member '{user_id}' kicked successfully from guild '{guild_id}'."

    @tool(
        "discord.members.ban",
        description="Ban a user from a server (guild) by User ID (requires BAN_MEMBERS permission)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "user_id": {"type": "string", "description": "User ID to ban"},
            "delete_message_days": {"type": "integer", "description": "Number of days of message history to delete (0 to 7)", "default": 0},
            "reason": {"type": "string", "description": "Reason for ban", "default": ""}
        },
    )
    async def members_ban(
        self, guild_id: str, user_id: str, delete_message_days: int = 0, reason: str = "", reason_arg: Optional[str] = None
    ) -> str:
        payload = {"delete_message_days": max(0, min(delete_message_days, 7))}
        if reason:
            payload["reason"] = reason
        await self._api_request("PUT", f"/guilds/{guild_id}/bans/{user_id}", json_data=payload)
        return f"User '{user_id}' banned successfully from guild '{guild_id}'."

    @tool(
        "discord.members.unban",
        description="Unban a user from a server (guild) by User ID",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "user_id": {"type": "string", "description": "User ID to unban"}
        },
    )
    async def members_unban(self, guild_id: str, user_id: str, reason: Optional[str] = None) -> str:
        await self._api_request("DELETE", f"/guilds/{guild_id}/bans/{user_id}")
        return f"User '{user_id}' unbanned successfully from guild '{guild_id}'."

    @tool(
        "discord.invites.create",
        description="Create an invite link for a channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID to create invite for"},
            "max_age": {"type": "integer", "description": "Duration of invite in seconds (0 for infinite, default 86400 = 24h)", "default": 86400},
            "max_uses": {"type": "integer", "description": "Max number of uses (0 for unlimited)", "default": 0},
            "unique": {"type": "boolean", "description": "Whether to force creation of a new unique invite code", "default": False}
        },
    )
    async def invites_create(
        self, channel_id: str, max_age: int = 86400, max_uses: int = 0, unique: bool = False, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        payload = {"max_age": max_age, "max_uses": max_uses, "unique": unique}
        inv = await self._api_request("POST", f"/channels/{channel_id}/invites", json_data=payload)
        code = inv.get("code")
        return {
            "code": code,
            "url": f"https://discord.gg/{code}",
            "channel_id": channel_id,
            "max_age": inv.get("max_age"),
            "max_uses": inv.get("max_uses"),
            "info": f"Invite link created: https://discord.gg/{code}",
        }

    @tool(
        "discord.invites.list",
        description="List active invite links for a server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def invites_list(self, guild_id: str, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        invites = await self._api_request("GET", f"/guilds/{guild_id}/invites")
        return [
            {
                "code": i.get("code"),
                "url": f"https://discord.gg/{i.get('code')}",
                "channel_id": i.get("channel", {}).get("id"),
                "channel_name": i.get("channel", {}).get("name"),
                "inviter": i.get("inviter", {}).get("username"),
                "uses": i.get("uses"),
                "max_uses": i.get("max_uses"),
            }
            for i in invites
        ]

    @tool(
        "discord.threads.create",
        description="Create a public thread channel under a text channel",
        arguments={
            "channel_id": {"type": "string", "description": "Parent text channel ID"},
            "name": {"type": "string", "description": "Thread name"},
            "auto_archive_duration": {"type": "integer", "description": "Auto archive duration in minutes (60, 1440, 4320, 10080)", "default": 1440}
        },
    )
    async def threads_create(
        self, channel_id: str, name: str, auto_archive_duration: int = 1440, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        payload = {"name": name, "auto_archive_duration": auto_archive_duration, "type": 11}
        th = await self._api_request("POST", f"/channels/{channel_id}/threads", json_data=payload)
        return {
            "id": th.get("id"),
            "name": th.get("name"),
            "parent_id": th.get("parent_id"),
            "guild_id": th.get("guild_id"),
            "info": f"Thread '{name}' created successfully (ID: {th.get('id')}).",
        }

    @tool(
        "discord.threads.list",
        description="List active public threads on a server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def threads_list(self, guild_id: str, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        res = await self._api_request("GET", f"/guilds/{guild_id}/threads/active")
        threads = res.get("threads", [])
        return [
            {
                "id": t.get("id"),
                "name": t.get("name"),
                "parent_id": t.get("parent_id"),
                "message_count": t.get("message_count"),
                "member_count": t.get("member_count"),
            }
            for t in threads
        ]

    @tool(
        "discord.voice.music_configure",
        description="Configure cookies and extractor settings for YouTube / yt-dlp music downloader",
        arguments={
            "cookie_file_path": {"type": "string", "description": "Absolute path to cookies.txt file (Netscape format)", "default": ""},
            "browser": {"type": "string", "description": "Browser to extract cookies from ('chrome', 'firefox', 'brave', 'edge', 'chromium', 'vivaldi')", "default": ""},
            "raw_cookies_content": {"type": "string", "description": "Raw text content of cookies.txt to save automatically to ~/.unai/data/discord/cookies.txt", "default": ""},
            "clear": {"type": "boolean", "description": "Clear saved music cookies and browser configuration", "default": False}
        },
    )
    async def voice_music_configure(
        self,
        cookie_file_path: str = "",
        browser: str = "",
        raw_cookies_content: str = "",
        clear: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        if clear:
            self._music_config = {}
            self._save_music_config()
            cf = _get_data_dir() / "cookies.txt"
            if cf.exists():
                cf.unlink()
            return {"configured": False, "info": "Music cookie settings cleared."}

        if raw_cookies_content:
            cf = _get_data_dir() / "cookies.txt"
            cf.write_text(raw_cookies_content.strip())
            self._music_config["cookiefile"] = str(cf)

        if cookie_file_path:
            p = Path(cookie_file_path)
            if not p.exists():
                raise RuntimeError(f"Cookie file not found at: {cookie_file_path}")
            self._music_config["cookiefile"] = str(p.resolve())

        if browser:
            self._music_config["browser"] = browser.lower().strip()

        self._save_music_config()
        return {
            "configured": True,
            "cookiefile": self._music_config.get("cookiefile"),
            "browser": self._music_config.get("browser"),
            "info": "YouTube music cookies configured successfully. yt-dlp will use these settings for audio downloading.",
        }

    @tool(
        "discord.voice.play_music",
        description="Search, download (to /tmp), and play music or audio tracks from YouTube or URL directly in a Discord Voice Channel (supports custom cookies, browser cookies, and request_id idempotency)",
        arguments={
            "channel_id": {"type": "string", "description": "Target Voice Channel ID or Guild ID"},
            "query_or_url": {"type": "string", "description": "Search query or YouTube/Soundcloud URL to play"},
            "volume": {"type": "number", "description": "Volume level (0.1 to 2.0)", "default": 1.0},
            "cookie_file": {"type": "string", "description": "Optional custom cookies.txt path for this playback", "default": ""},
            "browser": {"type": "string", "description": "Optional browser to read cookies from for this playback", "default": ""},
            "request_id": {"type": "string", "description": "Optional unique request ID / idempotency key to prevent duplicate playback on retries", "default": ""},
            "force_restart": {"type": "boolean", "description": "Whether to force restart track if already playing", "default": False}
        },
    )
    async def voice_play_music(
        self,
        channel_id: str,
        query_or_url: str,
        volume: float = 1.0,
        cookie_file: str = "",
        browser: str = "",
        request_id: str = "",
        force_restart: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        import yt_dlp
        import uuid
        import asyncio

        if not query_or_url:
            raise RuntimeError("query_or_url parameter is required.")

        vc = await self._voice_manager.get_or_connect(self._token, channel_id)
        gid = vc.guild.id

        # Idempotency check: If already playing this track / request in this guild
        if vc.is_playing() and not force_restart:
            curr = self._active_tracks.get(gid)
            if curr:
                same_req = request_id and curr.get("request_id") == request_id
                same_query = curr.get("query_or_url") == query_or_url.strip() or curr.get("url") == query_or_url.strip()
                if same_req or same_query:
                    elapsed = int(time.time() - curr.get("start_time", time.time()))
                    return {
                        "status": "already_playing",
                        "channel_id": str(vc.channel.id),
                        "channel_name": vc.channel.name,
                        "guild_id": str(gid),
                        "guild_name": vc.guild.name,
                        "track_meta": curr,
                        "elapsed_seconds": elapsed,
                        "volume": volume,
                        "info": f"Track '{curr.get('title', 'Audio')}' is already playing in '{vc.channel.name}' ({elapsed}s elapsed). Use force_restart=true to restart.",
                    }

        track_id = str(uuid.uuid4())[:8]
        out_tmpl = f"/tmp/unai_music_{track_id}.%(ext)s"
        ydl_opts: Dict[str, Any] = {
            "format": "bestaudio/best",
            "outtmpl": out_tmpl,
            "quiet": True,
            "no_warnings": True,
            "max_filesize": 100 * 1024 * 1024,
            "remote_components": ["ejs:github"],
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
        }

        # Resolve cookies: 1) direct param -> 2) music_config.json -> 3) auto-detect in ~/.unai/data/discord/
        active_cookie_file = cookie_file or self._music_config.get("cookiefile")
        if not active_cookie_file:
            for cand in ["cookies.txt", "youtube_cookies.txt", "yt_cookies.txt"]:
                cand_path = _get_data_dir() / cand
                if cand_path.exists():
                    active_cookie_file = str(cand_path)
                    break

        if active_cookie_file and Path(active_cookie_file).exists():
            ydl_opts["cookiefile"] = active_cookie_file

        active_browser = browser or self._music_config.get("browser")
        if active_browser and not active_cookie_file:
            ydl_opts["cookiesfrombrowser"] = (active_browser,)

        target = query_or_url if query_or_url.startswith("http") else f"ytsearch1:{query_or_url}"

        loop = asyncio.get_event_loop()
        def _download():
            import contextlib
            import io
            with contextlib.redirect_stdout(io.StringIO()):
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(target, download=True)
                    if "entries" in info and len(info["entries"]) > 0:
                        entry = info["entries"][0]
                    else:
                        entry = info
                    return {
                        "title": entry.get("title", "Track"),
                        "uploader": entry.get("uploader", "Unknown"),
                        "duration": entry.get("duration", 0),
                        "url": entry.get("webpage_url", query_or_url),
                        "file_path": f"/tmp/unai_music_{track_id}.mp3",
                        "cookies_used": bool(ydl_opts.get("cookiefile") or ydl_opts.get("cookiesfrombrowser")),
                    }

        track_meta = await loop.run_in_executor(None, _download)
        res = await self.voice_play_file(channel_id=channel_id, file_path=track_meta["file_path"], volume=volume)

        # Track active playback
        track_meta["query_or_url"] = query_or_url.strip()
        track_meta["request_id"] = request_id or track_id
        track_meta["start_time"] = time.time()
        track_meta["volume"] = volume
        self._active_tracks[gid] = track_meta

        res["track_meta"] = track_meta
        return res

    @tool(
        "discord.voice.stt_configure",
        description="Configure Speech-to-Text (STT) Whisper API endpoint and VAD sensitivity",
        arguments={
            "provider": {"type": "string", "description": "STT Provider: 'omniroute', 'openai_compatible', 'groq', 'google'", "default": "omniroute"},
            "api_base": {"type": "string", "description": "OpenAI-compatible Whisper API base URL (default: 'http://localhost:20128/v1')", "default": "http://localhost:20128/v1"},
            "api_key": {"type": "string", "description": "API Key for Whisper service", "default": "omniroute"},
            "model": {"type": "string", "description": "Whisper Model ID (e.g. 'groq/whisper-large-v3-turbo', 'groq/whisper-large-v3', 'whisper-1')", "default": "groq/whisper-large-v3-turbo"},
            "language": {"type": "string", "description": "Default language code ('ru', 'en')", "default": "ru"},
            "vad_mode": {"type": "integer", "description": "WebRTC VAD aggressiveness mode (0 = least aggressive, 3 = most aggressive)", "default": 2},
            "min_energy": {"type": "integer", "description": "Minimum audio RMS energy for speech gate", "default": 600}
        },
    )
    async def voice_stt_configure(
        self,
        provider: str = "omniroute",
        api_base: str = "http://localhost:20128/v1",
        api_key: str = "omniroute",
        model: str = "groq/whisper-large-v3-turbo",
        language: str = "ru",
        vad_mode: int = 2,
        min_energy: int = 600,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._stt_config = {
            "provider": provider.lower().strip(),
            "api_base": api_base.strip(),
            "api_key": api_key.strip(),
            "model": model.strip(),
            "language": language.strip(),
            "vad_mode": int(vad_mode),
            "min_energy": int(min_energy),
        }
        self._save_stt_config()
        return {
            "configured": True,
            "stt_config": self._stt_config,
            "info": f"STT configured successfully to use provider '{provider}' with model '{model}' at '{api_base}' (VAD mode={vad_mode}, min_energy={min_energy}).",
        }

    @tool(
        "discord.voice.listen_start",
        description="Start listening and transcribing voice audio in a Voice Channel using Speech-to-Text (STT)",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID"},
            "language": {"type": "string", "description": "Language code for speech recognition (e.g. 'ru-RU', 'en-US')", "default": "ru-RU"}
        },
    )
    async def voice_listen_start(self, channel_id: str, language: str = "ru-RU", reason: Optional[str] = None) -> Dict[str, Any]:
        return await self._voice_manager.start_listening(
            token=self._token,
            channel_id_str=channel_id,
            on_transcript_cb=self._add_voice_transcript,
            language=language,
            stt_config=self._stt_config,
        )

    @tool(
        "discord.voice.listen_stop",
        description="Stop active Voice Listener in a Voice Channel or Guild",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID or Guild ID"}
        },
    )
    async def voice_listen_stop(self, channel_id: str, reason: Optional[str] = None) -> str:
        return await self._voice_manager.stop_listening(
            token=self._token,
            channel_id_str=channel_id,
        )

    @tool(
        "discord.voice.listen_status",
        description="Get diagnostic statistics of active Voice Listener (packets received, PCM bytes, transcripts generated, active speakers)",
        arguments={
            "channel_id": {"type": "string", "description": "Voice Channel ID or Guild ID"}
        },
    )
    async def voice_listen_status(self, channel_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        return await self._voice_manager.get_status(
            token=self._token,
            channel_id_str=channel_id,
        )

    @tool(
        "discord.voice.transcripts_feed",
        description="Read transcribed voice speech items captured from active Voice Channel listeners. Automatically marks items as read.",
        arguments={
            "unread_only": {"type": "boolean", "description": "Return only unread transcripts", "default": True},
            "limit": {"type": "integer", "description": "Max items to return (default 20)", "default": 20}
        },
    )
    async def voice_transcripts_feed(self, unread_only: bool = True, limit: int = 20, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        res = []
        for item in self._voice_transcripts_cache:
            if unread_only and item.get("read"):
                continue
            res.append(item)
            item["read"] = True
            if len(res) >= limit:
                break

        self._save_voice_transcripts_cache()
        return res

    @tool(
        "discord.voice.transcripts_clear",
        description="Clear cached voice speech transcripts queue",
    )
    async def voice_transcripts_clear(self, reason: Optional[str] = None) -> str:
        self._voice_transcripts_cache.clear()
        self._save_voice_transcripts_cache()
        return "Voice speech transcripts cleared."

    # ====================================================================
    # Hermes Webhook Auto-Trigger Tools
    # ====================================================================

    @tool(
        "discord.webhook.subscribe_hermes",
        description="Subscribe Hermes CLI to incoming Discord messages with wake-word filtering and unread context aggregation",
        arguments={
            "route_name": {"type": "string", "description": "Hermes webhook route name (e.g. 'discord-inbound')", "default": "discord-inbound"},
            "wake_words": {"type": "string", "description": "Comma-separated wake words (e.g. 'диром, dirom'). If empty, triggers on all messages.", "default": "диром, dirom"},
            "always_trigger_in_dm": {"type": "boolean", "description": "Always wake Hermes for direct 1-on-1 private messages without requiring wake word", "default": True},
            "trigger_on_reply": {"type": "boolean", "description": "Wake Hermes when a user replies to the bot's message", "default": True},
            "trigger_on_mention": {"type": "boolean", "description": "Wake Hermes when @bot or <@bot_id> is mentioned", "default": True},
            "max_unread_history": {"type": "integer", "description": "Max number of unread missed messages from this channel to include as context on wake", "default": 10},
            "prompt": {
                "type": "string",
                "description": "Prompt template with {payload...} fields",
                "default": (
                    "[Hermes Webhook: Входящее сообщение Discord]\n"
                    "Сервер ID: {payload.guild_id}\n"
                    "Канал ID: {payload.channel_id}\n"
                    "От: {payload.author_name} (@{payload.author_username}, ID: {payload.author_id})\n"
                    "ID сообщения: {payload.message_id}\n"
                    "Причина активации: {payload.trigger_reason}\n"
                    "Ответ на: {payload.reply_context}\n"
                    "Текст: {payload.content}\n"
                    "Вложения: {payload.attachments_info}\n"
                    "{payload.missed_context_formatted}"
                )
            },
            "deliver": {"type": "string", "description": "Delivery target: 'log', 'telegram', 'discord', 'origin'", "default": "log"},
            "hermes_port": {"type": "integer", "description": "Port of Hermes webhook daemon (default 8644)", "default": 8644},
            "auto_trigger": {"type": "boolean", "description": "Enable auto HTTP POSTing payloads to Hermes webhook when new messages arrive", "default": True}
        },
    )
    async def webhook_subscribe_hermes(
        self,
        route_name: str = "discord-inbound",
        wake_words: str = "диром, dirom",
        always_trigger_in_dm: bool = True,
        trigger_on_reply: bool = True,
        trigger_on_mention: bool = True,
        max_unread_history: int = 10,
        prompt: Optional[str] = None,
        deliver: str = "log",
        hermes_port: int = 8644,
        auto_trigger: bool = True,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        import shutil
        import subprocess

        # Parse wake words
        if wake_words:
            self._hermes_wake_words = [w.strip().lower() for w in wake_words.split(",") if w.strip()]
        else:
            self._hermes_wake_words = []

        self._hermes_always_trigger_dm = bool(always_trigger_in_dm)
        self._hermes_trigger_on_reply = bool(trigger_on_reply)
        self._hermes_trigger_on_mention = bool(trigger_on_mention)
        self._hermes_max_unread_history = max(1, int(max_unread_history))

        if not prompt:
            prompt = (
                "[Hermes Webhook: Входящее сообщение Discord]\n"
                "Сервер ID: {payload.guild_id}\n"
                "Канал ID: {payload.channel_id}\n"
                "От: {payload.author_name} (@{payload.author_username}, ID: {payload.author_id})\n"
                "ID сообщения: {payload.message_id}\n"
                "Причина активации: {payload.trigger_reason}\n"
                "Ответ на: {payload.reply_context}\n"
                "Текст: {payload.content}\n"
                "Вложения: {payload.attachments_info}\n"
                "{payload.missed_context_formatted}"
            )

        hermes_bin = shutil.which("hermes") or "/home/kasperenok/.local/bin/hermes"
        cmd = [
            hermes_bin,
            "webhook",
            "subscribe",
            route_name,
            "--prompt",
            prompt,
            "--deliver",
            deliver,
        ]

        sub_output = ""
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            sub_output = res.stdout or res.stderr
        except Exception as e:
            sub_output = f"Hermes CLI call error: {e}"

        target_url = f"http://127.0.0.1:{hermes_port}/webhooks/{route_name}"
        if auto_trigger:
            self._hermes_webhook_url = target_url

        return {
            "route_name": route_name,
            "webhook_url": target_url,
            "wake_words": self._hermes_wake_words,
            "always_trigger_in_dm": self._hermes_always_trigger_dm,
            "trigger_on_reply": self._hermes_trigger_on_reply,
            "trigger_on_mention": self._hermes_trigger_on_mention,
            "max_unread_history": self._hermes_max_unread_history,
            "auto_trigger_enabled": auto_trigger,
            "cli_command": " ".join(cmd),
            "cli_output": sub_output.strip(),
            "info": f"Subscribed Hermes webhook route '{route_name}'. Wake words: {self._hermes_wake_words or 'ALL'}. Auto-trigger URL: {target_url}",
        }
