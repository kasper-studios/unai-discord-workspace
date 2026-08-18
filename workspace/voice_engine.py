"""Voice Engine & Micro-DSL for UnAI Discord Workspace.

Provides text-to-speech synthesis (edge-tts), media library indexing (~/Media/Music),
timing slice extraction for sound effects, audio effects (robot, radio, echo, etc.),
background music ducking/overlay, Discord Voice (.ogg Opus) compilation via FFmpeg,
and 256-sample Base64 waveform generation for native Discord Voice Notes (гски).
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import struct
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple
import base64

try:
    import edge_tts
except ImportError:
    edge_tts = None


DEFAULT_MEDIA_DIR = Path.home() / "Media" / "Music"
DEFAULT_VOICE = "ru-RU-DmitryNeural"

AVAILABLE_VOICES: Dict[str, str] = {
    "dmitry": "ru-RU-DmitryNeural",
    "svetlana": "ru-RU-SvetlanaNeural",
    "dmitryneural": "ru-RU-DmitryNeural",
    "svetlananeural": "ru-RU-SvetlanaNeural",
    "en_guy": "en-US-GuyNeural",
    "en_jenny": "en-US-JennyNeural",
    "en_aria": "en-US-AriaNeural",
    "uk_ostap": "uk-UA-OstapNeural",
    "uk_polina": "uk-UA-PolinaNeural",
}

AVAILABLE_EFFECTS: Dict[str, str] = {
    "robot": "asetrate=38000,aresample=48000,flanger=delay=5:depth=2:regen=50:width=80:speed=0.5",
    "radio": "highpass=f=500,lowpass=f=2800,volume=1.4",
    "echo": "aecho=0.8:0.88:60:0.4",
    "distort": "acrusher=bits=8:samples=16:mode=log,volume=1.2",
    "bass": "bass=g=14:f=120:w=0.6",
    "reverb": "aecho=0.8:0.9:1000:0.3",
    "pitch_up": "asetrate=48000*1.3,aresample=48000",
    "pitch_down": "asetrate=48000*0.75,aresample=48000",
    "phone": "bandpass=f=1200:width_type=h:w=1400,volume=1.5",
}


@dataclass
class SfxSpec:
    name: str
    file_path: Optional[Path] = None
    start: float = 0.0
    duration: Optional[float] = None
    volume: float = 1.0


@dataclass
class BgSpec:
    name: str
    file_path: Optional[Path] = None
    start: float = 0.0
    volume: float = 0.18
    enabled: bool = True


@dataclass
class SpeechSegment:
    text: str
    voice: str = DEFAULT_VOICE
    rate: str = "+0%"
    pitch: str = "+0Hz"
    effect: Optional[str] = None


@dataclass
class SfxSegment:
    sfx: SfxSpec


@dataclass
class PauseSegment:
    duration: float = 0.5


@dataclass
class ParsedVoiceScript:
    segments: List[Any] = field(default_factory=list)
    bg: Optional[BgSpec] = None
    default_voice: str = DEFAULT_VOICE


class MediaLibrary:
    """Indexes and caches media files in ~/Media/Music for fast fuzzy search."""

    def __init__(self, media_dir: Optional[Path] = None):
        self.media_dir = media_dir or DEFAULT_MEDIA_DIR
        self._cache_file = Path.home() / ".unai" / "data" / "discord" / "media_cache.json"
        self._items: List[Dict[str, Any]] = []
        self._cache_time: float = 0.0
        self._load_disk_cache()

    def _load_disk_cache(self) -> None:
        if self._cache_file.exists():
            try:
                data = json.loads(self._cache_file.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    self._items = data
                    self._cache_time = time.time()
            except Exception:
                pass

    def _save_disk_cache(self) -> None:
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            self._cache_file.write_text(json.dumps(self._items, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def refresh(self, force: bool = False) -> List[Dict[str, Any]]:
        now = time.time()
        if not force and self._items and (now - self._cache_time < 30.0):
            return self._items

        if not self.media_dir.exists():
            self._items = []
            return []

        supported_exts = {".mp3", ".ogg", ".wav", ".m4a", ".flac", ".aac", ".opus", ".wma"}
        cached_by_path = {i["path"]: i for i in self._items if "path" in i}

        files_to_probe = []
        new_items = []

        for p in self.media_dir.iterdir():
            if p.is_file() and p.suffix.lower() in supported_exts:
                p_str = str(p.resolve())
                mtime = p.stat().st_mtime
                size = p.stat().st_size

                cached = cached_by_path.get(p_str)
                if cached and cached.get("mtime") == mtime and cached.get("size") == size and "duration" in cached:
                    new_items.append(cached)
                else:
                    files_to_probe.append((p, mtime, size))

        if files_to_probe:
            with ThreadPoolExecutor(max_workers=8) as pool:
                probe_results = list(pool.map(lambda item: self._probe_item(*item), files_to_probe))
            new_items.extend(probe_results)

        self._items = sorted(new_items, key=lambda x: x["name"].lower())
        self._cache_time = now
        self._save_disk_cache()
        return self._items

    def _probe_item(self, p: Path, mtime: float, size: int) -> Dict[str, Any]:
        dur = self._probe_duration(p)
        cat = "sfx" if dur < 10.0 else "bg"
        return {
            "name": p.stem,
            "filename": p.name,
            "path": str(p.resolve()),
            "duration": round(dur, 2),
            "category": cat,
            "ext": p.suffix.lower(),
            "mtime": mtime,
            "size": size,
        }

    def _probe_duration(self, path: Path) -> float:
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            if res.returncode == 0 and res.stdout.strip():
                return float(res.stdout.strip())
        except Exception:
            pass
        return 0.0

    def find(self, query: str, category: Optional[str] = None) -> Optional[Dict[str, Any]]:
        items = self.refresh()
        if not items:
            return None

        clean_q = self._clean_str(query)
        filtered = items
        if category and category != "all":
            filtered = [i for i in items if i["category"] == category]
            if not filtered:
                filtered = items

        # 1. Exact match
        for item in filtered:
            if self._clean_str(item["name"]) == clean_q:
                return item

        # 2. Substring match
        for item in filtered:
            if clean_q in self._clean_str(item["name"]):
                return item

        # 3. Word match
        q_words = [w for w in clean_q.split() if len(w) > 2]
        if q_words:
            for item in filtered:
                item_clean = self._clean_str(item["name"])
                if any(w in item_clean for w in q_words):
                    return item

        return None

    def random_sample(self, category: str = "sfx") -> Optional[Dict[str, Any]]:
        items = self.refresh()
        filtered = [i for i in items if i["category"] == category]
        if not filtered:
            filtered = items
        return random.choice(filtered) if filtered else None

    def _clean_str(self, s: str) -> str:
        s = s.lower()
        s = re.sub(r"[^\w\sа-яёА-ЯЁ]", " ", s)
        return " ".join(s.split())


class VoiceDslParser:
    """Parses Micro-DSL syntax in text strings into structured audio segments."""

    def __init__(self, media_lib: MediaLibrary):
        self.media_lib = media_lib

    def parse(self, raw_text: str, default_voice: str = DEFAULT_VOICE, auto_mix: bool = False) -> ParsedVoiceScript:
        text = raw_text.strip()
        script = ParsedVoiceScript(default_voice=default_voice)

        current_voice = default_voice
        current_rate = "+0%"
        current_pitch = "+0Hz"
        current_effect: Optional[str] = None
        global_bg_vol = 0.18

        tag_pattern = re.compile(r"[\{\[]([a-zA-Z0-9_\-]+)(?::([^\]\}]+))?[\}\]]")
        
        has_explicit_sfx = False
        has_explicit_bg = False

        pos = 0
        tokens = []
        for match in tag_pattern.finditer(text):
            start, end = match.span()
            if start > pos:
                chunk = text[pos:start].strip()
                if chunk:
                    tokens.append(("text", chunk))
            tag_name = match.group(1).lower()
            tag_val = (match.group(2) or "").strip()
            tokens.append(("tag", tag_name, tag_val))
            pos = end

        if pos < len(text):
            chunk = text[pos:].strip()
            if chunk:
                tokens.append(("text", chunk))

        for tok in tokens:
            if tok[0] == "tag":
                tag_name = tok[1]
                tag_val = tok[2]

                if tag_name in ("bg", "music", "background"):
                    has_explicit_bg = True
                    bg_spec = self._parse_bg_tag(tag_val, global_bg_vol)
                    if bg_spec:
                        script.bg = bg_spec

                elif tag_name in ("bg_vol", "vol"):
                    try:
                        v_str = tag_val.replace("%", "").strip()
                        v_num = float(v_str)
                        if "%" in tag_val or v_num > 2.0:
                            v_num = v_num / 100.0
                        global_bg_vol = max(0.0, min(2.0, v_num))
                        if script.bg:
                            script.bg.volume = global_bg_vol
                    except Exception:
                        pass

                elif tag_name in ("sfx", "sound", "sample"):
                    has_explicit_sfx = True
                    sfx_spec = self._parse_sfx_tag(tag_val)
                    if sfx_spec:
                        script.segments.append(SfxSegment(sfx=sfx_spec))

                elif tag_name in ("pause", "sleep", "wait"):
                    try:
                        dur = float(tag_val.lower().replace("s", "").replace("сек", "").strip() or 0.5)
                        script.segments.append(PauseSegment(duration=max(0.1, min(10.0, dur))))
                    except Exception:
                        script.segments.append(PauseSegment(duration=0.5))

                elif tag_name in ("voice", "speaker"):
                    v_key = tag_val.lower().strip()
                    if v_key in AVAILABLE_VOICES:
                        current_voice = AVAILABLE_VOICES[v_key]
                    elif "neural" in v_key:
                        current_voice = tag_val.strip()

                elif tag_name in ("rate", "speed"):
                    current_rate = self._format_rate(tag_val)

                elif tag_name == "pitch":
                    current_pitch = self._format_pitch(tag_val)

                elif tag_name in ("effect", "fx", "filter"):
                    eff = tag_val.lower().strip()
                    if eff in ("none", "off", "clear"):
                        current_effect = None
                    elif eff in AVAILABLE_EFFECTS:
                        current_effect = eff

            elif tok[0] == "text":
                content = tok[1]
                if content:
                    script.segments.append(
                        SpeechSegment(
                            text=content,
                            voice=current_voice,
                            rate=current_rate,
                            pitch=current_pitch,
                            effect=current_effect,
                        )
                    )

        if auto_mix:
            if not has_explicit_bg:
                bg_sample = self.media_lib.random_sample(category="bg")
                if bg_sample:
                    script.bg = BgSpec(
                        name=bg_sample["name"],
                        file_path=Path(bg_sample["path"]),
                        start=0.0,
                        volume=global_bg_vol,
                        enabled=True,
                    )

            if not has_explicit_sfx and len(script.segments) == 1 and isinstance(script.segments[0], SpeechSegment):
                seg = script.segments[0]
                phrases = re.split(r"([.!?,;\n]+)", seg.text)
                new_segments = []
                accum = ""
                for part in phrases:
                    accum += part
                    if re.match(r"[.!?,;\n]+", part) and len(accum.strip()) > 15:
                        new_segments.append(
                            SpeechSegment(
                                text=accum.strip(),
                                voice=seg.voice,
                                rate=seg.rate,
                                pitch=seg.pitch,
                                effect=seg.effect,
                            )
                        )
                        accum = ""
                        if random.random() < 0.5:
                            sfx_item = self.media_lib.random_sample(category="sfx")
                            if sfx_item:
                                new_segments.append(
                                    SfxSegment(
                                        sfx=SfxSpec(
                                            name=sfx_item["name"],
                                            file_path=Path(sfx_item["path"]),
                                        )
                                    )
                                )
                if accum.strip():
                    new_segments.append(
                        SpeechSegment(
                            text=accum.strip(),
                            voice=seg.voice,
                            rate=seg.rate,
                            pitch=seg.pitch,
                            effect=seg.effect,
                        )
                    )
                if new_segments:
                    script.segments = new_segments

        return script

    def _parse_sfx_tag(self, val: str) -> Optional[SfxSpec]:
        """Parses sfx name and timing/volume parameters."""
        if not val or val.lower() in ("random", "?"):
            item = self.media_lib.random_sample(category="sfx")
            if not item:
                return None
            return SfxSpec(name=item["name"], file_path=Path(item["path"]))

        parts = [p.strip() for p in val.split(":") if p.strip()]
        name = parts[0]
        start = 0.0
        duration = None
        vol = 1.0

        for p in parts[1:]:
            if "=" in p:
                k, v = p.split("=", 1)
                k = k.lower().strip()
                v_clean = v.lower().replace("s", "").replace("сек", "").strip()
                if k in ("start", "offset", "from"):
                    try:
                        start = max(0.0, float(v_clean))
                    except Exception:
                        pass
                elif k in ("len", "duration", "length", "time"):
                    try:
                        duration = max(0.1, float(v_clean))
                    except Exception:
                        pass
                elif k in ("vol", "volume"):
                    try:
                        v_num = float(v_clean.replace("%", ""))
                        if "%" in v:
                            v_num /= 100.0
                        vol = max(0.0, min(3.0, v_num))
                    except Exception:
                        pass
            else:
                try:
                    num = float(p.replace("s", "").replace("сек", "").strip())
                    if start == 0.0:
                        start = max(0.0, num)
                    elif duration is None:
                        duration = max(0.1, num)
                    else:
                        if "%" in p:
                            num /= 100.0
                        vol = max(0.0, min(3.0, num))
                except Exception:
                    pass

        item = self.media_lib.find(name, category="sfx")
        if not item:
            item = self.media_lib.find(name)

        if item:
            return SfxSpec(
                name=item["name"],
                file_path=Path(item["path"]),
                start=start,
                duration=duration,
                volume=vol,
            )
        return None

    def _parse_bg_tag(self, val: str, default_vol: float) -> Optional[BgSpec]:
        clean = val.lower().strip()
        if clean in ("none", "off", "0", "disable"):
            return BgSpec(name="none", enabled=False)

        if not clean or clean in ("random", "?"):
            item = self.media_lib.random_sample(category="bg")
            if not item:
                return None
            return BgSpec(name=item["name"], file_path=Path(item["path"]), volume=default_vol)

        parts = [p.strip() for p in val.split(":") if p.strip()]
        name = parts[0]
        start = 0.0
        vol = default_vol

        for p in parts[1:]:
            if "=" in p:
                k, v = p.split("=", 1)
                k = k.lower().strip()
                v_clean = v.lower().replace("s", "").replace("сек", "").strip()
                if k in ("start", "offset", "from"):
                    try:
                        start = max(0.0, float(v_clean))
                    except Exception:
                        pass
                elif k in ("vol", "volume"):
                    try:
                        v_num = float(v_clean.replace("%", ""))
                        if "%" in v:
                            v_num /= 100.0
                        vol = max(0.0, min(2.0, v_num))
                    except Exception:
                        pass
            else:
                try:
                    num = float(p.replace("s", "").replace("сек", "").strip())
                    if start == 0.0:
                        start = max(0.0, num)
                    else:
                        if "%" in p:
                            num /= 100.0
                        vol = max(0.0, min(2.0, num))
                except Exception:
                    pass

        item = self.media_lib.find(name, category="bg")
        if not item:
            item = self.media_lib.find(name)

        if item:
            return BgSpec(
                name=item["name"],
                file_path=Path(item["path"]),
                start=start,
                volume=vol,
                enabled=True,
            )
        return None

    def _format_rate(self, val: str) -> str:
        v = val.lower().strip()
        if v == "fast":
            return "+40%"
        if v == "slow":
            return "-30%"
        if not v.startswith(("+", "-")):
            v = f"+{v}"
        if not v.endswith("%"):
            v = f"{v}%"
        return v

    def _format_pitch(self, val: str) -> str:
        v = val.lower().strip()
        if v in ("high", "up"):
            return "+30Hz"
        if v in ("low", "down"):
            return "-25Hz"
        if not v.startswith(("+", "-")):
            v = f"+{v}"
        if not (v.endswith("hz") or v.endswith("Hz")):
            v = f"{v}Hz"
        return v


class VoiceEngine:
    """Compiles ParsedVoiceScript into a finished Discord Voice (.ogg Opus) file and calculates waveforms."""

    def __init__(self, media_dir: Optional[Path] = None):
        self.media_lib = MediaLibrary(media_dir=media_dir)
        self.parser = VoiceDslParser(self.media_lib)

    async def render_to_ogg(
        self,
        text: str,
        default_voice: str = DEFAULT_VOICE,
        auto_mix: bool = False,
        output_path: Optional[Path] = None,
    ) -> Tuple[Path, float, Dict[str, Any]]:
        """Renders text with Micro-DSL into an Opus .ogg Discord voice message with waveform."""
        script = self.parser.parse(text, default_voice=default_voice, auto_mix=auto_mix)
        if not script.segments:
            raise RuntimeError("No speech or audio segments found in input text.")

        temp_dir = Path(tempfile.mkdtemp(prefix="unai_discord_voice_"))
        try:
            rendered_segments: List[Path] = []
            sfx_used: List[str] = []

            # Synthesize speech segments in parallel
            speech_tasks = []
            for idx, seg in enumerate(script.segments):
                seg_out = temp_dir / f"seg_{idx:03d}.wav"
                if isinstance(seg, SpeechSegment):
                    speech_tasks.append((idx, seg, seg_out))

            for idx, seg, seg_out in speech_tasks:
                await self._synthesize_speech_with_retry(seg, seg_out, temp_dir)

            for idx, seg in enumerate(script.segments):
                seg_out = temp_dir / f"seg_{idx:03d}.wav"
                if isinstance(seg, SpeechSegment):
                    if seg_out.exists():
                        rendered_segments.append(seg_out)

                elif isinstance(seg, SfxSegment):
                    if seg.sfx.file_path and seg.sfx.file_path.exists():
                        self._render_sfx_segment(seg.sfx, seg_out)
                        rendered_segments.append(seg_out)
                        sfx_used.append(seg.sfx.name)

                elif isinstance(seg, PauseSegment):
                    self._render_silence(seg.duration, seg_out)
                    rendered_segments.append(seg_out)

            if not rendered_segments:
                raise RuntimeError("Failed to render any audio segments.")

            # Concatenate voice segments
            voice_combined = temp_dir / "voice_combined.wav"
            self._concat_audio_files(rendered_segments, voice_combined, temp_dir)

            voice_duration = self._probe_duration(voice_combined)

            # Mix with background music if enabled
            final_wav = temp_dir / "final_mixed.wav"
            bg_used_name = None

            if script.bg and script.bg.enabled and script.bg.file_path and script.bg.file_path.exists():
                bg_used_name = script.bg.name
                self._mix_with_bg(voice_combined, script.bg, voice_duration, final_wav)
            else:
                shutil.copy(voice_combined, final_wav)

            # Encode to Discord Opus .ogg format
            out_file = output_path or (temp_dir / f"voice_{int(time.time())}.ogg")
            self._encode_to_opus_ogg(final_wav, out_file)

            final_duration = self._probe_duration(out_file)
            waveform_b64 = self.generate_waveform(out_file)

            meta = {
                "duration_seconds": round(final_duration, 2),
                "waveform": waveform_b64,
                "segments_count": len(script.segments),
                "bg_music": bg_used_name,
                "sfx_used": sfx_used,
            }

            if output_path is None:
                persisted_dir = Path.home() / ".unai" / "data" / "discord" / "voices"
                persisted_dir.mkdir(parents=True, exist_ok=True)
                persisted_file = persisted_dir / f"voice_{int(time.time())}_{random.randint(1000, 9999)}.ogg"
                shutil.copy(out_file, persisted_file)
                return persisted_file, final_duration, meta

            return out_file, final_duration, meta

        finally:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

    def convert_file_to_voice_ogg(self, input_file: Path) -> Tuple[Path, float, str]:
        """Converts an arbitrary audio file to Discord-compliant Opus .ogg and generates waveform."""
        if not input_file.exists():
            raise RuntimeError(f"Source audio file not found: {input_file}")

        persisted_dir = Path.home() / ".unai" / "data" / "discord" / "voices"
        persisted_dir.mkdir(parents=True, exist_ok=True)
        out_file = persisted_dir / f"voice_conv_{int(time.time())}_{random.randint(1000, 9999)}.ogg"

        self._encode_to_opus_ogg(input_file, out_file)
        dur = self._probe_duration(out_file)
        waveform_b64 = self.generate_waveform(out_file)
        return out_file, dur, waveform_b64

    def generate_waveform(self, audio_path: Path, num_samples: int = 256) -> str:
        """Extracts 256 peak amplitude bytes (0..255) from audio file and encodes to Base64 for Discord."""
        try:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(audio_path),
                "-f", "s16le",
                "-ac", "1",
                "-ar", "8000",
                "-",
            ]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            if res.returncode != 0 or not res.stdout:
                # Return neutral waveform on failure
                return base64.b64encode(bytes([128] * num_samples)).decode("ascii")

            raw_bytes = res.stdout
            total_samples = len(raw_bytes) // 2
            if total_samples == 0:
                return base64.b64encode(bytes([128] * num_samples)).decode("ascii")

            # Unpack signed 16-bit integers
            samples = struct.unpack(f"<{total_samples}h", raw_bytes[:total_samples * 2])
            
            chunk_size = max(1, total_samples // num_samples)
            peaks = []

            for i in range(num_samples):
                start = i * chunk_size
                end = min(total_samples, start + chunk_size)
                if start >= total_samples:
                    peaks.append(peaks[-1] if peaks else 128)
                    continue
                
                chunk = samples[start:end]
                # Peak absolute amplitude in chunk
                peak_val = max(abs(s) for s in chunk) if chunk else 0
                # Scale 0..32767 to 0..255 uint8
                scaled = min(255, max(0, int((peak_val / 32767.0) * 255.0)))
                peaks.append(scaled)

            # Smooth slight jitter
            return base64.b64encode(bytearray(peaks)).decode("ascii")
        except Exception:
            return base64.b64encode(bytes([128] * num_samples)).decode("ascii")

    async def _synthesize_speech_with_retry(
        self, seg: SpeechSegment, out_path: Path, temp_dir: Path, max_retries: int = 3
    ) -> None:
        last_err = None
        for attempt in range(max_retries):
            raw_mp3 = temp_dir / f"raw_speech_{random.randint(10000, 99999)}.mp3"
            try:
                communicate = edge_tts.Communicate(
                    text=seg.text,
                    voice=seg.voice,
                    rate=seg.rate,
                    pitch=seg.pitch,
                )
                await communicate.save(str(raw_mp3))

                audio_filter = AVAILABLE_EFFECTS.get(seg.effect.lower()) if seg.effect else None

                cmd = [
                    "ffmpeg", "-y",
                    "-i", str(raw_mp3),
                    "-ar", "48000",
                    "-ac", "1",
                ]
                if audio_filter:
                    cmd.extend(["-af", audio_filter])
                cmd.append(str(out_path))

                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
                if res.returncode == 0 and out_path.exists():
                    return
            except Exception as e:
                last_err = e
                await asyncio.sleep(0.3 * (attempt + 1))
            finally:
                if raw_mp3.exists():
                    raw_mp3.unlink()

        # Fallback: synthesize with standard parameters if custom failed
        try:
            raw_mp3 = temp_dir / f"fallback_{random.randint(10000, 99999)}.mp3"
            communicate = edge_tts.Communicate(text=seg.text, voice=DEFAULT_VOICE)
            await communicate.save(str(raw_mp3))
            subprocess.run(["ffmpeg", "-y", "-i", str(raw_mp3), "-ar", "48000", "-ac", "1", str(out_path)], check=True)
            if raw_mp3.exists():
                raw_mp3.unlink()
        except Exception:
            raise RuntimeError(f"Failed to synthesize speech for '{seg.text}': {last_err}")

    def _render_sfx_segment(self, sfx: SfxSpec, out_path: Path) -> None:
        cmd = ["ffmpeg", "-y"]
        if sfx.start > 0:
            cmd.extend(["-ss", str(sfx.start)])
        cmd.extend(["-i", str(sfx.file_path)])
        if sfx.duration is not None and sfx.duration > 0:
            cmd.extend(["-t", str(sfx.duration)])

        filters = []
        if sfx.volume != 1.0:
            filters.append(f"volume={sfx.volume:.2f}")

        filters.append("aresample=48000")
        cmd.extend(["-af", ",".join(filters), "-ar", "48000", "-ac", "1", str(out_path)])

        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        if res.returncode != 0:
            raise RuntimeError(f"FFmpeg SFX slice error: {res.stderr.decode('utf-8', errors='ignore')}")

    def _render_silence(self, duration: float, out_path: Path) -> None:
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", "anullsrc=r=48000:cl=mono",
            "-t", str(duration),
            "-ar", "48000",
            "-ac", "1",
            str(out_path),
        ]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)

    def _concat_audio_files(self, file_list: List[Path], out_path: Path, temp_dir: Path) -> None:
        list_file = temp_dir / "concat_list.txt"
        with open(list_file, "w", encoding="utf-8") as f:
            for p in file_list:
                f.write(f"file '{p.resolve()}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", str(list_file),
            "-c", "copy",
            str(out_path),
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        if res.returncode != 0:
            filter_inputs = "".join(f"[{i}:a]" for i in range(len(file_list)))
            filter_str = f"{filter_inputs}concat=n={len(file_list)}:v=0:a=1[out]"
            f_cmd = ["ffmpeg", "-y"]
            for p in file_list:
                f_cmd.extend(["-i", str(p)])
            f_cmd.extend(["-filter_complex", filter_str, "-map", "[out]", "-ar", "48000", "-ac", "1", str(out_path)])
            subprocess.run(f_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20, check=True)

    def _mix_with_bg(self, voice_path: Path, bg: BgSpec, duration: float, out_path: Path) -> None:
        fade_start = max(0.0, duration - 0.7)
        bg_filter = (
            f"[1:a]volume={bg.volume:.2f},"
            f"afade=t=out:st={fade_start:.2f}:d=0.7,"
            f"aresample=48000[bg];"
            f"[0:a][bg]amix=inputs=2:duration=first:dropout_transition=2[out]"
        )

        cmd = [
            "ffmpeg", "-y",
            "-i", str(voice_path),
        ]
        if bg.start > 0:
            cmd.extend(["-ss", str(bg.start)])
        cmd.extend([
            "-stream_loop", "-1",
            "-i", str(bg.file_path),
            "-filter_complex", bg_filter,
            "-map", "[out]",
            "-t", str(duration + 0.2),
            "-ar", "48000",
            "-ac", "1",
            str(out_path),
        ])
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=25)
        if res.returncode != 0:
            shutil.copy(voice_path, out_path)

    def _encode_to_opus_ogg(self, in_audio: Path, out_ogg: Path) -> None:
        cmd = [
            "ffmpeg", "-y",
            "-i", str(in_audio),
            "-c:a", "libopus",
            "-b:a", "64k",
            "-vbr", "on",
            "-ar", "48000",
            "-ac", "1",
            str(out_ogg),
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        if res.returncode != 0:
            raise RuntimeError(f"FFmpeg Opus encoding error: {res.stderr.decode('utf-8', errors='ignore')}")

    def _probe_duration(self, path: Path) -> float:
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            if res.returncode == 0 and res.stdout.strip():
                return float(res.stdout.strip())
        except Exception:
            pass
        return 0.0

    def get_dsl_documentation(self) -> Dict[str, Any]:
        media = self.media_lib.refresh()
        sfx_samples = [m["name"] for m in media if m["category"] == "sfx"][:20]
        bg_tracks = [m["name"] for m in media if m["category"] == "bg"][:20]

        return {
            "description": "Voice Micro-DSL allows rich synthesis of spoken voice notes (гски) and voice channel speech with SFX timing slices, background music ducking, pitch/rate controls, and audio effects for Discord.",
            "guidelines": [
                "Использование эффектов ОПЦИОНАЛЬНО: Наличие тегов Micro-DSL не означает, что их нужно пихать в каждую реплику. Чистый, естественный голос диктора без эффектов и музыки — абсолютно нормальный и часто лучший выбор.",
                "Уместность и контекст: Встраивайте звуки, фильтры или музыку только тогда, когда это действительно к месту (шутка, отыгрыш роли, эмоциональный акцент). Не устраивайте бессмысленный артхаус из 10 тегов подряд.",
                "Баланс громкости: Если используете фоновую музыку, держите её тихой (vol=0.12..0.20), чтобы диктора было разборчиво слышно.",
                "Точечные сэмплы: 1 подходящий сэмпл {sfx:...} или короткая {pause:0.5} создают нужный эффект гораздо лучше, чем перегруженный микс."
            ],
            "tags": {
                "{sfx:name}": "Insert a sound effect sample from ~/Media/Music (fuzzy matched by name). E.g. '{sfx:панос}'",
                "{sfx:name:start:len}": "Insert a precise timing slice of a sound. E.g. '{sfx:панос:start=1.5:len=2.0:vol=0.8}' or '{sfx:касперский:10:3}'",
                "{sfx:random} or {sfx}": "Insert a random short meme sound (< 10s)",
                "{bg:name}": "Set background music track under the voice. E.g. '{bg:фонк}' or '{bg:касперский король}'",
                "{bg:name:start:vol}": "Set background music track starting at offset seconds with volume. E.g. '{bg:фонк:start=15:vol=0.25}'",
                "{bg:none} or {bg:off}": "Disable background music",
                "{bg:random}": "Pick random background music track from library",
                "{bg_vol:0.2}": "Set global background music volume level (0.0 to 1.0, default 0.18)",
                "{voice:name}": f"Change narrator voice. Available: {list(AVAILABLE_VOICES.keys())}",
                "{rate:+30%}": "Change speech speed (e.g. '+40%', '-20%', 'fast', 'slow')",
                "{pitch:+25Hz}": "Change speech pitch (e.g. '+30Hz', '-20Hz', 'high', 'low')",
                "{pause:0.5}": "Insert a silence pause in seconds (e.g. '{pause:0.8}' or '{pause:1.2s}')",
                "{effect:name}": f"Apply audio filter on speech. Available: {list(AVAILABLE_EFFECTS.keys())} or 'none'",
            },
            "available_voices": AVAILABLE_VOICES,
            "available_effects": list(AVAILABLE_EFFECTS.keys()),
            "media_summary": {
                "total_files": len(media),
                "sample_sfx": sfx_samples,
                "sample_bg_tracks": bg_tracks,
            },
            "examples": [
                "{bg:фонк:vol=0.15} Здорово какуны! {sfx:дападаждитыпацан} Вы чё опять натворили {rate:+40%} быстро признавайтесь {sfx:панос} {pitch:+30Hz} а то щас всем капец {effect:robot} активирую самоуничтожение!",
                "{bg:касперский король идет:vol=0.20} {voice:dmitry} {effect:radio} Внимание всем постам! {sfx:1:vol=1.2} В секторе замечена подозрительная активность. {pause:0.7} Конец связи.",
                "{bg:random:vol=0.15} {rate:fast} {pitch:high} Эй ты {sfx:янепонялтычонарываешься:start=0.5:len=2.0} чё за дела такие!",
            ],
        }
