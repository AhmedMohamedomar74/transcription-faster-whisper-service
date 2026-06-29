import json
import subprocess


def detect_media_type(file_path: str) -> dict:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type,codec_name",
            "-of",
            "json",
            file_path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    has_video = False
    audio_codec = None
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            has_video = True
        if stream.get("codec_type") == "audio":
            audio_codec = stream.get("codec_name")
    return {"has_video": has_video, "audio_codec": audio_codec}


def needs_conversion(media_info: dict, file_size: int) -> bool:
    if media_info["has_video"]:
        return True
    if file_size > 10 * 1024 * 1024:
        return True
    if media_info.get("audio_codec") not in ("opus", "vorbis"):
        return True
    return False


def prepare_audio(input_path: str, output_path: str) -> str:
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-vn",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-c:a",
            "libopus",
            "-b:a",
            "32k",
            output_path,
        ],
        capture_output=True,
        check=True,
    )
    return output_path
