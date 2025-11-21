#!/usr/bin/env python3

import logging
import os
import time
from typing import Dict, Any
import whisper

# Import the official queuerPy package from PyPI
from queuerPy import new_queuer_with_db, DatabaseConfiguration

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global whisper model (loaded once)
whisper_model = None


def load_whisper_model(model_size: str = "base"):
    """Load the Whisper model once at startup."""
    global whisper_model
    if whisper_model is None:
        logger.info(f"🤖 Loading Whisper model: {model_size}")
        whisper_model = whisper.load_model(model_size)
        logger.info("✅ Whisper model loaded successfully")
    return whisper_model


def transcribe_audio_file(
    audio_file_path: str, language: str = "auto"
) -> Dict[str, Any]:
    """
    Transcribe an audio file using faster-whisper.

    :param audio_file_path: Path to the audio file
    :param language: Language code (e.g., "en", "es", "fr") or "auto" for auto-detection
    :returns: Dictionary with transcription results
    """
    start_time = time.time()

    logger.info(f"🎤 Starting transcription for: {audio_file_path}")
    logger.info(f"📝 Language: {language}")

    try:
        # Load the model if not already loaded
        model = load_whisper_model()

        if not os.path.exists(audio_file_path):
            raise FileNotFoundError(f"Audio file not found: {audio_file_path}")

        result_whisper = model.transcribe(
            audio_file_path,
            language=None if language == "auto" else language,
            word_timestamps=True,
        )

        # Collect all segments
        transcription_segments = []
        full_transcription = result_whisper.get("text", "")

        for segment in result_whisper.get("segments", []):
            segment_data = {
                "start": segment.get("start", 0.0),
                "end": segment.get("end", 0.0),
                "text": segment.get("text", "").strip(),
                "confidence": segment.get("avg_logprob", 0.0),
            }
            transcription_segments.append(segment_data)

        result = {
            "audio_file": audio_file_path,
            "language": language,
            "language_detected": result_whisper.get("language", "unknown"),
            "language_probability": 1.0,  # OpenAI Whisper doesn't provide this metric
            "transcription": full_transcription.strip(),
            "duration_seconds": (
                len(transcription_segments) * 10.0 if transcription_segments else 30.0
            ),  # Estimate
            "word_count": (
                len(full_transcription.strip().split())
                if full_transcription.strip()
                else 0
            ),
            "processing_time_seconds": time.time() - start_time,
            "segments": transcription_segments,
            "status": "completed",
        }

        logger.info(f"✅ Transcription completed for: {audio_file_path}")
        logger.info(f"🌍 Detected language: {result['language_detected']}")
        logger.info(f"📝 Text length: {len(full_transcription)} characters")
        logger.info(f"⏱️  Processing time: {result['processing_time_seconds']:.2f}s")

        return result
    except Exception as e:
        raise Exception(f"Transcription failed: {e}")


def main():
    """Main function to start the transcription worker."""
    try:
        # Create database configuration
        db_config = DatabaseConfiguration(
            host=os.getenv("QUEUER_DB_HOST", "localhost"),
            port=int(os.getenv("QUEUER_DB_PORT", "5432")),
            database=os.getenv("QUEUER_DB_NAME", "queuer"),
            username=os.getenv("QUEUER_DB_USER", "postgres"),
            password=os.getenv("QUEUER_DB_PASSWORD", "password"),
        )

        # Create queuer with database configuration
        queuer = new_queuer_with_db("transcription-worker-py", 3, "", db_config)
        queuer.add_task(transcribe_audio_file)

        logger.info(f"🚀 Loading Whisper model...")
        load_whisper_model()

        logger.info(f"🚀 Starting worker...")
        queuer.start()

        # Keep the worker running
        logger.info("🔄 Worker started and listening for jobs...")
        try:
            while True:
                time.sleep(1)  # Keep the main thread alive
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
            queuer.stop()

    except KeyboardInterrupt:
        logger.info("🛑 Worker stopped by user")
    except Exception as e:
        logger.error(f"🛑 Error starting worker: {e}")
        raise


if __name__ == "__main__":
    main()
