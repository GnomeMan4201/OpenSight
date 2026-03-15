"""
apps/api/config.py
Application settings. All values readable from environment / .env file.
"""

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────────
    database_url: str = "sqlite:///./opensight.db"

    # ── File storage ──────────────────────────────────────────────────────
    upload_dir: str = "./uploads"
    max_upload_size_mb: int = 500

    # ── CORS ──────────────────────────────────────────────────────────────
    # Comma-separated list of allowed frontend origins.
    # Production: set to your actual frontend URL, e.g. https://opensight.example.com
    cors_origins: str = "http://localhost:3000,http://localhost:5173,http://localhost:8080"

    # ── NLP / entity extraction ───────────────────────────────────────────
    # spaCy is used automatically when the model is loadable.
    # Install: pip install spacy && python -m spacy download en_core_web_sm
    spacy_model: str = "en_core_web_sm"

    # ── OCR ───────────────────────────────────────────────────────────────
    # Set USE_OCR=true after:
    #   pip install pymupdf pytesseract Pillow
    #   apt install tesseract-ocr  (or: brew install tesseract)
    use_ocr: bool = False
    ocr_language: str = "eng"

    # ── Redaction detection ───────────────────────────────────────────────
    # Requires PyMuPDF: pip install pymupdf
    # Disabled gracefully if PyMuPDF is absent.
    use_redaction_detection: bool = True

    # ── Ollama ───────────────────────────────────────────────────────────────
    use_ollama:   bool = False
    ollama_url:   str  = "http://localhost:11434"
    ollama_model: str  = "phi3:latest"

    # ── Ollama ───────────────────────────────────────────────────────────────
    use_ollama:   bool = False
    ollama_url:   str  = "http://localhost:11434"
    ollama_model: str  = "phi3:latest"

    # ── Application ───────────────────────────────────────────────────────
    app_name: str = "OpenSight"
    app_version: str = "0.1.0"
    debug: bool = False

    @property
    def upload_path(self) -> Path:
        p = Path(self.upload_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def tmp_upload_path(self) -> Path:
        p = self.upload_path / ".tmp"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def max_upload_bytes(self) -> int:
        return self.max_upload_size_mb * 1024 * 1024

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]



settings = Settings()


