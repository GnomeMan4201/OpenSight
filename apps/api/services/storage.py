"""
apps/api/services/storage.py

Local filesystem document storage.

Layout:  uploads/{source_tag}/{uuid}{ext}
Temp:    uploads/.tmp/{uuid}.tmp  (used during streaming upload)

v0.2 migration: replace this module with an S3/MinIO adapter implementing
the same interface (save_stream, finalize_temp, resolve, delete).
"""

import os
import shutil
import uuid
from pathlib import Path
from typing import BinaryIO

from apps.api.config import settings


_CHUNK = 65_536  # 64 KB streaming chunk


class LocalStorage:

    @property
    def base(self) -> Path:
        return settings.upload_path

    @property
    def tmp_dir(self) -> Path:
        return settings.tmp_upload_path

    def stream_to_temp(self) -> tuple[Path, "open"]:
        """
        Return (tmp_path, open_file_handle) for streaming an upload.
        Caller writes to the file handle, then calls finalize_temp or discard_temp.
        """
        tmp_path = self.tmp_dir / f"{uuid.uuid4()}.tmp"
        return tmp_path, open(tmp_path, "wb")

    def finalize_temp(
        self,
        tmp_path: Path,
        original_name: str,
        source_tag: str,
    ) -> str:
        """
        Move a completed temp file to its permanent location.
        Returns storage_path relative to upload_dir (e.g. "foia2024/abc123.pdf").

        Uses os.rename() which is atomic on POSIX when src and dst are on the
        same filesystem (guaranteed here since both are under upload_dir).
        Falls back to shutil.move() across filesystems.
        """
        suffix = _safe_suffix(original_name)
        tag_dir = self.base / _safe_tag(source_tag)
        tag_dir.mkdir(parents=True, exist_ok=True)
        dest = tag_dir / f"{uuid.uuid4()}{suffix}"

        try:
            os.rename(tmp_path, dest)
        except OSError:
            shutil.move(str(tmp_path), str(dest))

        return str(dest.relative_to(self.base))

    def discard_temp(self, tmp_path: Path) -> None:
        """Delete a temp file (used on dedup hit or validation failure)."""
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass

    def resolve(self, storage_path: str) -> Path:
        """Return absolute Path, rejecting traversal attempts."""
        resolved = (self.base / storage_path).resolve()
        if not str(resolved).startswith(str(self.base.resolve())):
            raise ValueError(f"Path traversal rejected: {storage_path}")
        return resolved

    def read_bytes(self, storage_path: str) -> bytes:
        return self.resolve(storage_path).read_bytes()

    def delete(self, storage_path: str) -> None:
        p = self.resolve(storage_path)
        if p.exists():
            p.unlink()


def _safe_suffix(name: str) -> str:
    """Extract extension, default .bin, force lowercase."""
    return (Path(name).suffix or ".bin").lower()[:16]


def _safe_tag(tag: str) -> str:
    """Sanitize source_tag for use as a directory name."""
    import re
    safe = re.sub(r"[^\w\-]", "_", tag or "default")
    return safe[:64] or "default"


storage = LocalStorage()
