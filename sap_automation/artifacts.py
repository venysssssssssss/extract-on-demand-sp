from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from .contracts import ExportJobSpec, ObjectArtifactPaths


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ArtifactStore:
    def __init__(self, output_root: Path) -> None:
        self.output_root = output_root.expanduser().resolve()

    def build_object_paths(self, job: ExportJobSpec) -> ObjectArtifactPaths:
        object_root = self.output_root / "runs" / job.run_id / job.object_slug
        raw_dir = object_root / "raw"
        normalized_dir = object_root / "normalized"
        metadata_dir = object_root / "metadata"
        stem = f"{job.object_slug}_{job.reference}_{job.run_id}"
        paths = ObjectArtifactPaths(
            object_code=job.object_code,
            object_root=object_root,
            raw_dir=raw_dir,
            normalized_dir=normalized_dir,
            metadata_dir=metadata_dir,
            raw_txt_path=raw_dir / job.export_filename,
            canonical_csv_path=normalized_dir / f"{stem}.csv",
            raw_csv_path=normalized_dir / f"{stem}.raw.csv",
            header_map_path=metadata_dir / f"{stem}.header_map.csv",
            rejects_path=metadata_dir / f"{stem}.rejects.csv",
            metadata_path=metadata_dir / f"{stem}.manifest.json",
            legacy_copy_path=self.output_root / "latest" / "legacy" / job.legacy_filename,
            log_path=metadata_dir / f"{stem}.log",
        )
        paths.ensure_directories()
        return paths

    def batch_manifest_path(self, run_id: str) -> Path:
        path = self.output_root / "runs" / run_id / "batch_manifest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def consolidated_paths(self, run_id: str) -> tuple[Path, Path]:
        consolidated_dir = self.output_root / "runs" / run_id / "consolidated"
        consolidated_dir.mkdir(parents=True, exist_ok=True)
        return consolidated_dir / "notes.csv", consolidated_dir / "interactions.csv"

    def copy_legacy_export(self, *, source_path: Path, destination_path: Path) -> None:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
