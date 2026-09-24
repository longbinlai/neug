#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download and prepare the Graphalytics datasets used by the SSSP report.

The published archives contain the original Graphalytics properties, vertex,
edge, and reference-output files.  This script verifies SHA-256 before
extracting and creates the ``.csv`` aliases expected by NeuG's COPY loader.
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path


BASE_URL = (
    "https://neug.oss-cn-beijing.aliyuncs.com/"
    "datasets/ldbc-graphalytics"
)

PACKAGES = {
    "datagen-8_0-fb": {
        "archive": "datagen-8_0-fb.tar.zst",
        "sha256": "feff2443de632907180a8cf84845bed6f3f454614cf0de4f06bbaa7d037278de",
    },
    "datagen-8_1-fb": {
        "archive": "datagen-8_1-fb.tar.zst",
        "sha256": "540c952f09266c87c4d189c4aa3c48a449c0faf56e3d647548319fd81f18fc4f",
    },
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=destination.name + ".", suffix=".part", dir=destination.parent,
        delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        print("[download] {}".format(url), flush=True)
        with urllib.request.urlopen(url) as response, tmp_path.open("wb") as out:
            shutil.copyfileobj(response, out, length=8 * 1024 * 1024)
        os.replace(tmp_path, destination)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def extract_zstd_tar(archive: Path, output_dir: Path) -> None:
    zstd = shutil.which("zstd")
    tar = shutil.which("tar")
    if not zstd or not tar:
        raise RuntimeError("extracting requires both 'zstd' and 'tar' on PATH")
    output_dir.mkdir(parents=True, exist_ok=True)
    zstd_proc = subprocess.Popen(
        [zstd, "-dc", str(archive)], stdout=subprocess.PIPE
    )
    assert zstd_proc.stdout is not None
    tar_proc = subprocess.run(
        [tar, "-xf", "-", "-C", str(output_dir)],
        stdin=zstd_proc.stdout,
        check=False,
    )
    zstd_proc.stdout.close()
    zstd_rc = zstd_proc.wait()
    if zstd_rc != 0 or tar_proc.returncode != 0:
        raise RuntimeError(
            "archive extraction failed: zstd={}, tar={}".format(
                zstd_rc, tar_proc.returncode
            )
        )


def prepare_dataset(dataset: str, output_dir: Path, force: bool) -> Path:
    package = PACKAGES[dataset]
    archive = output_dir / "archives" / package["archive"]
    expected_digest = package["sha256"]

    if force or not archive.exists():
        download("{}/{}".format(BASE_URL, package["archive"]), archive)

    actual_digest = sha256(archive)
    if actual_digest != expected_digest:
        raise RuntimeError(
            "SHA-256 mismatch for {}: expected {}, got {}".format(
                archive, expected_digest, actual_digest
            )
        )
    print("[verify] {} {}".format(actual_digest, archive), flush=True)

    data_root = output_dir / "datagen"
    required = [
        data_root / (dataset + ".properties"),
        data_root / (dataset + ".v"),
        data_root / (dataset + ".e"),
        data_root / (dataset + "-SSSP"),
    ]
    if force or not all(path.exists() for path in required):
        extract_zstd_tar(archive, data_root)
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError("archive is missing required files: " + ", ".join(missing))

    for suffix in (".v", ".e"):
        source = data_root / (dataset + suffix)
        alias = data_root / (dataset + suffix + ".csv")
        if not alias.exists():
            try:
                alias.symlink_to(source.name)
            except OSError:
                shutil.copyfile(source, alias)

    print("[ready] GRAPHALYTICS_DATA_ROOT={}".format(data_root.resolve()))
    return data_root


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download, verify, and prepare NeuG SSSP benchmark data"
    )
    parser.add_argument(
        "--dataset",
        choices=["all"] + sorted(PACKAGES),
        default="all",
    )
    parser.add_argument("--output", default="./graphalytics-data")
    parser.add_argument(
        "--force", action="store_true", help="redownload and re-extract"
    )
    args = parser.parse_args()

    datasets = sorted(PACKAGES) if args.dataset == "all" else [args.dataset]
    try:
        for dataset in datasets:
            prepare_dataset(dataset, Path(args.output), args.force)
    except (OSError, RuntimeError) as error:
        print("error: {}".format(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
