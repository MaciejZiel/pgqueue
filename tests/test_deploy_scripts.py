from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parent.parent / "deploy" / "termux"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")


def _logcap(log: Path, text: str, max_bytes: int, keep: int) -> None:
    subprocess.run(
        ["bash", str(DEPLOY / "logcap.sh"), str(log), str(max_bytes), str(keep)],
        input=text.encode(),
        check=True,
        timeout=60,
    )


def test_logcap_keeps_the_log_bounded(tmp_path: Path) -> None:
    log = tmp_path / "demo.log"
    line = "x" * 99 + "\n"
    _logcap(log, line * 5000, max_bytes=10_000, keep=2)

    files = sorted(p.name for p in tmp_path.iterdir())
    assert files == ["demo.log", "demo.log.1", "demo.log.2"]
    # Rotation is checked every 50 lines, so a file may overshoot by at most that much.
    for name in files:
        assert (tmp_path / name).stat().st_size <= 10_000 + 50 * len(line)
    # The newest output is in the live file.
    assert log.read_text().endswith(line)


def test_logcap_without_backups_truncates(tmp_path: Path) -> None:
    log = tmp_path / "demo.log"
    _logcap(log, ("y" * 99 + "\n") * 2000, max_bytes=5_000, keep=0)

    assert [p.name for p in tmp_path.iterdir()] == ["demo.log"]
    assert log.stat().st_size <= 5_000 + 50 * 100


def test_logcap_keeps_a_last_line_without_newline(tmp_path: Path) -> None:
    log = tmp_path / "demo.log"
    _logcap(log, "first\nlast", max_bytes=1_000_000, keep=2)
    assert log.read_text() == "first\nlast\n"


def test_boot_script_uses_logcap_and_all_scripts_parse() -> None:
    boot = (DEPLOY / "boot" / "pgqueue-demo").read_text()
    assert "logcap.sh" in boot
    assert ">>" not in boot
    for script in (DEPLOY / "boot" / "pgqueue-demo", DEPLOY / "logcap.sh", DEPLOY / "setup-db.sh"):
        subprocess.run(["bash", "-n", str(script)], check=True)
