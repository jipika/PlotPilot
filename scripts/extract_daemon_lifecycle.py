"""提取 main.py 守护进程生命周期到 interfaces/runtime/daemon_lifecycle.py"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
main_path = ROOT / "interfaces/main.py"
lines = main_path.read_text(encoding="utf-8").splitlines(keepends=True)

# 317-907 (1-based) -> 316:907
chunk = "".join(lines[316:907])

header = '''"""进程与守护进程生命周期 — 从 interfaces.main 抽取。"""
import logging
import multiprocessing
import os
import signal
import threading
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

# 由 main 在启动时注入
log_level: int = logging.INFO
log_file: str = "logs/plotpilot.log"


def configure_daemon_logging(level: int, file: str) -> None:
    global log_level, log_file
    log_level = level
    log_file = file


'''
out = ROOT / "interfaces/runtime/daemon_lifecycle.py"
out.parent.mkdir(exist_ok=True)
out.write_text(header + chunk, encoding="utf-8")

replacement = '''# 守护进程生命周期（见 interfaces.runtime.daemon_lifecycle）
from interfaces.runtime.daemon_lifecycle import (
    configure_daemon_logging,
    get_shared_novel_state,
    restart_autopilot_daemon,
    update_shared_novel_state,
    _get_shared_state,
    _init_dag_node_registry,
    _recover_drafts_on_startup,
    _start_autopilot_daemon_thread,
    _stop_all_running_novels,
    _stop_autopilot_daemon_thread,
)

'''
new_main = "".join(lines[:316]) + replacement + "".join(lines[907:])
main_path.write_text(new_main, encoding="utf-8")
(ROOT / "interfaces/runtime/__init__.py").write_text('"""运行时进程管理。"""\n', encoding="utf-8")

# inject configure call after setup_logging in main
main_text = main_path.read_text(encoding="utf-8")
needle = 'setup_logging(level=log_level, log_file=log_file)\n'
if "configure_daemon_logging" not in main_text:
    main_text = main_text.replace(
        needle,
        needle + "from interfaces.runtime.daemon_lifecycle import configure_daemon_logging\n"
        "configure_daemon_logging(log_level, log_file)\n",
    )
    main_path.write_text(main_text, encoding="utf-8")
print("extracted", out)
