"""Run a pipeline step, save its console output to output/logs/<name>.log and render a
terminal-style screenshot to output/screenshots/<name>.png (execution evidence for the report).

Usage:
    python tools/capture.py <name> -- <command ...>      run + log + screenshot
    python tools/capture.py <name> --stdin FILE -- <command ...>   feed FILE to the command's stdin
    python tools/capture.py <name> --from-file FILE      screenshot an existing log file
    python tools/capture.py <name> --render-only         re-render an existing log
"""
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common.config import OUTPUT_DIR, PROJECT_ROOT

LOG_DIR = os.path.join(OUTPUT_DIR, "logs")
SHOT_DIR = os.path.join(OUTPUT_DIR, "screenshots")
MAX_LINES = 90
MAX_COLS = 150

# JVM / Windows noise that says nothing about the step itself.
NOISE = re.compile(r"WARN |^\s+at |SLF4J|setLogLevel|Setting default log level|SUCCESS: The process|"
                   r"ERROR: The process with PID|Reason: There is no running instance|NativeCodeLoader|"
                   r"^\s*$|log4j|Using Spark's default|^ecommerce> (ecommerce)?\s*$|"
                   r"^Progress: (?!(200|400|600|800),000/|1,000,000/)")  # generator: keep every 200k sessions


def run(name, cmd, stdin_path=None):
    os.makedirs(LOG_DIR, exist_ok=True)
    start = time.time()
    stdin = open(os.path.join(PROJECT_ROOT, stdin_path), encoding="utf-8") if stdin_path else None
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, stdin=stdin, capture_output=True, text=True,
                          encoding="utf-8", errors="replace")
    elapsed = time.time() - start
    shown = " ".join(c.replace(sys.executable, "python") for c in cmd)
    with open(os.path.join(LOG_DIR, f"{name}.log"), "w", encoding="utf-8") as f:
        f.write(f"PS dataanalytics> {shown}{' < ' + stdin_path if stdin_path else ''}\n")
        f.write(proc.stdout + proc.stderr)
        f.write(f"\n[exit code {proc.returncode} | {elapsed:,.1f} s | {time.strftime('%Y-%m-%d %H:%M:%S')}]\n")
    print(f"{name}: exit {proc.returncode} in {elapsed:,.1f}s")
    return proc.returncode


def render(name):
    with open(os.path.join(LOG_DIR, f"{name}.log"), encoding="utf-8") as f:
        lines = [l.rstrip("\n") for l in f if not NOISE.search(l)]
    if len(lines) > MAX_LINES:  # keep the command, then the most informative tail
        lines = lines[:1] + [f"... ({len(lines) - MAX_LINES + 1} lines omitted, full log in output/logs/{name}.log) ..."] \
            + lines[-(MAX_LINES - 2):]
    lines = [l if len(l) <= MAX_COLS else l[:MAX_COLS - 3] + "..." for l in lines]

    width = max(9.0, min(MAX_COLS, max(len(l) for l in lines)) * 0.062)
    height = 0.55 + 0.165 * len(lines)
    fig = plt.figure(figsize=(width, height), facecolor="#1e1e1e")
    fig.text(0.008, 1 - 0.18 / height, f"  Windows PowerShell - {name}", color="#c8c8c8", fontsize=8.5,
             family="DejaVu Sans", va="top")
    fig.add_artist(plt.Line2D([0, 1], [1 - 0.38 / height] * 2, color="#3c3c3c", linewidth=1))
    for i, line in enumerate(lines):
        color = "#9cdcfe" if i == 0 else ("#f48771" if ("Error" in line or "Traceback" in line) else "#d4d4d4")
        fig.text(0.008, 1 - (0.5 + 0.165 * i) / height, line, color=color, fontsize=7.6, family="DejaVu Sans Mono", va="top")
    os.makedirs(SHOT_DIR, exist_ok=True)
    fig.savefig(os.path.join(SHOT_DIR, f"{name}.png"), dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)


if __name__ == "__main__":
    step = sys.argv[1]
    code = 0
    if "--from-file" in sys.argv:
        src = sys.argv[sys.argv.index("--from-file") + 1]
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(src, encoding="utf-8") as f, open(os.path.join(LOG_DIR, f"{step}.log"), "w", encoding="utf-8") as out:
            out.write(f"PS dataanalytics> Get-Content {src}\n" + f.read())
    elif "--render-only" not in sys.argv:
        stdin_path = sys.argv[sys.argv.index("--stdin") + 1] if "--stdin" in sys.argv else None
        code = run(step, sys.argv[sys.argv.index("--") + 1:], stdin_path)
    render(step)
    sys.exit(code)
