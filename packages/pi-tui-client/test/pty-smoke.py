"""Exercise the built production entry point and its terminal cleanup.

Usage: python3 test/pty-smoke.py /path/to/pi-acp-server [regular|fullscreen]
The explicitly selected fixture provider makes no paid provider calls.
"""
import fcntl
import json
import os
import pty
import select
import struct
import subprocess
import sys
import tempfile
import termios
import time
from pathlib import Path

mode = sys.argv[2] if len(sys.argv) > 2 else "regular"
package = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="pirua-tui-pty-") as directory:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 110, 0, 0))
    command = [os.environ.get("NODE", "node"), str(package / "dist/main.js"),
               "--server", sys.argv[1], "--data-dir", directory,
               "--server-arg", "--config-dir", "--server-arg", directory + "/config",
               "--server-arg", "--fixture-provider", "--server-arg", "--fixture-delay-ms",
               "--server-arg", "25", "--" + mode, "--prompt", "hello original UI"]
    process = subprocess.Popen(command, stdin=slave, stdout=slave, stderr=slave,
                               env=dict(os.environ, TERM="xterm-256color", PIRUA_UI_DATA_DIR=directory + "/ui"),
                               cwd=directory, start_new_session=True)
    os.close(slave)
    output = bytearray()
    rendered_at = None
    shutdown = False
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and process.poll() is None:
        if select.select([master], [], [], 0.1)[0]:
            try:
                output.extend(os.read(master, 65536))
            except OSError:
                break
        if b"Fixture response: hello original UI" in output and rendered_at is None:
            rendered_at = time.monotonic()
        if rendered_at is not None and time.monotonic() - rendered_at > 0.4 and not shutdown:
            os.write(master, b"\x04")
            shutdown = True
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    while select.select([master], [], [], 0.05)[0]:
        try:
            output.extend(os.read(master, 65536))
        except OSError:
            break
    os.close(master)
    result = {"mode": mode, "exitCode": process.returncode, "renderedFixture": rendered_at is not None,
              "shutdownRequested": shutdown, "restoredBracketedPaste": b"\x1b[?2004l" in output,
              "restoredCursor": b"\x1b[?25h" in output,
              "leftAlternateScreen": mode != "fullscreen" or b"\x1b[?1049l" in output}
    print(json.dumps(result))
    if process.returncode != 0 or not all(v for k, v in result.items() if k not in ("mode", "exitCode")):
        print(output[-6000:].decode(errors="replace"), file=sys.stderr)
        sys.exit(1)
