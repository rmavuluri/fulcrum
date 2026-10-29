"""Start the server exactly as Claude Code does (the command in .mcp.json) and list its tools.

Usage: python tests/mcp_smoke.py [command ...]

With no arguments it runs the .mcp.json command (`uvx fulcrum-mcp==X`, from PyPI). The publish
workflow passes `uvx --from dist/<wheel> fulcrum-mcp` to test the built package before uploading it.
"""

import json
import subprocess
import sys
from pathlib import Path

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "fulcrum"


def handshake(command: list[str], cwd: Path | None = None) -> tuple[dict, int]:
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
    )
    try:

        def send(message: dict) -> None:
            proc.stdin.write(json.dumps(message) + "\n")
            proc.stdin.flush()

        send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "smoke", "version": "0"},
                },
            }
        )
        info = json.loads(proc.stdout.readline())["result"]["serverInfo"]
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        tools = json.loads(proc.stdout.readline())["result"]["tools"]
        return info, len(tools)
    finally:
        proc.kill()
        proc.wait()


def mcp_json_command() -> list[str]:
    config = json.loads((PLUGIN / ".mcp.json").read_text(encoding="utf-8"))["mcpServers"]["msk"]
    return [config["command"], *(a.replace("${CLAUDE_PLUGIN_ROOT}", str(PLUGIN)) for a in config["args"])]


if __name__ == "__main__":
    info, count = handshake(sys.argv[1:] or mcp_json_command())
    print(f"ok {info['name']} {info['version']}: {count} tools")
    sys.exit(0 if count else 1)
