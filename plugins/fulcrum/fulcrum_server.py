"""Starts the Fulcrum MCP server. .mcp.json runs this file with `uv run`, which installs the pinned
dependencies from uv.lock. The plugin itself is not built or installed as a package: there is no build
step, so nothing runs at install time beyond unpacking the locked wheels."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from fulcrum_mcp.server import main

if __name__ == "__main__":
    main()
