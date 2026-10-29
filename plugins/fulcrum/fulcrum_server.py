"""Starts the Fulcrum MCP server. .mcp.json runs this file with `uv run`, which installs the pinned
dependencies from uv.lock and the fulcrum_mcp package from src/ before Python starts."""

from fulcrum_mcp.server import main

if __name__ == "__main__":
    main()
