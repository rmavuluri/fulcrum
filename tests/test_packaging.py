"""Keep the plugin wiring consistent with the server: versions, MCP config and skill tool lists."""

import asyncio
import json
import re
import tomllib
from pathlib import Path

from fulcrum_mcp import __version__
from fulcrum_mcp.server import mcp

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "fulcrum"
PREFIX = "mcp__plugin_fulcrum_msk__"


def _tool_names() -> set[str]:
    return {t.name for t in asyncio.run(mcp.list_tools())}


def test_versions_match():
    manifest = json.loads((PLUGIN / ".claude-plugin" / "plugin.json").read_text())
    pyproject = tomllib.loads((PLUGIN / "pyproject.toml").read_text())
    assert manifest["version"] == pyproject["project"]["version"] == __version__
    assert f"## [{__version__}]" in (PLUGIN / "CHANGELOG.md").read_text()


def test_mcp_json_uses_plugin_paths_and_declared_options():
    config = json.loads((PLUGIN / ".mcp.json").read_text())["mcpServers"]["msk"]
    manifest = json.loads((PLUGIN / ".claude-plugin" / "plugin.json").read_text())
    assert "${CLAUDE_PLUGIN_ROOT}" in config["args"]
    assert "--frozen" in config["args"]
    # The plugin directory rejects env vars that look like package-index redirects.
    assert not any(k.startswith(("UV_", "PIP_", "NPM_")) for k in config["env"])
    referenced = set(re.findall(r"\$\{user_config\.(\w+)\}", json.dumps(config)))
    assert referenced == set(manifest["userConfig"])


def test_skill_allowed_tools_match_server_tools():
    tools = _tool_names()
    for skill in ("fulcrum", "kafka-insights"):
        text = (PLUGIN / "skills" / skill / "SKILL.md").read_text()
        allowed = {name.removeprefix(PREFIX) for name in re.findall(rf"{PREFIX}\w+", text.split("---")[1])}
        assert allowed == tools, skill
        for mentioned in re.findall(r"`(\w+)`", text):
            if mentioned.startswith(("list_", "describe_", "get_", "diff_")):
                assert mentioned in tools, f"{skill} mentions unknown tool {mentioned}"


def test_runtime_lock_is_next_to_mcp_json_and_small():
    lock = PLUGIN / "uv.lock"
    assert lock.is_file()
    # The plugin directory does not inspect files of 256 KiB or more.
    assert lock.stat().st_size < 256 * 1024
    pyproject = tomllib.loads((PLUGIN / "pyproject.toml").read_text())
    assert "dependency-groups" not in pyproject


def test_iam_policy_is_read_only():
    policy = json.loads((PLUGIN / "iam" / "fulcrum-readonly-policy.json").read_text())
    actions = {a for s in policy["Statement"] for a in s["Action"]}
    verbs = ("List", "Describe", "Get", "Connect")
    assert all(a.split(":")[1].startswith(verbs) for a in actions), actions
    assert all(s["Effect"] == "Allow" for s in policy["Statement"])


def test_all_iam_templates_are_valid_json():
    files = sorted((PLUGIN / "iam").glob("*.json"))
    assert {f.name for f in files} >= {
        "fulcrum-readonly-policy.json",
        "fulcrum-role-trust-policy.json",
        "fulcrum-assume-role-policy.json",
    }
    for f in files:
        assert json.loads(f.read_text())["Version"] == "2012-10-17"
