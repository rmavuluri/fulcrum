"""Keep the plugin wiring consistent with the server: versions, MCP config, locks and skill tool lists."""

import asyncio
import json
import re
import sys
import tomllib
from pathlib import Path

from fulcrum_mcp import __version__
from fulcrum_mcp.server import mcp

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "fulcrum"
PACKAGE = ROOT / "packages" / "fulcrum-mcp"
PREFIX = "mcp__plugin_fulcrum_msk__"
PYPI = {"registry": "https://pypi.org/simple"}


def _tool_names() -> set[str]:
    return {t.name for t in asyncio.run(mcp.list_tools())}


def _toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _mcp_server() -> dict:
    return _json(PLUGIN / ".mcp.json")["mcpServers"]["msk"]


def _pinned_server_version() -> str:
    """The fulcrum-mcp version the plugin runs, from `uvx fulcrum-mcp==X` in .mcp.json."""
    (arg,) = _mcp_server()["args"]
    name, version = arg.split("==")
    assert name == "fulcrum-mcp"
    return version


def test_versions_match():
    manifest = _json(PLUGIN / ".claude-plugin" / "plugin.json")
    assert manifest["version"] == _toml(PLUGIN / "pyproject.toml")["project"]["version"]
    assert f"## [{manifest['version']}]" in (PLUGIN / "CHANGELOG.md").read_text(encoding="utf-8")
    assert _toml(PACKAGE / "pyproject.toml")["project"]["version"] == __version__


def test_mcp_json_runs_the_pinned_package_with_declared_options():
    config = _mcp_server()
    # The directory can only verify a lock for the `uvx <package>==<version>` form.
    assert config["command"] == "uvx"
    pin = f"fulcrum-mcp=={_pinned_server_version()}"
    assert _toml(PLUGIN / "pyproject.toml")["project"]["dependencies"] == [pin]
    # The plugin directory rejects env vars that look like package-index redirects.
    assert not any(k.startswith(("UV_", "PIP_", "NPM_")) for k in config["env"])
    referenced = set(re.findall(r"\$\{user_config\.(\w+)\}", json.dumps(config)))
    assert referenced == set(_json(PLUGIN / ".claude-plugin" / "plugin.json")["userConfig"])


def test_runtime_lock_is_next_to_mcp_json_and_pins_the_published_server():
    lock = PLUGIN / "uv.lock"
    # The plugin directory does not inspect files of 256 KiB or more.
    assert lock.stat().st_size < 256 * 1024
    packages = {p["name"]: p for p in _toml(lock)["package"]}
    assert packages.pop("fulcrum-plugin")["source"] == {"virtual": "."}
    assert packages["fulcrum-mcp"]["version"] == _pinned_server_version()
    for name, package in packages.items():
        assert package["source"] == PYPI, name
        assert package["wheels"], name
        assert all(w["hash"].startswith("sha256:") for w in package["wheels"]), name


def test_skill_allowed_tools_match_server_tools():
    tools = _tool_names()
    for skill in ("fulcrum", "kafka-insights"):
        text = (PLUGIN / "skills" / skill / "SKILL.md").read_text(encoding="utf-8")
        allowed = {name.removeprefix(PREFIX) for name in re.findall(rf"{PREFIX}\w+", text.split("---")[1])}
        assert allowed == tools, skill
        for mentioned in re.findall(r"`(\w+)`", text):
            if mentioned.startswith(("list_", "describe_", "get_", "diff_")):
                assert mentioned in tools, f"{skill} mentions unknown tool {mentioned}"


def test_iam_policy_is_read_only():
    policy = _json(PLUGIN / "iam" / "fulcrum-readonly-policy.json")
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
        assert _json(f)["Version"] == "2012-10-17"


def test_no_package_source_redirects():
    # The plugin directory blocks any registry/index/source redirect in a pyproject in the repository.
    for path in (PLUGIN / "pyproject.toml", PACKAGE / "pyproject.toml", ROOT / "pyproject.toml"):
        uv = _toml(path).get("tool", {}).get("uv", {})
        assert not {"sources", "index", "index-url", "extra-index-url", "find-links"} & set(uv), path


def test_dev_dependencies_match_server_pins():
    runtime = _toml(PACKAGE / "pyproject.toml")["project"]["dependencies"]
    dev = _toml(ROOT / "pyproject.toml")["project"]["dependencies"]
    assert sorted(dev) == sorted(runtime)


def test_privacy_policy_is_shipped_and_linked():
    assert "# Fulcrum privacy policy" in (PLUGIN / "PRIVACY.md").read_text(encoding="utf-8")
    assert "(PRIVACY.md)" in (PLUGIN / "README.md").read_text(encoding="utf-8")


def test_plugin_ships_no_python_source():
    # The server is installed from PyPI; the plugin only pins it, so nothing in it is built or run.
    assert not list(PLUGIN.rglob("*.py"))
    assert "build-system" not in _toml(PLUGIN / "pyproject.toml")


def test_package_entry_point_starts_the_server():
    from tests.mcp_smoke import handshake

    assert _toml(PACKAGE / "pyproject.toml")["project"]["scripts"] == {"fulcrum-mcp": "fulcrum_mcp.server:main"}
    info, count = handshake([sys.executable, "-m", "fulcrum_mcp"], cwd=PACKAGE / "src")
    assert info["version"] == __version__
    assert count == len(_tool_names())
