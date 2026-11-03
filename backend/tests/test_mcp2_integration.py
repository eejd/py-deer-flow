"""Exercise DeerFlow's MCP paths against the installed MCP 2 stack."""
import asyncio
import importlib.metadata
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import fastmcp
import langchain_mcp_adapters
import mcp
import pytest

from deerflow.config.extensions_config import ExtensionsConfig, McpServerConfig
from deerflow.mcp.client import build_server_params
from deerflow.mcp.session_pool import MCPSessionPool, call_pooled_session_tool
from deerflow.mcp.task_tool_caller import McpTaskToolCaller
from deerflow.mcp.tasks.ordinary import _structured_content
from deerflow.mcp.tools import _convert_call_tool_result

SERVER = Path(__file__).with_name("mcp2_server.py")


def test_installed_stack():
    for module, distribution, version in ((mcp, "mcp", "2.3.0"), (fastmcp, "fastmcp", "4.1.4"), (langchain_mcp_adapters, "langchain-mcp-adapters", "0.2.2+local01")):
        assert str(Path(module.__file__).resolve()).startswith("/opt/local/")
        assert importlib.metadata.version(distribution) == version


def test_stdio_pool():
    async def run():
        pool = MCPSessionPool()
        try:
            conn = {"transport": "stdio", "command": sys.executable, "args": [str(SERVER)]}
            session = await pool.get_session("local", "scope", conn)
            assert any(tool.name == "add" for tool in (await session.list_tools()).tools)
            async def call(name, args):
                return await call_pooled_session_tool(session, pool, server_name="local", scope_key="scope", tool_name=name, arguments=args, call_kwargs={"read_timeout_seconds": 5.0})
            result = await call("add", {"a": 19, "b": 23})
            assert _structured_content(result, tool_name="add") == {"result": 42}
            assert _convert_call_tool_result(result)[1] == {"structured_content": {"result": 42}}
            task = asyncio.create_task(call("slow", {}))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert _structured_content(await call("add", {"a": 20, "b": 22}), tool_name="add") == {"result": 42}
        finally:
            await pool.close_all()
    asyncio.run(run())


def test_http_task_caller():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proc = subprocess.Popen([sys.executable, str(SERVER), str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    async def run():
        for _ in range(100):
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            raise RuntimeError("MCP server did not start")
        cfg = ExtensionsConfig(mcpServers={"local": {"type": "http", "url": f"http://127.0.0.1:{port}/mcp", "tool_call_timeout": 5}})
        caller = McpTaskToolCaller(cfg)
        for _ in range(2):
            result = await caller.call_tool(server_name="local", tool_name="add", arguments={"a": 19, "b": 23}, user_id="u", thread_id="t")
            assert _structured_content(result, tool_name="add") == {"result": 42}
        task = asyncio.create_task(caller.call_tool(server_name="local", tool_name="slow", arguments={}, user_id="u", thread_id="t"))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await caller.call_tool(server_name="local", tool_name="add", arguments={"a": 19, "b": 23}, user_id="u", thread_id="t")
        assert _structured_content(result, tool_name="add") == {"result": 42}
    try:
        asyncio.run(run())
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_websocket_rejected():
    with pytest.raises(ValueError, match="WebSocket transport is not supported by MCP 2"):
        build_server_params("bad", McpServerConfig(type="websocket", url="ws://localhost"))


@pytest.mark.parametrize("redirect", [False, True])
def test_http_unauthorized_and_redirect_do_not_leak(redirect):
    received = []

    class Target(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(self.headers.get("Authorization"))
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    target = ThreadingHTTPServer(("127.0.0.1", 0), Target)

    class Origin(Target):
        def do_POST(self):
            received.append(("origin", self.headers.get("Authorization")))
            self.send_response(307 if redirect else 401)
            if redirect:
                self.send_header("Location", f"http://127.0.0.1:{target.server_port}/mcp")
            self.send_header("Content-Length", "0")
            self.end_headers()

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in (target, origin)]
    for thread in threads:
        thread.start()
    async def run():
        cfg = ExtensionsConfig(mcpServers={"local": {"type": "http", "url": f"http://127.0.0.1:{origin.server_port}/mcp", "headers": {"Authorization": "Bearer secret"}}})
        caller = McpTaskToolCaller(cfg)
        with pytest.raises(Exception):
            await caller.call_tool(server_name="local", tool_name="add", arguments={"a": 19, "b": 23}, user_id="u", thread_id="t")
    try:
        asyncio.run(run())
        assert ("origin", "Bearer secret") in received
        assert "Bearer secret" not in received
    finally:
        for server in (origin, target):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()
