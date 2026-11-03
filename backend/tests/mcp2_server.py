"""Real MCP server used by the MCP 2 transport regression tests."""
import asyncio
import sys
from fastmcp import FastMCP

server = FastMCP("deerflow-regression")

@server.tool
async def add(a: int, b: int) -> int:
    return a + b

@server.tool
async def slow() -> int:
    await asyncio.sleep(30)
    return 1

if __name__ == "__main__":
    if len(sys.argv) == 1:
        server.run(transport="stdio", show_banner=False)
    else:
        server.run(transport="http", host="127.0.0.1", port=int(sys.argv[1]), show_banner=False)
