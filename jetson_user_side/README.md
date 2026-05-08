# Jetson/User-side Components

This directory contains the user-side components running on the Jetson device.

- `server.py`: MCP server entry that exposes RIC control tools to the user-side agent.
- `agent-tars.config.json`: Agent TARS configuration for connecting the agent with the RIC-MCP tools.

These components allow the user-side agent to translate service intent into executable RIC tool calls through the 5G air interface.
