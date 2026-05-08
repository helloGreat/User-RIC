# Spark/RAN-side Components

This directory contains the control-side scripts running on the Spark/RAN side.

- `control_xapp.py`: RIC/xApp control backend for applying slice-level and UE-level radio control.
- `gnb_rnti_watcher.py`: runtime watcher for tracking UE RNTI and reporting current RAN control states.
- `profiles.json`: predefined service profiles that map task intents to radio-control configurations.

These components work with the near-RT RIC, Sionna-RK/OAI gNB, and the MCP interface used by the user-side agent.
