#!/usr/bin/env python3
# gnb_rnti_watcher.py
#
# 作用：
# 1) 监听 patched gNB 容器日志（默认 oai-gnb-slicepatch-v2）
# 2) 从 RK-SLICE 日志里提取当前真实 scheduler RNTI + 运行时参数
# 3) 将该 RNTI 推送给 Spark 上的 control_xapp:7777
#
# 与原 rnti_watcher.py 分工：
# - rnti_watcher.py：monitor_xapp 观测同步（UE idx / PRBs(total) / heartbeat）
# - gnb_rnti_watcher.py：gNB scheduler 真值 RNTI 同步（控制优先用它）

import os
import re
import json
import time
import socket
import select
import subprocess
import sys
from typing import Any, Dict, Optional

GNB_CONTAINER = os.environ.get("GNB_CONTAINER", "oai-gnb-final")
CTRL_HOST = os.environ.get("CTRL_HOST", "127.0.0.1")
CTRL_PORT = int(os.environ.get("CTRL_PORT", "7777"))
SOCK_TIMEOUT = float(os.environ.get("SOCK_TIMEOUT", "2.5"))

LOG_SINCE = os.environ.get("LOG_SINCE", "30s")
RESTART_SEC = float(os.environ.get("RESTART_SEC", "1.0"))
HEARTBEAT_S = float(os.environ.get("HEARTBEAT_S", "2.0"))
EVENT_MIN_INTERVAL_S = float(os.environ.get("EVENT_MIN_INTERVAL_S", "0.25"))
STATE_FILE = os.environ.get("STATE_FILE", "/tmp/gnb_rnti_watcher_state.json")
UE_IDLE_TTL_S = float(os.environ.get("UE_IDLE_TTL_S", "15.0"))

FNUM = r"[-+]?\d+(?:\.\d+)?"

P_ASSOC = re.compile(r"RK-SLICE ASSOC UE\s+([0-9a-fA-F]+)\s*->\s*dl=(\d+)\s+ul=(\d+)", re.IGNORECASE)
P_DL_ORDER = re.compile(rf"RK-SLICE DL-ORDER UE\s+([0-9a-fA-F]+)\s+dl_slice=(\d+)\s+dl_mul=({FNUM})\s+coeff=({FNUM})\s+final=({FNUM})", re.IGNORECASE)
P_DL_ALLOC = re.compile(r"RK-SLICE DL-ALLOC UE\s+([0-9a-fA-F]+)\s+dl_slice=(\d+)\s+dl_cap=(\d+)\s+rbSize=(\d+)\s+current_rbs=(\d+)", re.IGNORECASE)
P_UL_ORDER = re.compile(rf"RK-SLICE UL-ORDER UE\s+([0-9a-fA-F]+)\s+ul_slice=(\d+)\s+ul_mul=({FNUM})\s+coeff=({FNUM})\s+final=({FNUM})", re.IGNORECASE)
P_UL_ALLOC = re.compile(r"RK-SLICE UL-ALLOC UE\s+([0-9a-fA-F]+)\s+ul_slice=(\d+)\s+ul_cap=(\d+)\s+rbSize=(\d+)\s+current_rbs=(\d+)", re.IGNORECASE)
P_DL_LIMIT = re.compile(r"RK-SLICE DL-LIMIT UE\s+([0-9a-fA-F]+)\s+dl_floor=(\d+)\s+dl_cap=(\d+)\s+dl_maxcg=(\d+)\s+rbSize=(\d+)", re.IGNORECASE)
P_UL_LIMIT = re.compile(r"RK-SLICE UL-LIMIT UE\s+([0-9a-fA-F]+)\s+ul_floor=(\d+)\s+ul_cap=(\d+)\s+ul_maxcg=(\d+)\s+rbSize=(\d+)", re.IGNORECASE)
P_DL_THROTTLED = re.compile(r"RK-SLICE DL-LIMIT UE\s+([0-9a-fA-F]+)\s+dl_maxcg=(\d+)\s+throttled=1", re.IGNORECASE)
P_UL_THROTTLED = re.compile(r"RK-SLICE UL-LIMIT UE\s+([0-9a-fA-F]+)\s+ul_maxcg=(\d+)\s+throttled=1", re.IGNORECASE)

P_LA_UL_CFG = re.compile(
    rf"RK-LA UL-CFG UE\s+([0-9a-fA-F]+)\s+ul_max_mcs=(\d+)\s+min_grant_prb=(\d+)\s+ulsch_max_frame_inactivity=(\d+)(?:\s+ul_sched_mul=({FNUM})\s+ul_maxcg_override=(-?\d+)\s+ul_small_burst_bytes=(-?\d+)\s+ul_small_burst_mul=({FNUM}))?",
    re.IGNORECASE,
)
P_LA_UL_ORDER = re.compile(
    rf"RK-LA UL-ORDER UE\s+([0-9a-fA-F]+)\s+ul_sched_mul=({FNUM})\s+ul_small_burst_hit=(\d+)\s+ul_small_burst_mul=({FNUM})\s+coeff_after_slice=({FNUM})\s+coeff_after_burst=({FNUM})\s+final=({FNUM})",
    re.IGNORECASE,
)
P_LA_UL_TPC = re.compile(
    r"RK-LA UL-TPC UE\s+([0-9a-fA-F]+)\s+pusch_target_snrx10=(\d+)\s+tpc0=(-?\d+)\s+pusch_snrx10=(-?\d+)",
    re.IGNORECASE,
)

P_LA_DL_CFG = re.compile(
    rf"RK-LA DL-CFG UE\s+([0-9a-fA-F]+)\s+dl_max_mcs=(\d+)\s+dl_min_grant_prb=(\d+)\s+dl_sched_mul=({FNUM})(?:\s+dl_maxcg_override=(-?\d+)\s+dl_small_burst_bytes=(-?\d+)\s+dl_small_burst_mul=({FNUM}))?",
    re.IGNORECASE,
)
P_LA_DL_ORDER = re.compile(
    rf"RK-LA DL-ORDER UE\s+([0-9a-fA-F]+)\s+dl_sched_mul=({FNUM})\s+dl_small_burst_hit=(\d+)\s+dl_small_burst_mul=({FNUM})\s+coeff_after_slice=({FNUM})\s+coeff_after_burst=({FNUM})\s+final=({FNUM})",
    re.IGNORECASE,
)

def _hex_to_int(s: str) -> Optional[int]:
    try:
        return int(s.strip(), 16)
    except Exception:
        return None


def parse_line(line: str) -> Optional[Dict[str, Any]]:
    m = P_ASSOC.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {"rnti": int(r), "rnti_hex": f"0x{int(r):x}", "reason": "assoc", "dl_id": int(m.group(2)), "ul_id": int(m.group(3))}

    m = P_DL_ORDER.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "dl_order",
                "dl_id": int(m.group(2)),
                "dl_mul": float(m.group(3)),
                "dl_coeff": float(m.group(4)),
                "dl_final": float(m.group(5)),
            }

    m = P_DL_ALLOC.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "dl_alloc",
                "dl_id": int(m.group(2)),
                "dl_cap": int(m.group(3)),
                "dl_rbSize": int(m.group(4)),
                "dl_current_rbs": int(m.group(5)),
            }

    m = P_UL_ORDER.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "ul_order",
                "ul_id": int(m.group(2)),
                "ul_mul": float(m.group(3)),
                "ul_coeff": float(m.group(4)),
                "ul_final": float(m.group(5)),
            }

    m = P_UL_ALLOC.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "ul_alloc",
                "ul_id": int(m.group(2)),
                "ul_cap": int(m.group(3)),
                "ul_rbSize": int(m.group(4)),
                "ul_current_rbs": int(m.group(5)),
            }

    m = P_DL_LIMIT.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "dl_limit",
                "dl_floor": int(m.group(2)),
                "dl_cap": int(m.group(3)),
                "dl_maxcg": int(m.group(4)),
                "dl_rbSize": int(m.group(5)),
                "dl_throttled": False,
            }

    m = P_UL_LIMIT.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "ul_limit",
                "ul_floor": int(m.group(2)),
                "ul_cap": int(m.group(3)),
                "ul_maxcg": int(m.group(4)),
                "ul_rbSize": int(m.group(5)),
                "ul_throttled": False,
            }

    m = P_DL_THROTTLED.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {"rnti": int(r), "rnti_hex": f"0x{int(r):x}", "reason": "dl_throttled", "dl_maxcg": int(m.group(2)), "dl_throttled": True}

    m = P_UL_THROTTLED.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {"rnti": int(r), "rnti_hex": f"0x{int(r):x}", "reason": "ul_throttled", "ul_maxcg": int(m.group(2)), "ul_throttled": True}

    m = P_LA_UL_CFG.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            out = {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "la_ul_cfg",
                "ul_max_mcs": int(m.group(2)),
                "min_grant_prb": int(m.group(3)),
                "ulsch_max_frame_inactivity": int(m.group(4)),
            }
            if m.group(5) is not None:
                out.update({
                    "ul_sched_mul": float(m.group(5)),
                    "ul_maxcg_override": int(m.group(6)),
                    "ul_small_burst_bytes": int(m.group(7)),
                    "ul_small_burst_mul": float(m.group(8)),
                })
            return out

    m = P_LA_UL_ORDER.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "la_ul_order",
                "ul_sched_mul": float(m.group(2)),
                "ul_small_burst_hit": int(m.group(3)),
                "ul_small_burst_mul": float(m.group(4)),
                "ul_coeff_after_slice": float(m.group(5)),
                "ul_coeff_after_burst": float(m.group(6)),
                "ul_final": float(m.group(7)),
            }

    m = P_LA_UL_TPC.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "la_ul_tpc",
                "pusch_target_snrx10": int(m.group(2)),
                "tpc0": int(m.group(3)),
                "pusch_snrx10": int(m.group(4)),
            }

    m = P_LA_DL_CFG.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            out = {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "la_dl_cfg",
                "dl_max_mcs": int(m.group(2)),
                "dl_min_grant_prb": int(m.group(3)),
                "dl_sched_mul": float(m.group(4)),
            }
            if m.group(5) is not None:
                out.update({
                    "dl_maxcg_override": int(m.group(5)),
                    "dl_small_burst_bytes": int(m.group(6)),
                    "dl_small_burst_mul": float(m.group(7)),
                })
            return out

    m = P_LA_DL_ORDER.search(line)
    if m:
        r = _hex_to_int(m.group(1))
        if r:
            return {
                "rnti": int(r),
                "rnti_hex": f"0x{int(r):x}",
                "reason": "la_dl_order",
                "dl_sched_mul": float(m.group(2)),
                "dl_small_burst_hit": int(m.group(3)),
                "dl_small_burst_mul": float(m.group(4)),
                "dl_coeff_after_slice": float(m.group(5)),
                "dl_coeff_after_burst": float(m.group(6)),
                "dl_final": float(m.group(7)),
            }

    return None


def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, "r") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def save_state(obj: Dict[str, Any]) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(obj, f)
    except Exception:
        pass


def _fmt_num(v: Any, nd: int = 2) -> str:
    if v is None:
        return "-"
    try:
        if isinstance(v, int):
            return str(v)
        return f"{float(v):.{nd}f}"
    except Exception:
        return str(v)

def _fmt_bool(v: Any) -> str:
    if v is None:
        return "-"
    return "1" if bool(v) else "0"

def _sec(title: str, parts: list[str]) -> Optional[str]:
    parts = [p for p in parts if p]
    if not parts:
        return None
    return f"  {title:<9} " + " | ".join(parts)

def _format_entry_log(entry: Dict[str, Any], push_reason: str) -> str:
    lines = [
        f"[gnb_rnti_watcher] UE {entry.get('rnti')} ({entry.get('rnti_hex')}) push_reason={push_reason} event={entry.get('reason')}"
    ]

    dl_slice = []
    if any(entry.get(k) is not None for k in ("dl_id", "dl_mul", "dl_cap", "dl_floor", "dl_maxcg", "dl_rbSize", "dl_current_rbs", "dl_throttled")):
        dl_slice.append(f"id={entry.get('dl_id')}")
        dl_slice.append(f"mul={_fmt_num(entry.get('dl_mul'))}")
        dl_slice.append(f"cap={entry.get('dl_cap')}")
        dl_slice.append(f"floor={entry.get('dl_floor')}")
        dl_slice.append(f"maxcg={entry.get('dl_maxcg')}")
        dl_slice.append(f"thr={_fmt_bool(entry.get('dl_throttled'))}")
        dl_slice.append(f"rb={entry.get('dl_rbSize')}")
        dl_slice.append(f"cur={entry.get('dl_current_rbs')}")

    ul_slice = []
    if any(entry.get(k) is not None for k in ("ul_id", "ul_mul", "ul_cap", "ul_floor", "ul_maxcg", "ul_rbSize", "ul_current_rbs", "ul_throttled")):
        ul_slice.append(f"id={entry.get('ul_id')}")
        ul_slice.append(f"mul={_fmt_num(entry.get('ul_mul'))}")
        ul_slice.append(f"cap={entry.get('ul_cap')}")
        ul_slice.append(f"floor={entry.get('ul_floor')}")
        ul_slice.append(f"maxcg={entry.get('ul_maxcg')}")
        ul_slice.append(f"thr={_fmt_bool(entry.get('ul_throttled'))}")
        ul_slice.append(f"rb={entry.get('ul_rbSize')}")
        ul_slice.append(f"cur={entry.get('ul_current_rbs')}")

    slice_parts = []
    if dl_slice:
        slice_parts.append("DL[" + " ".join(dl_slice) + "]")
    if ul_slice:
        slice_parts.append("UL[" + " ".join(ul_slice) + "]")
    sec = _sec("SLICE", slice_parts)
    if sec:
        lines.append(sec)

    ul_link = _sec("UL-LINK", [
        f"ul_max_mcs={entry.get('ul_max_mcs')}" if entry.get("ul_max_mcs") is not None else "",
        f"min_grant_prb={entry.get('min_grant_prb')}" if entry.get("min_grant_prb") is not None else "",
        f"ulsch_inact={entry.get('ulsch_max_frame_inactivity')}" if entry.get("ulsch_max_frame_inactivity") is not None else "",
        f"pusch_target={entry.get('pusch_target_snrx10')}" if entry.get("pusch_target_snrx10") is not None else "",
        f"tpc0={entry.get('tpc0')}" if entry.get("tpc0") is not None else "",
        f"pusch_snrx10={entry.get('pusch_snrx10')}" if entry.get("pusch_snrx10") is not None else "",
    ])
    if ul_link:
        lines.append(ul_link)

    ul_sched = _sec("UL-SCHED", [
        f"ul_sched_mul={_fmt_num(entry.get('ul_sched_mul'))}" if entry.get("ul_sched_mul") is not None else "",
        f"ul_maxcg_override={entry.get('ul_maxcg_override')}" if entry.get("ul_maxcg_override") is not None else "",
        f"ul_small_burst_bytes={entry.get('ul_small_burst_bytes')}" if entry.get("ul_small_burst_bytes") is not None else "",
        f"ul_small_burst_mul={_fmt_num(entry.get('ul_small_burst_mul'))}" if entry.get("ul_small_burst_mul") is not None else "",
        f"ul_small_burst_hit={entry.get('ul_small_burst_hit')}" if entry.get("ul_small_burst_hit") is not None else "",
    ])
    if ul_sched:
        lines.append(ul_sched)

    dl_link = _sec("DL-LINK", [
        f"dl_max_mcs={entry.get('dl_max_mcs')}" if entry.get("dl_max_mcs") is not None else "",
        f"dl_min_grant_prb={entry.get('dl_min_grant_prb')}" if entry.get("dl_min_grant_prb") is not None else "",
    ])
    if dl_link:
        lines.append(dl_link)

    dl_sched = _sec("DL-SCHED", [
        f"dl_sched_mul={_fmt_num(entry.get('dl_sched_mul'))}" if entry.get("dl_sched_mul") is not None else "",
        f"dl_maxcg_override={entry.get('dl_maxcg_override')}" if entry.get("dl_maxcg_override") is not None else "",
        f"dl_small_burst_bytes={entry.get('dl_small_burst_bytes')}" if entry.get("dl_small_burst_bytes") is not None else "",
        f"dl_small_burst_mul={_fmt_num(entry.get('dl_small_burst_mul'))}" if entry.get("dl_small_burst_mul") is not None else "",
        f"dl_small_burst_hit={entry.get('dl_small_burst_hit')}" if entry.get("dl_small_burst_hit") is not None else "",
    ])
    if dl_sched:
        lines.append(dl_sched)

    return "\n".join(lines)


def push_scheduler_rnti(entry: Dict[str, Any], reason: str) -> bool:
    req: Dict[str, Any] = {
        "cmd": "update_scheduler_rnti",
        "rnti": int(entry["rnti"]),
        "meta": {"src": "gnb_rnti_watcher", "reason": reason, "seen_ts": time.time()},
    }
    for k in [
        "dl_id", "ul_id", "dl_mul", "ul_mul", "dl_cap", "ul_cap",
        "dl_floor", "ul_floor", "dl_maxcg", "ul_maxcg",
        "dl_throttled", "ul_throttled",
        "dl_rbSize", "ul_rbSize", "dl_current_rbs", "ul_current_rbs",

        "ul_max_mcs", "min_grant_prb", "ulsch_max_frame_inactivity",
        "pusch_target_snrx10", "ul_sched_mul", "ul_maxcg_override",
        "ul_small_burst_bytes", "ul_small_burst_mul", "ul_small_burst_hit",

        "dl_max_mcs", "dl_min_grant_prb", "dl_sched_mul",
        "dl_maxcg_override", "dl_small_burst_bytes", "dl_small_burst_mul", "dl_small_burst_hit",

        "tpc0", "pusch_snrx10", "rnti_hex",
    ]:
        if k in entry and entry[k] is not None:
            req[k] = entry[k]
    data = (json.dumps(req) + "\n").encode("utf-8")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(SOCK_TIMEOUT)
    try:
        s.connect((CTRL_HOST, CTRL_PORT))
        s.sendall(data)
        return True
    except Exception as e:
        print(f"[gnb_rnti_watcher] send failed: {e!r}", flush=True)
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


def start_docker_logs() -> subprocess.Popen:
    cmd = ["docker", "logs", "-f", "--since", LOG_SINCE, GNB_CONTAINER]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)


def main() -> None:
    print(f"[gnb_rnti_watcher] start: gnb={GNB_CONTAINER}, ctrl={CTRL_HOST}:{CTRL_PORT}, since={LOG_SINCE}, heartbeat={HEARTBEAT_S}s, timeout={SOCK_TIMEOUT}s, event_min_interval={EVENT_MIN_INTERVAL_S}s, state_file={STATE_FILE}", flush=True)
    st = load_state()
    active_ues: Dict[int, Dict[str, Any]] = {}
    if isinstance(st.get("active_ues"), dict):
        for k, v in st["active_ues"].items():
            try:
                r = int(k)
                if isinstance(v, dict):
                    active_ues[r] = dict(v)
            except Exception:
                pass

    for rnti, entry in list(active_ues.items()):
        if entry.get("rnti") and push_scheduler_rnti(entry, reason="startup"):
            entry["last_push_ts"] = time.time()
            print(f"[gnb_rnti_watcher] preload scheduler_rnti={entry['rnti']} ({entry.get('rnti_hex')})", flush=True)

    p = start_docker_logs()
    while True:
        if p.poll() is not None:
            print("[gnb_rnti_watcher] docker logs exited, restart soon", flush=True)
            time.sleep(RESTART_SEC)
            p = start_docker_logs()
            continue

        now = time.time()
        for rnti, entry in list(active_ues.items()):
            if (now - float(entry.get("last_seen", 0))) > UE_IDLE_TTL_S:
                active_ues.pop(rnti, None)
                continue
            if HEARTBEAT_S > 0 and (now - float(entry.get("last_push_ts", 0))) >= HEARTBEAT_S:
                if push_scheduler_rnti(entry, reason="heartbeat"):
                    entry["last_push_ts"] = now

        if p.stdout is None:
            time.sleep(0.2)
            continue

        rlist, _, _ = select.select([p.stdout], [], [], 0.5)
        if not rlist:
            save_state({"ts": time.time(), "active_ues": active_ues})
            continue

        line = p.stdout.readline()
        if not line:
            continue

        parsed = parse_line(line)
        if not parsed:
            continue

        rnti = int(parsed["rnti"])
        entry = active_ues.get(rnti, {})
        entry.update(parsed)
        entry["last_seen"] = now
        active_ues[rnti] = entry

        last_push_ts = float(entry.get("last_push_ts", 0) or 0.0)
        should_push = parsed.get("reason") == "assoc" or (now - last_push_ts) >= EVENT_MIN_INTERVAL_S
        if should_push and push_scheduler_rnti(entry, reason=str(parsed.get("reason", "event"))):
            entry["last_push_ts"] = now
            if str(parsed.get("reason", "")) == "heartbeat":
                print(
                    f"[gnb_rnti_watcher] heartbeat UE {rnti} ({entry.get('rnti_hex')}) "
                    f"event={entry.get('reason')} dl_id={entry.get('dl_id')} ul_id={entry.get('ul_id')}",
                    flush=True,
                )
            else:
                print(_format_entry_log(entry, push_reason=str(parsed.get("reason", "event"))), flush=True)
        save_state({"ts": time.time(), "active_ues": active_ues})


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[gnb_rnti_watcher] bye", flush=True)
        sys.exit(0)
