#!/usr/bin/env python3
import json
import os
import socket
import time
import traceback
from typing import Any, Dict, List, Optional

import xapp_sdk as ric

import threading

CTRL_CALL_TIMEOUT_S = float(os.environ.get("CTRL_CALL_TIMEOUT_S", "2.5"))
_CTRL_LOCK = threading.Lock()

class ControlSMTimeout(Exception):
    pass

def control_slice_sm_timed(node_id, ctrl, timeout_s: float = CTRL_CALL_TIMEOUT_S):
    ret_holder = {}
    err_holder = {}

    def _run():
        try:
            ret_holder["ret"] = ric.control_slice_sm(node_id, ctrl)
        except Exception as e:
            err_holder["err"] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout_s)

    if t.is_alive():
        raise ControlSMTimeout(f"control_slice_sm timeout after {timeout_s}s (ctrl_type={int(ctrl.type)})")
    if "err" in err_holder:
        raise err_holder["err"]
    return ret_holder.get("ret", None)

def _get_target_nodes():
    try:
        ns = ric.conn_e2_nodes()
        if ns and len(ns) > 0:
            return list(ns)
    except Exception:
        pass
    return [node] if node is not None else []

def control_slice_sm_safe(ctrl, broadcast: bool = True):
    with _CTRL_LOCK:
        targets = _get_target_nodes() if broadcast else ([node] if node is not None else [])
        if not targets:
            raise RuntimeError("no E2 nodes available")

        oks = []
        last_err = None

        for n in targets:
            try:
                ret = control_slice_sm_timed(n.id, ctrl, timeout_s=CTRL_CALL_TIMEOUT_S)
                oks.append(ret)
            except Exception as e:
                last_err = e

        if oks:
            return oks[-1]

        raise last_err if last_err is not None else RuntimeError("control_slice_sm failed on all nodes")

ALG_NVS = getattr(ric, "SLICE_ALG_SM_V0_NVS", 2)
NVS_RATE = getattr(ric, "SLICE_SM_NVS_V0_RATE", 0)
NVS_CAP  = getattr(ric, "SLICE_SM_NVS_V0_CAPACITY", 1)

HOST, PORT = "0.0.0.0", 7777

# profiles.json（容器内路径；已通过 volume 挂载）
PROFILES_PATH = os.environ.get("PROFILES_PATH", "/xapp/profiles.json")
UE_LA_STATE_PATH = os.environ.get("UE_LA_STATE_PATH", "/xapp-state/ue_la.conf")
_UE_LA_LOCK = threading.Lock()
UE_UL_LA_REGISTRY: Dict[int, Dict[str, Any]] = {}

UE_POSTURE_INT_FIELDS = (
    "ul_max_mcs",
    "min_grant_prb",
    "ulsch_max_frame_inactivity",
    "pusch_target_snrx10",
    "ul_maxcg_override",
    "ul_small_burst_bytes",
    "dl_max_mcs",
    "dl_min_grant_prb",
    "dl_maxcg_override",
    "dl_small_burst_bytes",
)

UE_POSTURE_FLOAT_FIELDS = (
    "ul_sched_mul",
    "ul_small_burst_mul",
    "dl_sched_mul",
    "dl_small_burst_mul",
)

UE_POSTURE_FIELDS = UE_POSTURE_INT_FIELDS + UE_POSTURE_FLOAT_FIELDS

def _parse_optional_int(req: Dict[str, Any], key: str) -> Optional[int]:
    v = req.get(key)
    if v is None:
        return None
    return int(v)

def _parse_optional_float(req: Dict[str, Any], key: str) -> Optional[float]:
    v = req.get(key)
    if v is None:
        return None
    return float(v)

def _format_posture_value(key: str, v: Any) -> str:
    if key in UE_POSTURE_FLOAT_FIELDS:
        return f"{float(v):g}"
    return str(int(v))

def _clear_ue_posture_snapshot(rnti: int) -> None:
    with _UE_LOCK:
        entry = UE_REGISTRY.get(int(rnti))
        if not entry:
            return
        for k in (
            *UE_POSTURE_FIELDS,
            "ul_small_burst_hit",
            "dl_small_burst_hit",
        ):
            entry.pop(k, None)
        UE_REGISTRY[int(rnti)] = entry

def _build_ue_posture_entry(req: Dict[str, Any]) -> Dict[str, Any]:
    try:
        entry = {
            "ul_max_mcs": _parse_optional_int(req, "ul_max_mcs"),
            "min_grant_prb": _parse_optional_int(req, "min_grant_prb"),
            "ulsch_max_frame_inactivity": _parse_optional_int(req, "ulsch_max_frame_inactivity"),
            "pusch_target_snrx10": _parse_optional_int(req, "pusch_target_snrx10"),
            "ul_sched_mul": _parse_optional_float(req, "ul_sched_mul"),
            "ul_maxcg_override": _parse_optional_int(req, "ul_maxcg_override"),
            "ul_small_burst_bytes": _parse_optional_int(req, "ul_small_burst_bytes"),
            "ul_small_burst_mul": _parse_optional_float(req, "ul_small_burst_mul"),
            "dl_max_mcs": _parse_optional_int(req, "dl_max_mcs"),
            "dl_min_grant_prb": _parse_optional_int(req, "dl_min_grant_prb"),
            "dl_sched_mul": _parse_optional_float(req, "dl_sched_mul"),
            "dl_maxcg_override": _parse_optional_int(req, "dl_maxcg_override"),
            "dl_small_burst_bytes": _parse_optional_int(req, "dl_small_burst_bytes"),
            "dl_small_burst_mul": _parse_optional_float(req, "dl_small_burst_mul"),
        }
    except Exception as e:
        raise ValueError(f"invalid UE posture field: {e}")

    if entry["ul_max_mcs"] is not None and not (0 <= entry["ul_max_mcs"] <= 28):
        raise ValueError("ul_max_mcs out of range (0..28)")
    if entry["dl_max_mcs"] is not None and not (0 <= entry["dl_max_mcs"] <= 28):
        raise ValueError("dl_max_mcs out of range (0..28)")
    if entry["min_grant_prb"] is not None and not (1 <= entry["min_grant_prb"] <= 50):
        raise ValueError("min_grant_prb out of range (1..50)")
    if entry["dl_min_grant_prb"] is not None and not (1 <= entry["dl_min_grant_prb"] <= 50):
        raise ValueError("dl_min_grant_prb out of range (1..50)")
    if entry["ulsch_max_frame_inactivity"] is not None and not (0 <= entry["ulsch_max_frame_inactivity"] <= 20):
        raise ValueError("ulsch_max_frame_inactivity out of range (0..20)")
    if entry["pusch_target_snrx10"] is not None and not (0 <= entry["pusch_target_snrx10"] <= 400):
        raise ValueError("pusch_target_snrx10 out of range (0..400)")
    for k in ("ul_sched_mul", "ul_small_burst_mul", "dl_sched_mul", "dl_small_burst_mul"):
        v = entry.get(k)
        if v is not None and not (v > 0.0):
            raise ValueError(f"{k} must be > 0")
    for k in ("ul_maxcg_override", "dl_maxcg_override"):
        v = entry.get(k)
        if v is not None and not (v > 0):
            raise ValueError(f"{k} must be > 0")
    for k in ("ul_small_burst_bytes", "dl_small_burst_bytes"):
        v = entry.get(k)
        if v is not None and not (v > 0):
            raise ValueError(f"{k} must be > 0")

    return entry

node = None

# 手动或自动缓存的 RNTI（monitor/backend 视角）
LAST_RNTI: Optional[int] = None

# gNB scheduler 真值 RNTI（由 gnb_rnti_watcher 推送）
CURRENT_SCHEDULER_RNTI: Optional[int] = None
CURRENT_SCHEDULER_RNTI_TS: Optional[float] = None

# ====== UE registry (active UE table + TTL + roles) ======
UE_TTL_S = int(os.environ.get("UE_TTL_S", "120"))  # UE entry 过期时间（秒）
UE_ACTIVE_MAX_AGE_S = float(os.environ.get("UE_ACTIVE_MAX_AGE_S", "15"))
UE_REGISTRY: Dict[int, Dict[str, Any]] = {}        # rnti -> entry
_UE_LOCK = threading.Lock()

def _now() -> float:
    return time.time()

def touch_ue(
    rnti: int,
    meta: Optional[Dict[str, Any]] = None,
    ue_idx: Optional[int] = None,
    ul_bytes: Optional[int] = None,
    dl_bytes: Optional[int] = None,
    rsrp: Optional[float] = None,
    snr: Optional[float] = None,
    role: Optional[str] = None,
    dl_id: Optional[int] = None,
    ul_id: Optional[int] = None,
    profile: Optional[str] = None,

    dl_mul: Optional[float] = None,
    ul_mul: Optional[float] = None,
    dl_cap: Optional[int] = None,
    ul_cap: Optional[int] = None,
    dl_floor: Optional[int] = None,
    ul_floor: Optional[int] = None,
    dl_maxcg: Optional[int] = None,
    ul_maxcg: Optional[int] = None,
    dl_rbSize: Optional[int] = None,
    ul_rbSize: Optional[int] = None,
    dl_current_rbs: Optional[int] = None,
    ul_current_rbs: Optional[int] = None,
    dl_throttled: Optional[bool] = None,
    ul_throttled: Optional[bool] = None,
    ul_event_inc: Optional[int] = None,
    dl_event_inc: Optional[int] = None,
    ul_grant_inc: Optional[int] = None,
    dl_grant_inc: Optional[int] = None,
    last_reason: Optional[str] = None,

    ul_max_mcs: Optional[int] = None,
    min_grant_prb: Optional[int] = None,
    ulsch_max_frame_inactivity: Optional[int] = None,
    pusch_target_snrx10: Optional[int] = None,
    ul_sched_mul: Optional[float] = None,
    ul_maxcg_override: Optional[int] = None,
    ul_small_burst_bytes: Optional[int] = None,
    ul_small_burst_mul: Optional[float] = None,
    ul_small_burst_hit: Optional[int] = None,

    dl_max_mcs: Optional[int] = None,
    dl_min_grant_prb: Optional[int] = None,
    dl_sched_mul: Optional[float] = None,
    dl_maxcg_override: Optional[int] = None,
    dl_small_burst_bytes: Optional[int] = None,
    dl_small_burst_mul: Optional[float] = None,
    dl_small_burst_hit: Optional[int] = None,

    tpc0: Optional[int] = None,
    pusch_snrx10: Optional[int] = None,
):
    """记录/刷新某个 RNTI 的 last_seen，并更新 UE_REGISTRY 字段 + LAST_RNTI + STATE"""
    global LAST_RNTI
    rnti = int(rnti)
    if rnti <= 0:
        return

    with _UE_LOCK:
        entry = UE_REGISTRY.get(rnti) or {}
        entry["last_seen"] = _now()

        if meta:
            old = entry.get("meta") or {}
            try:
                old.update(dict(meta))
            except Exception:
                pass
            entry["meta"] = old
            try:
                src_val = str((meta or {}).get("src") or "")
            except Exception:
                src_val = ""
            if src_val in ("gnb_rnti_watcher", "rnti_watcher"):
                entry["source_family"] = src_val

        def _set_int(key, v):
            try:
                entry[key] = int(v)
            except Exception:
                pass

        def _set_float(key, v):
            try:
                entry[key] = float(v)
            except Exception:
                pass

        if ue_idx is not None: _set_int("ue_idx", ue_idx)
        if ul_bytes is not None: _set_int("ul_bytes", ul_bytes)
        if dl_bytes is not None: _set_int("dl_bytes", dl_bytes)
        if rsrp is not None: _set_float("rsrp", rsrp)
        if snr is not None: _set_float("snr", snr)
        if role is not None: entry["role"] = str(role)
        if dl_id is not None: _set_int("dl_id", dl_id)
        if ul_id is not None: _set_int("ul_id", ul_id)
        if profile is not None: entry["profile"] = str(profile)

        if dl_mul is not None: _set_float("dl_mul", dl_mul)
        if ul_mul is not None: _set_float("ul_mul", ul_mul)
        if dl_cap is not None: _set_int("dl_cap", dl_cap)
        if ul_cap is not None: _set_int("ul_cap", ul_cap)
        if dl_floor is not None: _set_int("dl_floor", dl_floor)
        if ul_floor is not None: _set_int("ul_floor", ul_floor)
        if dl_maxcg is not None: _set_int("dl_maxcg", dl_maxcg)
        if ul_maxcg is not None: _set_int("ul_maxcg", ul_maxcg)
        if dl_rbSize is not None: _set_int("dl_rbSize", dl_rbSize)
        if ul_rbSize is not None: _set_int("ul_rbSize", ul_rbSize)
        if dl_current_rbs is not None: _set_int("dl_current_rbs", dl_current_rbs)
        if ul_current_rbs is not None: _set_int("ul_current_rbs", ul_current_rbs)

        if ul_max_mcs is not None: _set_int("ul_max_mcs", ul_max_mcs)
        if min_grant_prb is not None: _set_int("min_grant_prb", min_grant_prb)
        if ulsch_max_frame_inactivity is not None: _set_int("ulsch_max_frame_inactivity", ulsch_max_frame_inactivity)
        if pusch_target_snrx10 is not None: _set_int("pusch_target_snrx10", pusch_target_snrx10)
        if ul_sched_mul is not None: _set_float("ul_sched_mul", ul_sched_mul)
        if ul_maxcg_override is not None: _set_int("ul_maxcg_override", ul_maxcg_override)
        if ul_small_burst_bytes is not None: _set_int("ul_small_burst_bytes", ul_small_burst_bytes)
        if ul_small_burst_mul is not None: _set_float("ul_small_burst_mul", ul_small_burst_mul)
        if ul_small_burst_hit is not None: _set_int("ul_small_burst_hit", ul_small_burst_hit)

        if dl_max_mcs is not None: _set_int("dl_max_mcs", dl_max_mcs)
        if dl_min_grant_prb is not None: _set_int("dl_min_grant_prb", dl_min_grant_prb)
        if dl_sched_mul is not None: _set_float("dl_sched_mul", dl_sched_mul)
        if dl_maxcg_override is not None: _set_int("dl_maxcg_override", dl_maxcg_override)
        if dl_small_burst_bytes is not None: _set_int("dl_small_burst_bytes", dl_small_burst_bytes)
        if dl_small_burst_mul is not None: _set_float("dl_small_burst_mul", dl_small_burst_mul)
        if dl_small_burst_hit is not None: _set_int("dl_small_burst_hit", dl_small_burst_hit)

        if tpc0 is not None: _set_int("tpc0", tpc0)
        if pusch_snrx10 is not None: _set_int("pusch_snrx10", pusch_snrx10)

        if dl_throttled is not None:
            entry["dl_throttled"] = bool(dl_throttled)
            if bool(dl_throttled):
                _set_int("dl_throttled_count", int(entry.get("dl_throttled_count", 0)) + 1)
        if ul_throttled is not None:
            entry["ul_throttled"] = bool(ul_throttled)
            if bool(ul_throttled):
                _set_int("ul_throttled_count", int(entry.get("ul_throttled_count", 0)) + 1)
        if ul_event_inc is not None:
            _set_int("ul_event_count", int(entry.get("ul_event_count", 0)) + int(ul_event_inc))
        if dl_event_inc is not None:
            _set_int("dl_event_count", int(entry.get("dl_event_count", 0)) + int(dl_event_inc))
        if ul_grant_inc is not None:
            _set_int("ul_grant_count", int(entry.get("ul_grant_count", 0)) + int(ul_grant_inc))
        if dl_grant_inc is not None:
            _set_int("dl_grant_count", int(entry.get("dl_grant_count", 0)) + int(dl_grant_inc))
        if last_reason is not None:
            entry["last_reason"] = str(last_reason)

        UE_REGISTRY[rnti] = entry
        LAST_RNTI = rnti
        try:
            STATE["last_rnti"] = rnti
        except Exception:
            pass

def prune_ues():
    """清理过期 RNTI"""
    t = _now()
    with _UE_LOCK:
        dead = [r for r, e in UE_REGISTRY.items() if (t - float(e.get("last_seen", 0))) > UE_TTL_S]
        for r in dead:
            UE_REGISTRY.pop(r, None)

def list_ues(
    active_only: bool = False,
    max_age_s: Optional[float] = None,
    src: Optional[str] = None,
) -> List[Dict[str, Any]]:
    prune_ues()
    t = _now()
    max_age = float(max_age_s if max_age_s is not None else UE_ACTIVE_MAX_AGE_S)

    with _UE_LOCK:
        items = []
        for r, e in UE_REGISTRY.items():
            last = float(e.get("last_seen", 0))
            age = t - last
            if active_only and age > max_age:
                continue
            entry_src = str(e.get("source_family") or (e.get("meta") or {}).get("src") or "")
            if src and entry_src != str(src):
                continue
            items.append({
                "rnti": int(r),
                "rnti_hex": hex(int(r)),
                "ue_idx": e.get("ue_idx"),
                "ul_bytes": e.get("ul_bytes"),
                "dl_bytes": e.get("dl_bytes"),
                "rsrp": e.get("rsrp"),
                "snr": e.get("snr"),
                "role": (e.get("role") or "unknown"),
                "dl_id": e.get("dl_id"),
                "ul_id": e.get("ul_id"),
                "profile": e.get("profile"),
                "dl_mul": e.get("dl_mul"),
                "ul_mul": e.get("ul_mul"),
                "dl_cap": e.get("dl_cap"),
                "ul_cap": e.get("ul_cap"),
                "dl_floor": e.get("dl_floor"),
                "ul_floor": e.get("ul_floor"),
                "dl_maxcg": e.get("dl_maxcg"),
                "ul_maxcg": e.get("ul_maxcg"),
                "dl_rbSize": e.get("dl_rbSize"),
                "ul_rbSize": e.get("ul_rbSize"),
                "dl_current_rbs": e.get("dl_current_rbs"),
                "ul_current_rbs": e.get("ul_current_rbs"),
                "dl_throttled": e.get("dl_throttled"),
                "ul_throttled": e.get("ul_throttled"),
                "dl_throttled_count": e.get("dl_throttled_count"),
                "ul_throttled_count": e.get("ul_throttled_count"),
                "dl_grant_count": e.get("dl_grant_count"),
                "ul_grant_count": e.get("ul_grant_count"),
                "ul_event_count": e.get("ul_event_count"),
                "dl_event_count": e.get("dl_event_count"),

                "ul_max_mcs": e.get("ul_max_mcs"),
                "min_grant_prb": e.get("min_grant_prb"),
                "ulsch_max_frame_inactivity": e.get("ulsch_max_frame_inactivity"),
                "pusch_target_snrx10": e.get("pusch_target_snrx10"),
                "ul_sched_mul": e.get("ul_sched_mul"),
                "ul_maxcg_override": e.get("ul_maxcg_override"),
                "ul_small_burst_bytes": e.get("ul_small_burst_bytes"),
                "ul_small_burst_mul": e.get("ul_small_burst_mul"),
                "ul_small_burst_hit": e.get("ul_small_burst_hit"),

                "dl_max_mcs": e.get("dl_max_mcs"),
                "dl_min_grant_prb": e.get("dl_min_grant_prb"),
                "dl_sched_mul": e.get("dl_sched_mul"),
                "dl_maxcg_override": e.get("dl_maxcg_override"),
                "dl_small_burst_bytes": e.get("dl_small_burst_bytes"),
                "dl_small_burst_mul": e.get("dl_small_burst_mul"),
                "dl_small_burst_hit": e.get("dl_small_burst_hit"),

                "tpc0": e.get("tpc0"),
                "pusch_snrx10": e.get("pusch_snrx10"),
                "last_reason": e.get("last_reason"),
                "source_family": e.get("source_family"),
                "last_seen": last,
                "age_s": age,
                "meta": e.get("meta") or {},
            })
    items.sort(key=lambda x: x["last_seen"], reverse=True)
    return items

def _pick_by_role(items: List[Dict[str, Any]], role: Optional[str]) -> List[Dict[str, Any]]:
    if not role:
        return items
    role = str(role).strip().lower()
    return [u for u in items if str(u.get("role") or "").strip().lower() == role]

def pick_rnti(strategy: str = "latest", role: Optional[str] = None) -> Optional[int]:
    """根据策略选一个 RNTI: latest / only / last_rnti (可选 role=agent/competitor/unknown)"""
    prune_ues()
    ues = _pick_by_role(list_ues(), role)

    if strategy == "only":
        if len(ues) == 1:
            return int(ues[0]["rnti"])
        return None

    if strategy == "last_rnti":
        return int(LAST_RNTI) if LAST_RNTI else None

    # default: latest
    if ues:
        return int(ues[0]["rnti"])
    return int(LAST_RNTI) if LAST_RNTI else None

def get_scheduler_rnti() -> Optional[int]:
    return int(CURRENT_SCHEDULER_RNTI) if CURRENT_SCHEDULER_RNTI else None


def _normalize_target(target: Optional[str]) -> Optional[str]:
    if target is None:
        return None
    t = str(target).strip().lower()
    if t in ("agent", "competitor", "unknown"):
        return t
    return None


def resolve_control_rnti(req: Dict[str, Any]) -> Optional[int]:
    """
    控制类命令的默认 RNTI 解析顺序：
    1) 显式 rnti
    2) 对默认/agent 路径，优先 scheduler_rnti
    3) 再退回现有 UE_REGISTRY / pick_rnti
    4) 最后退回 LAST_RNTI
    """
    rnti = req.get("rnti", None)
    if rnti is not None:
        try:
            return int(rnti)
        except Exception:
            return None

    target = _normalize_target(req.get("target"))
    strategy = str(req.get("rnti_strategy", "latest"))

    # 默认控制和 agent 控制，优先使用 gNB scheduler 真值
    if target in (None, "agent"):
        sr = get_scheduler_rnti()
        if sr is not None:
            return sr

    role = target if target in ("agent", "competitor", "unknown") else None
    picked = pick_rnti(strategy=strategy, role=role)
    if picked is not None:
        return int(picked)

    sr = get_scheduler_rnti()
    if sr is not None:
        return sr

    return int(LAST_RNTI) if LAST_RNTI else None

# 防止 SWIG 指针字段被 GC（非常重要）
KEEPALIVE: Dict[str, Any] = {}

PROFILE_BINDING_FALLBACK = {
    "default": {"dl_id": 2, "ul_id": 2},   # balanced
    "text": {"dl_id": 2, "ul_id": 2},      # balanced
    "image": {"dl_id": 3, "ul_id": 3},     # throughput
    "video": {"dl_id": 3, "ul_id": 3},     # throughput

    "high_throughput_boost": {"dl_id": 3, "ul_id": 3},
    "low_latency_guard": {"dl_id": 4, "ul_id": 4},
    "burst_uplink": {"dl_id": 2, "ul_id": 5},
    "background_upload": {"dl_id": 1, "ul_id": 5},
    "night_idle": {"dl_id": 1, "ul_id": 1},
    "fairness_guard": {"dl_id": 6, "ul_id": 6},
    "plain_text_guard": {"dl_id": 7, "ul_id": 7},
    "agentic_loop": {"dl_id": 8, "ul_id": 8},
    "anti_jitter_guard": {"dl_id": 9, "ul_id": 9},
}

SLICE_RUNTIME_TABLE = {
    0: {
        "slice_id": 0,
        "name": "default",
        "human_summary": "默认调度，不额外抬升单次 grant，也不额外限制连续占用。",
        "dl_weight_mul": 1.0,
        "ul_weight_mul": 1.0,
        "dl_rb_cap": 0,
        "ul_rb_cap": 0,
        "dl_rb_floor": 0,
        "ul_rb_floor": 0,
        "dl_max_consecutive_grants": 0,
        "ul_max_consecutive_grants": 0,
    },
    1: {
        "slice_id": 1,
        "name": "background",
        "human_summary": "后台低优先级，单次 grant 更小，也更容易被打断。",
        "dl_weight_mul": 0.5,
        "ul_weight_mul": 0.5,
        "dl_rb_cap": 6,
        "ul_rb_cap": 6,
        "dl_rb_floor": 2,
        "ul_rb_floor": 2,
        "dl_max_consecutive_grants": 1,
        "ul_max_consecutive_grants": 1,
    },
    2: {
        "slice_id": 2,
        "name": "balanced",
        "human_summary": "默认均衡模式，吞吐、公平和稳定性折中。",
        "dl_weight_mul": 1.0,
        "ul_weight_mul": 1.0,
        "dl_rb_cap": 12,
        "ul_rb_cap": 12,
        "dl_rb_floor": 4,
        "ul_rb_floor": 4,
        "dl_max_consecutive_grants": 4,
        "ul_max_consecutive_grants": 4,
    },
    3: {
        "slice_id": 3,
        "name": "throughput",
        "human_summary": "高吞吐模式，更容易优先调度，且单次 grant 不容易太小。",
        "dl_weight_mul": 10.0,
        "ul_weight_mul": 4.0,
        "dl_rb_cap": 50,
        "ul_rb_cap": 30,
        "dl_rb_floor": 12,
        "ul_rb_floor": 12,
        "dl_max_consecutive_grants": 0,
        "ul_max_consecutive_grants": 0,
    },
    4: {
        "slice_id": 4,
        "name": "latency",
        "human_summary": "低时延偏置，更容易尽快排到前面，但避免长 burst。",
        "dl_weight_mul": 6.0,
        "ul_weight_mul": 6.0,
        "dl_rb_cap": 10,
        "ul_rb_cap": 10,
        "dl_rb_floor": 4,
        "ul_rb_floor": 4,
        "dl_max_consecutive_grants": 6,
        "ul_max_consecutive_grants": 6,
    },
    5: {
        "slice_id": 5,
        "name": "uplink_boost",
        "human_summary": "上行增强模式，适合上传、同步、回传。",
        "dl_weight_mul": 1.0,
        "ul_weight_mul": 10.0,
        "dl_rb_cap": 12,
        "ul_rb_cap": 50,
        "dl_rb_floor": 4,
        "ul_rb_floor": 12,
        "dl_max_consecutive_grants": 2,
        "ul_max_consecutive_grants": 0,
    },
    6: {
        "slice_id": 6,
        "name": "capped_fair",
        "human_summary": "公平受限模式，避免连续多拍独占资源。",
        "dl_weight_mul": 1.2,
        "ul_weight_mul": 1.2,
        "dl_rb_cap": 8,
        "ul_rb_cap": 8,
        "dl_rb_floor": 2,
        "ul_rb_floor": 2,
        "dl_max_consecutive_grants": 2,
        "ul_max_consecutive_grants": 2,
    },
    7: {
        "slice_id": 7,
        "name": "text_lite",
        "human_summary": "普通文字轻量模式，保持轻量和温和调度，不主动放大吞吐。",
        "dl_weight_mul": 1.0,
        "ul_weight_mul": 1.0,
        "dl_rb_cap": 10,
        "ul_rb_cap": 10,
        "dl_rb_floor": 3,
        "ul_rb_floor": 3,
        "dl_max_consecutive_grants": 3,
        "ul_max_consecutive_grants": 3,
    },
    8: {
        "slice_id": 8,
        "name": "agentic_loop",
        "human_summary": "多轮 agent 回环模式，双向都更强调小包、首包和短周期轮转。",
        "dl_weight_mul": 5.0,
        "ul_weight_mul": 6.0,
        "dl_rb_cap": 8,
        "ul_rb_cap": 8,
        "dl_rb_floor": 4,
        "ul_rb_floor": 4,
        "dl_max_consecutive_grants": 3,
        "ul_max_consecutive_grants": 3,
    },
    9: {
        "slice_id": 9,
        "name": "jitter_guard",
        "human_summary": "抗抖动保护模式，优先收敛交互抖动和尾时延，而不是追求峰值吞吐。",
        "dl_weight_mul": 2.2,
        "ul_weight_mul": 2.2,
        "dl_rb_cap": 8,
        "ul_rb_cap": 8,
        "dl_rb_floor": 3,
        "ul_rb_floor": 3,
        "dl_max_consecutive_grants": 2,
        "ul_max_consecutive_grants": 2,
    },
}

PARAMS_EXPORT_ORDER = ["slice_id", "weight_mul", "rb_cap", "rb_floor", "max_consecutive_grants"]


def get_slice_runtime(slice_id: int) -> Dict[str, Any]:
    base = SLICE_RUNTIME_TABLE.get(int(slice_id)) or SLICE_RUNTIME_TABLE[0]
    return dict(base)


def _build_half_policy(slice_id: Optional[int], direction: str) -> Dict[str, Any]:
    sid = int(slice_id) if slice_id is not None else 0
    pol = get_slice_runtime(sid)
    return {
        "slice_id": sid,
        "name": pol.get("name", "unknown"),
        "human_summary": pol.get("human_summary"),
        "weight_mul": pol.get(f"{direction}_weight_mul"),
        "rb_cap": pol.get(f"{direction}_rb_cap"),
        "rb_floor": pol.get(f"{direction}_rb_floor"),
        "max_consecutive_grants": pol.get(f"{direction}_max_consecutive_grants"),
    }


def build_binding_runtime(dl_id: int, ul_id: int) -> Dict[str, Any]:
    return {"dl": _build_half_policy(dl_id, "dl"), "ul": _build_half_policy(ul_id, "ul")}


def _get_profile_meta(profile: Optional[str]) -> Dict[str, Any]:
    if not profile:
        return {}
    p = load_profiles(force=False) or {}
    prof = (p.get("profiles") or {}).get(str(profile))
    return dict(prof) if isinstance(prof, dict) else {}


def _entry_policy_snapshot(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    entry = entry or {}
    dl_id = int(entry.get("dl_id") or 0)
    ul_id = int(entry.get("ul_id") or 0)
    pol = build_binding_runtime(dl_id, ul_id)
    if entry.get("dl_mul") is not None: pol["dl"]["weight_mul"] = entry.get("dl_mul")
    if entry.get("ul_mul") is not None: pol["ul"]["weight_mul"] = entry.get("ul_mul")
    if entry.get("dl_cap") is not None: pol["dl"]["rb_cap"] = entry.get("dl_cap")
    if entry.get("ul_cap") is not None: pol["ul"]["rb_cap"] = entry.get("ul_cap")
    if entry.get("dl_floor") is not None: pol["dl"]["rb_floor"] = entry.get("dl_floor")
    if entry.get("ul_floor") is not None: pol["ul"]["rb_floor"] = entry.get("ul_floor")
    if entry.get("dl_maxcg") is not None: pol["dl"]["max_consecutive_grants"] = entry.get("dl_maxcg")
    if entry.get("ul_maxcg") is not None: pol["ul"]["max_consecutive_grants"] = entry.get("ul_maxcg")
    pol["runtime"] = {
        "dl_rbSize": entry.get("dl_rbSize"), "ul_rbSize": entry.get("ul_rbSize"),
        "dl_current_rbs": entry.get("dl_current_rbs"), "ul_current_rbs": entry.get("ul_current_rbs"),
        "dl_throttled": entry.get("dl_throttled"), "ul_throttled": entry.get("ul_throttled"),
        "dl_throttled_count": entry.get("dl_throttled_count", 0), "ul_throttled_count": entry.get("ul_throttled_count", 0),
        "dl_grant_count": entry.get("dl_grant_count", 0), "ul_grant_count": entry.get("ul_grant_count", 0),
    }
    return pol


def _policy_diff(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    before = before or {}
    after = after or {}
    diff: Dict[str, Any] = {"dl": {}, "ul": {}}
    for half in ("dl", "ul"):
        b = before.get(half) or {}
        a = after.get(half) or {}
        for key in PARAMS_EXPORT_ORDER + ["name"]:
            if b.get(key) != a.get(key):
                diff[half][key] = {"before": b.get(key), "after": a.get(key)}
    return diff

# 运行状态（给 get_state 用）
STATE: Dict[str, Any] = {
    "started_at": time.time(),
    "profiles_path": PROFILES_PATH,
    "profiles_loaded_at": None,
    "profiles": None,          # dict
    "last_rnti": None,
    "scheduler_rnti": None,
    "scheduler_rnti_ts": None,
    "last_profile": None,
    "last_slice": None,
    "last_ctrl_ts": None,
    "last_ctrl": None,
    "active_mode": None,
    "active_alg": None,
}


def ok(**kw):
    d = {"ok": True}
    d.update(kw)
    return d


def err(msg: str, **kw):
    d = {"ok": False, "error": msg}
    d.update(kw)
    return d


def safe(v):
    try:
        json.dumps(v)
        return v
    except Exception:
        return str(v)


def _get_ctrl_const(name_contains: str) -> Optional[int]:
    for n in dir(ric):
        if n.startswith("SLICE_CTRL_SM_V0_") and name_contains in n:
            return getattr(ric, n)
    return None


CTRL_ADD = _get_ctrl_const("ADD")
CTRL_DEL = _get_ctrl_const("DEL")
CTRL_UE_ASSOC = getattr(ric, "SLICE_CTRL_SM_V0_UE_SLICE_ASSOC", None)

ALG_STATIC = None
for n in dir(ric):
    if n.startswith("SLICE_ALG_SM_V0_") and ("STATIC" in n or n.endswith("_STA")):
        ALG_STATIC = getattr(ric, n)
        break


def load_profiles(force: bool = False) -> Optional[Dict[str, Any]]:
    """
    读取 /xapp/profiles.json 并缓存到 STATE["profiles"]
    """
    if (not force) and STATE.get("profiles") is not None:
        return STATE["profiles"]
    try:
        with open(PROFILES_PATH, "r", encoding="utf-8") as f:
            p = json.load(f)
        STATE["profiles"] = p
        STATE["profiles_loaded_at"] = time.time()
        return p
    except FileNotFoundError:
        STATE["profiles"] = None
        STATE["profiles_loaded_at"] = time.time()
        return None
    except Exception:
        # 不直接炸掉服务：返回 None，由调用方处理
        STATE["profiles"] = None
        STATE["profiles_loaded_at"] = time.time()
        return None

def _write_ue_la_state_atomic() -> None:
    tmp_path = UE_LA_STATE_PATH + ".tmp"
    os.makedirs(os.path.dirname(UE_LA_STATE_PATH), exist_ok=True)

    with _UE_LA_LOCK:
        items = sorted(UE_UL_LA_REGISTRY.items(), key=lambda kv: int(kv[0]))
        lines: List[str] = []
        for rnti, entry in items:
            parts = [f"rnti={int(rnti)}"]
            for k in UE_POSTURE_FIELDS:
                v = entry.get(k)
                if v is not None:
                    parts.append(f"{k}={_format_posture_value(k, v)}")
            lines.append(" ".join(parts))

    with open(tmp_path, "w", encoding="utf-8") as f:
        if lines:
            f.write("\n".join(lines) + "\n")
        else:
            f.write("")
    os.replace(tmp_path, UE_LA_STATE_PATH)

def get_profile_binding(profile: str) -> Dict[str, int]:
    """
    v2:
    優先從 profiles.json["profiles"][profile] 讀 dl_id/ul_id
    若沒有，再退回 PROFILE_BINDING_FALLBACK
    """
    p = load_profiles(force=False)

    if p and isinstance(p, dict):
        prof = (p.get("profiles") or {}).get(profile)
        if isinstance(prof, dict):
            dl_id = int(prof["dl_id"])
            ul_id = int(prof.get("ul_id", dl_id))
            return {"dl_id": dl_id, "ul_id": ul_id}

    if profile in PROFILE_BINDING_FALLBACK:
        return dict(PROFILE_BINDING_FALLBACK[profile])

    raise KeyError(f"unknown profile={profile}")


def profile_to_rate_slices(p: Dict[str, Any]) -> (List[Dict[str, Any]], List[Dict[str, Any]]):
    """
    从 profiles.json 生成 do_add_nvs_rate_slices 需要的 dl/ul slice 列表
    要求 profiles.json 至少包含：
      {
        "slice_ids": {"text":1,"image":2,"video":3},
        "nvs_rate": {
          "dl": {"text":{"mbps_required":..,"mbps_reference":..}, ...},
          "ul": {...}
        }
      }
    """
    slice_ids = p["slice_ids"]
    nvs = p["nvs_rate"]

    dl = []
    for name, sid in slice_ids.items():
        x = nvs["dl"][name]
        dl.append({
            "id": int(sid),
            "label": str(name),
            "mbps_required": float(x.get("mbps_required", 0)),
            "mbps_reference": float(x.get("mbps_reference", 100)),
        })

    ul = []
    for name, sid in slice_ids.items():
        x = nvs["ul"][name]
        ul.append({
            "id": int(sid),
            "label": str(name),
            "mbps_required": float(x.get("mbps_required", 0)),
            "mbps_reference": float(x.get("mbps_reference", 100)),
        })

    return dl, ul

def policy_to_capacity_slices(p: Dict[str, Any], pol: Dict[str, Any]):
    slice_ids = p["slice_ids"]
    dl = []
    for name, sid in slice_ids.items():
        x = pol["dl"].get(name, {})
        dl.append({
            "id": int(sid),
            "label": str(name),
            "pct_reserved": float(x.get("pct_reserved", x.get("pct", 0))),
        })
    ul = []
    for name, sid in slice_ids.items():
        x = pol["ul"].get(name, {})
        ul.append({
            "id": int(sid),
            "label": str(name),
            "pct_reserved": float(x.get("pct_reserved", x.get("pct", 0))),
        })
    return dl, ul


def policy_to_rate_slices(p: Dict[str, Any], pol: Dict[str, Any]):
    slice_ids = p["slice_ids"]
    dl = []
    for name, sid in slice_ids.items():
        x = pol["dl"].get(name, {})
        dl.append({
            "id": int(sid),
            "label": str(name),
            "mbps_required": float(x.get("mbps_required", 0)),
            "mbps_reference": float(x.get("mbps_reference", 100)),
        })
    ul = []
    for name, sid in slice_ids.items():
        x = pol["ul"].get(name, {})
        ul.append({
            "id": int(sid),
            "label": str(name),
            "mbps_required": float(x.get("mbps_required", 0)),
            "mbps_reference": float(x.get("mbps_reference", 100)),
        })
    return dl, ul


def do_set_mode(req: Dict[str, Any]):
    """
    v2:
    mode 本質上就是上層 profile。
    預設只做 profile -> {dl_id, ul_id} -> bind
    不再默認下發舊 NVS policy。
    """
    mode = str(req.get("mode", req.get("profile", "default")))
    print(f"[MODE_V2] request mode={mode} req={req}", flush=True)
    try:
        bind = get_profile_binding(mode)
    except Exception as e:
        return err("unknown mode/profile", mode=mode, detail=str(e))

    target_rnti = resolve_control_rnti(req)
    if target_rnti is None:
        return err("rnti missing and cannot auto-resolve", hint="call ue_list / get_state, or pass rnti explicitly", scheduler_rnti=CURRENT_SCHEDULER_RNTI, last_rnti=LAST_RNTI, ue_list=list_ues())

    with _UE_LOCK:
        before_entry = dict(UE_REGISTRY.get(int(target_rnti)) or {})
    policy_before = _entry_policy_snapshot(before_entry)

    req2 = dict(req)
    req2["cmd"] = "ue_slice_assoc"
    req2["rnti"] = int(target_rnti)
    req2["dl_id"] = int(bind["dl_id"])
    req2["ul_id"] = int(bind["ul_id"])
    resp_bind = do_ue_slice_assoc(req2)

    policy_after = build_binding_runtime(int(bind["dl_id"]), int(bind["ul_id"]))
    policy_diff = _policy_diff(policy_before, policy_after)
    profile_meta = _get_profile_meta(mode)

    if resp_bind.get("ok"):
        r = int(resp_bind["rnti"])
        touch_ue(
            r,
            dl_id=int(bind["dl_id"]), ul_id=int(bind["ul_id"]), profile=mode,
            dl_mul=policy_after["dl"].get("weight_mul"), ul_mul=policy_after["ul"].get("weight_mul"),
            dl_cap=policy_after["dl"].get("rb_cap"), ul_cap=policy_after["ul"].get("rb_cap"),
            dl_floor=policy_after["dl"].get("rb_floor"), ul_floor=policy_after["ul"].get("rb_floor"),
            dl_maxcg=policy_after["dl"].get("max_consecutive_grants"), ul_maxcg=policy_after["ul"].get("max_consecutive_grants"),
            meta={"src": "set_mode_v2"},
        )
        STATE["active_mode"] = mode
        STATE["active_alg"] = "profile_binding_v2"
        STATE["last_profile"] = mode
        STATE["last_slice"] = int(bind["dl_id"])
        STATE["last_ctrl_ts"] = time.time()
        STATE["last_ctrl"] = {"cmd": "set_mode", "mode": mode, "rnti": r, "dl_id": int(bind["dl_id"]), "ul_id": int(bind["ul_id"])}

    return ok(mode=mode, alg="profile_binding_v2", bind=resp_bind, policy=policy_after, policy_before=policy_before, policy_diff=policy_diff, profile_meta=profile_meta)

def build_ul_dl_static_conf(sched_name: str, slices: List[Dict[str, Any]], keep_key: str):
    conf = ric.ul_dl_slice_conf_t()

    sn = str(sched_name)
    conf.sched_name = sn
    conf.len_sched_name = len(sn)
    KEEPALIVE[f"{keep_key}_sched_name"] = sn

    n = len(slices)
    conf.len_slices = n

    arr = ric.slice_array(n)
    KEEPALIVE[keep_key] = arr

    for i, s in enumerate(slices):
        fs = arr[i]
        fs.id = int(s["id"])

        label = str(s.get("label", f"slice{fs.id}"))
        fs.label = label
        fs.len_label = len(label)
        KEEPALIVE[f"{keep_key}_label_{fs.id}"] = label

        fs.sched = sn
        fs.len_sched = len(sn)

        if ALG_STATIC is not None:
            fs.params.type = ALG_STATIC
        else:
            fs.params.type = 0

        fs.params.u.sta.pos_low = int(s.get("pos_low", 0))
        fs.params.u.sta.pos_high = int(s.get("pos_high", 100))

    conf.slices = arr
    return conf


def do_add_static_slices(req: Dict[str, Any]):
    if CTRL_ADD is None:
        return err("Cannot find SLICE_CTRL_SM_V0_ADD* constant in SDK")

    sched_name = req.get("sched_name", "STATIC")
    dl_slices = req.get("dl", [])
    ul_slices = req.get("ul", dl_slices)

    if not dl_slices:
        return err("dl slices empty")

    sc = ric.slice_conf_t()
    sc.dl = build_ul_dl_static_conf(sched_name, dl_slices, keep_key="dl_slices")
    sc.ul = build_ul_dl_static_conf(sched_name, ul_slices, keep_key="ul_slices")

    ctrl = ric.slice_ctrl_msg_t()
    ctrl.type = CTRL_ADD
    ctrl.u.add_mod_slice = sc

    try:
        ret = control_slice_sm_safe(ctrl)
    except ControlSMTimeout as e:
        return err("control_slice_sm timeout", detail=str(e), timeout_s=CTRL_CALL_TIMEOUT_S)
    except Exception as e:
        return err("control_slice_sm failed", detail=str(e))

    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "add_static_slices", "dl": len(dl_slices), "ul": len(ul_slices)}
    return ok(ret=safe(ret), ctrl_type=int(ctrl.type), dl_count=len(dl_slices), ul_count=len(ul_slices))


def do_ue_slice_assoc(req: Dict[str, Any]):
    global LAST_RNTI

    if CTRL_UE_ASSOC is None:
        return err("SDK has no SLICE_CTRL_SM_V0_UE_SLICE_ASSOC")

    rnti = resolve_control_rnti(req)
    if rnti is None:
        return err(
            "rnti missing and cannot auto-resolve",
            hint="provide rnti explicitly, or start gnb_rnti_watcher.py",
            scheduler_rnti=CURRENT_SCHEDULER_RNTI,
            last_rnti=LAST_RNTI,
            ue_list=list_ues(),
        )

    dl_id = req.get("dl_id", None)
    if dl_id is None:
        return err("dl_id missing (must be >0)")
    ul_id = req.get("ul_id", dl_id)

    rnti = int(rnti)
    dl_id = int(dl_id)
    ul_id = int(ul_id)

    if rnti <= 0:
        return err(f"invalid rnti={rnti} (must be >0)")
    if dl_id <= 0 or ul_id <= 0:
        return err(f"invalid slice id dl_id={dl_id}, ul_id={ul_id} (must be >0)")

    uconf = ric.ue_slice_conf_t()
    uconf.len_ue_slice = 1

    arr = ric.ue_slice_assoc_array(1)
    KEEPALIVE["ue_assoc_arr"] = arr

    e = ric.ue_slice_assoc_t()
    e.rnti = rnti
    e.dl_id = dl_id
    e.ul_id = ul_id
    arr[0] = e

    rb = arr[0]
    print(f"[DBG] ue_slice_assoc ARRAY readback rnti={rb.rnti} dl_id={rb.dl_id} ul_id={rb.ul_id}", flush=True)

    try:
        uconf.ues = arr
    except Exception:
        uconf.ues = arr.cast()

    ctrl = ric.slice_ctrl_msg_t()
    ctrl.type = CTRL_UE_ASSOC
    ctrl.u.ue_slice = uconf

    print(f"[DBG] SEND UE_SLICE_ASSOC rnti={rnti} dl_id={dl_id} ul_id={ul_id}", flush=True)
    try:
        ret = control_slice_sm_safe(ctrl)
    except ControlSMTimeout as e:
        return err("control_slice_sm timeout", detail=str(e), timeout_s=CTRL_CALL_TIMEOUT_S)
    except Exception as e:
        return err("control_slice_sm failed", detail=str(e))

    # 更新状态
    # 更新 UE registry + last_rnti
    expected_policy = build_binding_runtime(dl_id, ul_id)
    touch_ue(
        rnti,
        dl_id=dl_id,
        ul_id=ul_id,
        dl_mul=expected_policy["dl"].get("weight_mul"),
        ul_mul=expected_policy["ul"].get("weight_mul"),
        dl_cap=expected_policy["dl"].get("rb_cap"),
        ul_cap=expected_policy["ul"].get("rb_cap"),
        dl_floor=expected_policy["dl"].get("rb_floor"),
        ul_floor=expected_policy["ul"].get("rb_floor"),
        dl_maxcg=expected_policy["dl"].get("max_consecutive_grants"),
        ul_maxcg=expected_policy["ul"].get("max_consecutive_grants"),
        meta={"src": "ue_slice_assoc"},
    )
    STATE["last_slice"] = dl_id
    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "ue_slice_assoc", "rnti": rnti, "dl_id": dl_id, "ul_id": ul_id}

    return ok(ret=safe(ret), rnti=rnti, dl_id=dl_id, ul_id=ul_id)


def do_set_profile(req: Dict[str, Any]):
    """v2: profile -> {dl_id, ul_id} -> ue_slice_assoc"""
    profile = str(req.get("profile", "default"))
    rnti = resolve_control_rnti(req)
    if rnti is None:
        return err("rnti missing and cannot auto-resolve", hint="call ue_list / get_state, or pass rnti explicitly", scheduler_rnti=CURRENT_SCHEDULER_RNTI, last_rnti=LAST_RNTI, ue_list=list_ues())
    try:
        bind = get_profile_binding(profile)
    except Exception as e:
        return err("unknown profile", profile=profile, detail=str(e))

    with _UE_LOCK:
        before_entry = dict(UE_REGISTRY.get(int(rnti)) or {})
    policy_before = _entry_policy_snapshot(before_entry)

    req2 = dict(req)
    req2["cmd"] = "ue_slice_assoc"
    req2["rnti"] = int(rnti)
    req2["dl_id"] = int(bind["dl_id"])
    req2["ul_id"] = int(bind["ul_id"])
    resp = do_ue_slice_assoc(req2)

    policy_after = build_binding_runtime(int(bind["dl_id"]), int(bind["ul_id"]))
    policy_diff = _policy_diff(policy_before, policy_after)
    profile_meta = _get_profile_meta(profile)

    if resp.get("ok"):
        rr = int(resp["rnti"])
        STATE["last_profile"] = profile
        STATE["last_slice"] = int(bind["dl_id"])
        STATE["last_ctrl_ts"] = time.time()
        STATE["last_ctrl"] = {"cmd": "set_profile", "profile": profile, "rnti": rr, "dl_id": int(bind["dl_id"]), "ul_id": int(bind["ul_id"])}
        touch_ue(
            rr,
            dl_id=int(bind["dl_id"]), ul_id=int(bind["ul_id"]), profile=profile,
            dl_mul=policy_after["dl"].get("weight_mul"), ul_mul=policy_after["ul"].get("weight_mul"),
            dl_cap=policy_after["dl"].get("rb_cap"), ul_cap=policy_after["ul"].get("rb_cap"),
            dl_floor=policy_after["dl"].get("rb_floor"), ul_floor=policy_after["ul"].get("rb_floor"),
            dl_maxcg=policy_after["dl"].get("max_consecutive_grants"), ul_maxcg=policy_after["ul"].get("max_consecutive_grants"),
            meta={"src": "set_profile_v2"},
        )

    return ok(profile=profile, bind=resp, policy=policy_after, policy_before=policy_before, policy_diff=policy_diff, profile_meta=profile_meta)
    
def do_set_ue_ul_la(req: Dict[str, Any]):
    rnti = resolve_control_rnti(req)
    if rnti is None:
        return err(
            "rnti missing and cannot auto-resolve",
            hint="provide rnti explicitly, or start gnb_rnti_watcher.py",
            scheduler_rnti=CURRENT_SCHEDULER_RNTI,
            last_rnti=LAST_RNTI,
            ue_list=list_ues(),
        )

    try:
        entry = _build_ue_posture_entry(req)
    except ValueError as e:
        return err("invalid UE posture field", detail=str(e))

    if not any(v is not None for v in entry.values()):
        return err("no UE posture fields provided", allowed=list(UE_POSTURE_FIELDS))

    with _UE_LA_LOCK:
        cur = dict(UE_UL_LA_REGISTRY.get(int(rnti)) or {})
        cur.update({k: v for k, v in entry.items() if v is not None})
        UE_UL_LA_REGISTRY[int(rnti)] = cur

    _write_ue_la_state_atomic()

    touch_ue(
        int(rnti),
        ul_max_mcs=entry.get("ul_max_mcs"),
        min_grant_prb=entry.get("min_grant_prb"),
        ulsch_max_frame_inactivity=entry.get("ulsch_max_frame_inactivity"),
        pusch_target_snrx10=entry.get("pusch_target_snrx10"),
        ul_sched_mul=entry.get("ul_sched_mul"),
        ul_maxcg_override=entry.get("ul_maxcg_override"),
        ul_small_burst_bytes=entry.get("ul_small_burst_bytes"),
        ul_small_burst_mul=entry.get("ul_small_burst_mul"),
        dl_max_mcs=entry.get("dl_max_mcs"),
        dl_min_grant_prb=entry.get("dl_min_grant_prb"),
        dl_sched_mul=entry.get("dl_sched_mul"),
        dl_maxcg_override=entry.get("dl_maxcg_override"),
        dl_small_burst_bytes=entry.get("dl_small_burst_bytes"),
        dl_small_burst_mul=entry.get("dl_small_burst_mul"),
        meta={"src": "set_ue_posture"},
    )

    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "set_ue_ul_la", "rnti": int(rnti), **{k: v for k, v in entry.items() if v is not None}}

    return ok(
        rnti=int(rnti),
        rnti_hex=hex(int(rnti)),
        entry=dict(UE_UL_LA_REGISTRY.get(int(rnti)) or {}),
        state_path=UE_LA_STATE_PATH,
    )

def do_get_ue_ul_la(req: Dict[str, Any]):
    rnti = resolve_control_rnti(req)
    if rnti is None:
        return err(
            "rnti missing and cannot auto-resolve",
            scheduler_rnti=CURRENT_SCHEDULER_RNTI,
            last_rnti=LAST_RNTI,
            ue_list=list_ues(),
        )

    with _UE_LA_LOCK:
        entry = dict(UE_UL_LA_REGISTRY.get(int(rnti)) or {})

    return ok(
        rnti=int(rnti),
        rnti_hex=hex(int(rnti)),
        entry=entry,
        state_path=UE_LA_STATE_PATH,
    )

def do_clear_ue_ul_la(req: Dict[str, Any]):
    rnti = resolve_control_rnti(req)
    if rnti is None:
        return err(
            "rnti missing and cannot auto-resolve",
            scheduler_rnti=CURRENT_SCHEDULER_RNTI,
            last_rnti=LAST_RNTI,
            ue_list=list_ues(),
        )

    with _UE_LA_LOCK:
        UE_UL_LA_REGISTRY.pop(int(rnti), None)

    _write_ue_la_state_atomic()
    _clear_ue_posture_snapshot(int(rnti))

    touch_ue(
        int(rnti),
        meta={"src": "clear_ue_posture"},
    )

    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "clear_ue_ul_la", "rnti": int(rnti)}

    return ok(
        rnti=int(rnti),
        rnti_hex=hex(int(rnti)),
        cleared=True,
        state_path=UE_LA_STATE_PATH,
    )
    
def _set_pct_reserved(cap_obj, pct):
    for path in [("pct_reserved",), ("u", "pct_reserved")]:
        try:
            t = cap_obj
            for p in path[:-1]:
                t = getattr(t, p)
            setattr(t, path[-1], float(pct))
            return True
        except Exception:
            pass
    try:
        cap_obj = float(pct)
        return True
    except Exception:
        return False


def build_ul_dl_nvs_capacity_conf(sched_name: str, slices, keep_key: str):
    conf = ric.ul_dl_slice_conf_t()
    sn = str(sched_name)
    conf.sched_name = sn
    conf.len_sched_name = len(sn)
    KEEPALIVE[f"{keep_key}_sched_name"] = sn

    n = len(slices)
    conf.len_slices = n
    arr = ric.slice_array(n)
    KEEPALIVE[keep_key] = arr

    for i, s in enumerate(slices):
        fs = arr[i]
        fs.id = int(s["id"])
        label = str(s.get("label", f"slice{fs.id}"))
        fs.label = label
        fs.len_label = len(label)
        KEEPALIVE[f"{keep_key}_label_{fs.id}"] = label

        fs.sched = sn
        fs.len_sched = len(sn)

        fs.params.type = ALG_NVS
        fs.params.u.nvs.conf = NVS_CAP

        pct = s.get("pct_reserved", s.get("pct", 10))
        ok_set = _set_pct_reserved(fs.params.u.nvs.u.capacity, pct)
        if not ok_set:
            raise RuntimeError(f"Cannot set pct_reserved for slice id={fs.id}")

    conf.slices = arr
    return conf


def do_add_nvs_slices(req):
    if CTRL_ADD is None:
        return err("Cannot find SLICE_CTRL_SM_V0_ADD constant")

    dl_slices = req.get("dl", [])
    ul_slices = req.get("ul", dl_slices)
    if not dl_slices:
        return err("dl slices empty")

    sc = ric.slice_conf_t()
    sc.dl = build_ul_dl_nvs_capacity_conf("NVS", dl_slices, "dl_nvs")
    sc.ul = build_ul_dl_nvs_capacity_conf("NVS", ul_slices, "ul_nvs")

    ctrl = ric.slice_ctrl_msg_t()
    ctrl.type = CTRL_ADD
    ctrl.u.add_mod_slice = sc

    try:
        ret = control_slice_sm_safe(ctrl)
    except ControlSMTimeout as e:
        return err("control_slice_sm timeout", detail=str(e), timeout_s=CTRL_CALL_TIMEOUT_S)
    except Exception as e:
        return err("control_slice_sm failed", detail=str(e))

    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "add_nvs_slices", "dl": len(dl_slices), "ul": len(ul_slices)}
    return ok(ret=safe(ret), ctrl_type=int(ctrl.type), dl_count=len(dl_slices), ul_count=len(ul_slices))


def build_ul_dl_nvs_rate_conf(sched_name: str, slices, keep_key: str):
    conf = ric.ul_dl_slice_conf_t()

    sn = str(sched_name)
    conf.sched_name = sn
    conf.len_sched_name = len(sn)
    KEEPALIVE[f"{keep_key}_sched_name"] = sn

    n = len(slices)
    conf.len_slices = n

    arr = ric.slice_array(n)
    KEEPALIVE[keep_key] = arr

    for i, s in enumerate(slices):
        fs = arr[i]
        fs.id = int(s["id"])

        label = str(s.get("label", f"slice{fs.id}"))
        fs.label = label
        fs.len_label = len(label)
        KEEPALIVE[f"{keep_key}_label_{fs.id}"] = label

        fs.sched = sn
        fs.len_sched = len(sn)

        fs.params.type = ALG_NVS
        fs.params.u.nvs.conf = NVS_RATE

        req_m = float(s.get("mbps_required", 0))
        ref_m = float(s.get("mbps_reference", 100))

        fs.params.u.nvs.u.rate.u1.mbps_required = req_m
        fs.params.u.nvs.u.rate.u2.mbps_reference = ref_m

    conf.slices = arr
    return conf


def do_add_nvs_rate_slices(req):
    if CTRL_ADD is None:
        return err("Cannot find SLICE_CTRL_SM_V0_ADD constant")

    dl_slices = req.get("dl", [])
    ul_slices = req.get("ul", dl_slices)
    if not dl_slices:
        return err("dl slices empty")

    sc = ric.slice_conf_t()
    sc.dl = build_ul_dl_nvs_rate_conf("NVS", dl_slices, "dl_nvs_rate")
    sc.ul = build_ul_dl_nvs_rate_conf("NVS", ul_slices, "ul_nvs_rate")

    ctrl = ric.slice_ctrl_msg_t()
    ctrl.type = CTRL_ADD
    ctrl.u.add_mod_slice = sc

    try:
        ret = control_slice_sm_safe(ctrl)
    except ControlSMTimeout as e:
        return err("control_slice_sm timeout", detail=str(e), timeout_s=CTRL_CALL_TIMEOUT_S)
    except Exception as e:
        return err("control_slice_sm failed", detail=str(e))

    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "add_nvs_rate_slices", "dl": len(dl_slices), "ul": len(ul_slices)}
    return ok(ret=safe(ret), ctrl_type=int(ctrl.type), dl_count=len(dl_slices), ul_count=len(ul_slices))


def do_get_state():
    ninfo = None
    try:
        if node is not None:
            ninfo = {"mcc": int(node.id.plmn.mcc), "mnc": int(node.id.plmn.mnc)}
    except Exception:
        ninfo = None

    return ok(
        node=ninfo,
        started_at=STATE["started_at"],
        uptime_s=time.time() - STATE["started_at"],
        last_rnti=STATE["last_rnti"],
        last_rnti_hex=(hex(STATE["last_rnti"]) if STATE["last_rnti"] else None),
        scheduler_rnti=CURRENT_SCHEDULER_RNTI,
        scheduler_rnti_hex=(hex(CURRENT_SCHEDULER_RNTI) if CURRENT_SCHEDULER_RNTI else None),
        scheduler_rnti_ts=CURRENT_SCHEDULER_RNTI_TS,
        last_profile=STATE["last_profile"],
        last_slice=STATE["last_slice"],
        last_ctrl_ts=STATE["last_ctrl_ts"],
        last_ctrl=STATE["last_ctrl"],
        profiles_path=STATE["profiles_path"],
        profiles_loaded_at=STATE["profiles_loaded_at"],
        profiles_loaded=(STATE["profiles"] is not None),
        active_mode=STATE.get("active_mode"),
        active_alg=STATE.get("active_alg"),
        ue_la_state_path=UE_LA_STATE_PATH,
    )


def do_apply_profiles(req: Dict[str, Any]):
    """
    cmd: apply_profiles
    读取 profiles.json -> 下发 NVS_RATE slices
    可选参数:
      {"cmd":"apply_profiles","force_reload":true}
    """
    force = bool(req.get("force_reload", False))
    p = load_profiles(force=force)
    if not p:
        return err("profiles.json not loaded", profiles_path=PROFILES_PATH)

    try:
        dl, ul = profile_to_rate_slices(p)
    except Exception as e:
        return err("profiles.json missing required keys", detail=str(e))

    resp = do_add_nvs_rate_slices({"dl": dl, "ul": ul})

    STATE["last_ctrl_ts"] = time.time()
    STATE["last_ctrl"] = {"cmd": "apply_profiles", "dl": len(dl), "ul": len(ul)}
    return resp


def handle(req: Dict[str, Any]):
    global LAST_RNTI, CURRENT_SCHEDULER_RNTI, CURRENT_SCHEDULER_RNTI_TS

    cmd = req.get("cmd", "")

    if cmd == "ping":
        return ok(pong=True)

    if cmd == "get_state":
        return do_get_state()

    if cmd == "apply_profiles":
        return do_apply_profiles(req)

    if cmd == "introspect":
        return ok(info={
            "CTRL_ADD": safe(CTRL_ADD),
            "CTRL_DEL": safe(CTRL_DEL),
            "CTRL_UE_ASSOC": safe(CTRL_UE_ASSOC),
            "ALG_STATIC": safe(ALG_STATIC),
            "PROFILE_MAP": PROFILE_MAP,
            "profiles_path": PROFILES_PATH,
            "profiles_loaded": (STATE["profiles"] is not None),
            "UE_TTL_S": UE_TTL_S,
            "slice_ctrl_msg_u": [a for a in dir(ric.slice_ctrl_msg_u) if not a.startswith("_")],
            "ul_dl_slice_conf_t": [a for a in dir(ric.ul_dl_slice_conf_t) if not a.startswith("_")],
            "fr_slice_t": [a for a in dir(ric.fr_slice_t) if not a.startswith("_")],
            "slice_params_union": [a for a in dir(ric.slice_params_t().u) if not a.startswith("_")],
            "has_slice_array": hasattr(ric, "slice_array"),
            "has_ue_slice_assoc_array": hasattr(ric, "ue_slice_assoc_array"),
        })
    
    if cmd in ("set_ue_ul_la", "set_ue_posture"):
        return do_set_ue_ul_la(req)

    if cmd in ("get_ue_ul_la", "get_ue_posture"):
        return do_get_ue_ul_la(req)

    if cmd in ("clear_ue_ul_la", "clear_ue_posture"):
        return do_clear_ue_ul_la(req)

    if cmd == "reset_ues":
        with _UE_LOCK:
            UE_REGISTRY.clear()
        LAST_RNTI = None
        try:
            STATE["last_rnti"] = None
        except Exception:
            pass
        return ok(cleared=True, ttl_s=UE_TTL_S)

    if cmd == "set_last_rnti":
        r = int(req["rnti"])
        # 写入 UE_REGISTRY + LAST_RNTI + STATE
        touch_ue(r, meta={"src": "set_last_rnti"})
        return ok(last_rnti=r, ues=list_ues(), ttl_s=UE_TTL_S)

    if cmd == "get_last_rnti":
        return ok(last_rnti=LAST_RNTI)
    
    if cmd == "update_scheduler_rnti":

        r = req.get("rnti", None)
        if r is None:
            return err("rnti missing")

        try:
            r = int(r)
        except Exception:
            return err("invalid rnti", detail=req.get("rnti"))

        meta = req.get("meta", None)
        try:
            meta = dict(meta) if isinstance(meta, dict) else {}
        except Exception:
            meta = {}

        meta.setdefault("src", "gnb_rnti_watcher")

        CURRENT_SCHEDULER_RNTI = r
        CURRENT_SCHEDULER_RNTI_TS = time.time()

        # 为了兼容现有逻辑，也顺手刷新 LAST_RNTI
        LAST_RNTI = r

        STATE["scheduler_rnti"] = r
        STATE["scheduler_rnti_ts"] = CURRENT_SCHEDULER_RNTI_TS
        STATE["last_rnti"] = r

        reason = str(meta.get("reason") or "")
        ul_event_inc = 1 if reason.startswith("ul_") else 0
        dl_event_inc = 1 if reason.startswith("dl_") else 0

        touch_ue(
            r,
            meta=meta,
            dl_id=req.get("dl_id"),
            ul_id=req.get("ul_id"),
            dl_mul=req.get("dl_mul"),
            ul_mul=req.get("ul_mul"),
            dl_cap=req.get("dl_cap"),
            ul_cap=req.get("ul_cap"),
            dl_floor=req.get("dl_floor"),
            ul_floor=req.get("ul_floor"),
            dl_maxcg=req.get("dl_maxcg"),
            ul_maxcg=req.get("ul_maxcg"),
            dl_rbSize=req.get("dl_rbSize"),
            ul_rbSize=req.get("ul_rbSize"),
            dl_current_rbs=req.get("dl_current_rbs"),
            ul_current_rbs=req.get("ul_current_rbs"),
            ul_max_mcs=req.get("ul_max_mcs"),
            min_grant_prb=req.get("min_grant_prb"),
            ulsch_max_frame_inactivity=req.get("ulsch_max_frame_inactivity"),
            pusch_target_snrx10=req.get("pusch_target_snrx10"),
            ul_sched_mul=req.get("ul_sched_mul"),
            ul_maxcg_override=req.get("ul_maxcg_override"),
            ul_small_burst_bytes=req.get("ul_small_burst_bytes"),
            ul_small_burst_mul=req.get("ul_small_burst_mul"),
            ul_small_burst_hit=req.get("ul_small_burst_hit"),
            dl_max_mcs=req.get("dl_max_mcs"),
            dl_min_grant_prb=req.get("dl_min_grant_prb"),
            dl_sched_mul=req.get("dl_sched_mul"),
            dl_maxcg_override=req.get("dl_maxcg_override"),
            dl_small_burst_bytes=req.get("dl_small_burst_bytes"),
            dl_small_burst_mul=req.get("dl_small_burst_mul"),
            dl_small_burst_hit=req.get("dl_small_burst_hit"),
            tpc0=req.get("tpc0"),
            pusch_snrx10=req.get("pusch_snrx10"),
            dl_throttled=req.get("dl_throttled"),
            ul_throttled=req.get("ul_throttled"),
            ul_event_inc=ul_event_inc,
            dl_event_inc=dl_event_inc,
            ul_grant_inc=(1 if reason == "ul_alloc" else 0),
            dl_grant_inc=(1 if reason == "dl_alloc" else 0),
            last_reason=reason,
        )

        return ok(
            scheduler_rnti=r,
            scheduler_rnti_hex=hex(r),
            scheduler_rnti_ts=CURRENT_SCHEDULER_RNTI_TS,
            ttl_s=UE_TTL_S,
        )


    if cmd == "get_scheduler_rnti":
        return ok(
            scheduler_rnti=CURRENT_SCHEDULER_RNTI,
            scheduler_rnti_hex=(hex(CURRENT_SCHEDULER_RNTI) if CURRENT_SCHEDULER_RNTI else None),
            scheduler_rnti_ts=CURRENT_SCHEDULER_RNTI_TS,
        )

    if cmd == "ue_slice_assoc":
        return do_ue_slice_assoc(req)

    if cmd == "add_static_slices":
        return do_add_static_slices(req)

    if cmd == "set_mode":
        return do_set_mode(req)

    if cmd == "set_profile":
        return do_set_profile(req)

    if cmd == "add_nvs_slices":
        return do_add_nvs_slices(req)

    if cmd == "add_nvs_rate_slices":
        return do_add_nvs_rate_slices(req)

    if cmd == "ue_list":
        role = req.get("role")
        active_only = bool(req.get("active_only", True))
        max_age_s = float(req.get("max_age_s", UE_ACTIVE_MAX_AGE_S))
        src = req.get("src")

        ues = list_ues(active_only=active_only, max_age_s=max_age_s, src=src)

        if role:
            ues = _pick_by_role(ues, role)

        return ok(
            ues=ues,
            ttl_s=UE_TTL_S,
            active_only=active_only,
            max_age_s=max_age_s,
            src=src,
        )

    if cmd == "tag_ue":
        r = req.get("rnti", None)
        if r is None:
            return err("rnti missing")
        role = str(req.get("role", "unknown"))
        touch_ue(int(r), role=role, meta={"role_src": "tag_ue"})
        return ok(
            rnti=int(r),
            role=role,
            ues=list_ues(active_only=True, max_age_s=UE_ACTIVE_MAX_AGE_S, src="gnb_rnti_watcher"),
            ttl_s=UE_TTL_S,
        )

    if cmd == "update_ue":
        # watcher/monitor 推送：可带 ue_idx/ul_bytes/dl_bytes/rsrp/snr + meta
        r = req.get("rnti", None)
        if r is None:
            return err("rnti missing")

        meta = req.get("meta", None)
        try:
            meta = dict(meta) if isinstance(meta, dict) else None
        except Exception:
            meta = None

        touch_ue(
            int(r),
            meta=meta,
            ue_idx=req.get("ue_idx"),
            ul_bytes=req.get("ul_bytes"),
            dl_bytes=req.get("dl_bytes"),
            rsrp=req.get("rsrp"),
            snr=req.get("snr"),
        )
        return ok(rnti=int(r), ues=list_ues(), ttl_s=UE_TTL_S)

    return err("unknown cmd", cmd=cmd)


def serve_forever():
    import threading

    def handle_conn(conn, addr):
        conn.settimeout(0.3)
        try:
            buf = b""
            deadline = time.time() + 0.6
            while time.time() < deadline:
                try:
                    chunk = conn.recv(65535)
                except socket.timeout:
                    break
                if not chunk:
                    break
                buf += chunk
                if b"\n" in buf:
                    break
                try:
                    json.loads(buf.decode("utf-8", errors="replace").strip())
                    break
                except Exception:
                    pass

            if not buf:
                return

            text = buf.decode("utf-8", errors="replace").strip()
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            last_resp = None

            for ln in lines:
                try:
                    req = json.loads(ln)
                    cmd = req.get("cmd", "")
                    print(f"[REQ] from={addr[0]}:{addr[1]} cmd={cmd}", flush=True)
                    last_resp = handle(req)
                except Exception as e:
                    last_resp = err("exception", detail=str(e), traceback=traceback.format_exc())

            if last_resp is None:
                last_resp = ok()

            conn.sendall((json.dumps(last_resp) + "\n").encode("utf-8"))

        finally:
            try:
                conn.close()
            except Exception:
                pass

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(64)
    print(f"control_xapp listening on {HOST}:{PORT}", flush=True)

    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()

def main():
    global node
    ric.init()
    nodes = ric.conn_e2_nodes()
    if not nodes:
        raise RuntimeError("no E2 nodes connected")
    node = nodes[0]
    print(f"control_xapp connected: node_count={len(nodes)}", flush=True)
    print("control_xapp connected:", node.id.plmn.mcc, node.id.plmn.mnc, flush=True)

    # 尝试预加载 profiles（不存在也不影响服务）
    load_profiles(force=False)

    serve_forever()


if __name__ == "__main__":
    main()
