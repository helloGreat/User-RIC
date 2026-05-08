#!/usr/bin/env python3
import os, sys, json, socket, traceback, time, select, subprocess, re, statistics
from typing import Any, Dict, Optional, List, Tuple

# -------- backend (Spark control_xapp TCP) --------
BACKEND_HOST = os.environ.get("RIC_BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.environ.get("RIC_BACKEND_PORT", "7777"))

# 兼容两种命名：你 agent.servers.json 里用的是 *_TIMEOUT_S
BACKEND_TIMEOUT_S = float(
    os.environ.get("RIC_BACKEND_TIMEOUT_S",
        os.environ.get("RIC_BACKEND_TIMEOUT", "5.0")
    )
)

# -------- local speedtest (Jetson -> Spark iperf3) --------
SPEEDTEST_HOST = os.environ.get("RIC_SPEEDTEST_HOST", BACKEND_HOST)
SPEEDTEST_PORT = int(os.environ.get("RIC_SPEEDTEST_PORT", "5203"))
SPEEDTEST_DURATION_S = int(os.environ.get("RIC_SPEEDTEST_DURATION_S", "5"))
SPEEDTEST_PARALLEL = int(os.environ.get("RIC_SPEEDTEST_PARALLEL", "4"))
SPEEDTEST_SETTLE_AFTER_MODE_S = float(os.environ.get("RIC_SPEEDTEST_SETTLE_AFTER_MODE_S", "2.0"))

# -------- UL effect measurement packs --------
EFFECT_PING_HOST = os.environ.get("RIC_EFFECT_PING_HOST", SPEEDTEST_HOST)

EFFECT_QUICK_UL_SECONDS = int(os.environ.get("RIC_EFFECT_QUICK_UL_SECONDS", "3"))
EFFECT_QUICK_UL_PARALLEL = int(os.environ.get("RIC_EFFECT_QUICK_UL_PARALLEL", "2"))
EFFECT_QUICK_BURST_BYTES = int(os.environ.get("RIC_EFFECT_QUICK_BURST_BYTES", str(64 * 1024)))
EFFECT_QUICK_BURST_REPEAT = int(os.environ.get("RIC_EFFECT_QUICK_BURST_REPEAT", "3"))
EFFECT_QUICK_IDLE_RESUME_REPEAT = int(os.environ.get("RIC_EFFECT_QUICK_IDLE_RESUME_REPEAT", "3"))

EFFECT_FULL_UL_SECONDS = int(os.environ.get("RIC_EFFECT_FULL_UL_SECONDS", "5"))
EFFECT_FULL_UL_PARALLEL = int(os.environ.get("RIC_EFFECT_FULL_UL_PARALLEL", "4"))
EFFECT_FULL_BURST_SMALL_BYTES = int(os.environ.get("RIC_EFFECT_FULL_BURST_SMALL_BYTES", str(64 * 1024)))
EFFECT_FULL_BURST_LARGE_BYTES = int(os.environ.get("RIC_EFFECT_FULL_BURST_LARGE_BYTES", str(256 * 1024)))
EFFECT_FULL_BURST_REPEAT = int(os.environ.get("RIC_EFFECT_FULL_BURST_REPEAT", "8"))
EFFECT_FULL_IDLE_RESUME_REPEAT = int(os.environ.get("RIC_EFFECT_FULL_IDLE_RESUME_REPEAT", "5"))

EFFECT_IDLE_SLEEP_S = float(os.environ.get("RIC_EFFECT_IDLE_SLEEP_S", "1.5"))
EFFECT_PING_INTERVAL_S = float(os.environ.get("RIC_EFFECT_PING_INTERVAL_S", "0.2"))
EFFECT_STAGE_BUDGET_S = float(os.environ.get("RIC_EFFECT_STAGE_BUDGET_S", "45.0"))

PACK_LABELS = {
    "quick": "quick pack",
    "full_sustained": "full pack 第1阶段（持续吞吐）",
    "full_burst": "full pack 第2阶段（burst / 小包突发）",
    "full_resume": "full pack 第3阶段（空闲后恢复）",
    "full_rtt": "full pack 第4阶段（负载下 RTT）",
}

STAGED_FULL_PACKS = ("full_sustained", "full_burst", "full_resume", "full_rtt")

def _remaining_budget(deadline_ts: Optional[float]) -> Optional[float]:
    if deadline_ts is None:
        return None
    return max(0.0, float(deadline_ts) - time.time())

def _should_stop_for_budget(deadline_ts: Optional[float], reserve_s: float = 1.0) -> bool:
    rem = _remaining_budget(deadline_ts)
    return rem is not None and rem <= float(reserve_s)

def _timeout_cap_from_deadline(deadline_ts: Optional[float], reserve_s: float = 0.5) -> Optional[float]:
    rem = _remaining_budget(deadline_ts)
    if rem is None:
        return None
    rem = rem - float(reserve_s)
    return max(2.0, rem) if rem > 0 else 2.0


UL_LINK_PARAM_KEYS = (
    "ul_max_mcs",
    "min_grant_prb",
    "ulsch_max_frame_inactivity",
    "pusch_target_snrx10",
)

UL_SCHED_PARAM_KEYS = (
    "ul_sched_mul",
    "ul_maxcg_override",
    "ul_small_burst_bytes",
    "ul_small_burst_mul",
)

DL_LINK_PARAM_KEYS = (
    "dl_max_mcs",
    "dl_min_grant_prb",
)

DL_SCHED_PARAM_KEYS = (
    "dl_sched_mul",
    "dl_maxcg_override",
    "dl_small_burst_bytes",
    "dl_small_burst_mul",
)

UL_POSTURE_PARAM_KEYS = UL_LINK_PARAM_KEYS + UL_SCHED_PARAM_KEYS
FULL_POSTURE_PARAM_KEYS = UL_LINK_PARAM_KEYS + UL_SCHED_PARAM_KEYS + DL_LINK_PARAM_KEYS + DL_SCHED_PARAM_KEYS

POSTURE_PARAM_GROUPS = {
    "ul_link": UL_LINK_PARAM_KEYS,
    "ul_sched": UL_SCHED_PARAM_KEYS,
    "dl_link": DL_LINK_PARAM_KEYS,
    "dl_sched": DL_SCHED_PARAM_KEYS,
}

POSTURE_GROUP_LABELS = {
    "ul_link": "上行链路姿态",
    "ul_sched": "上行调度姿态",
    "dl_link": "下行链路姿态",
    "dl_sched": "下行调度姿态",
}

POSTURE_PARAM_LABELS = {
    "ul_max_mcs": "ul_max_mcs",
    "min_grant_prb": "min_grant_prb",
    "ulsch_max_frame_inactivity": "ulsch_max_frame_inactivity",
    "pusch_target_snrx10": "pusch_target_snrx10",
    "ul_sched_mul": "ul_sched_mul",
    "ul_maxcg_override": "ul_maxcg_override",
    "ul_small_burst_bytes": "ul_small_burst_bytes",
    "ul_small_burst_mul": "ul_small_burst_mul",
    "dl_max_mcs": "dl_max_mcs",
    "dl_min_grant_prb": "dl_min_grant_prb",
    "dl_sched_mul": "dl_sched_mul",
    "dl_maxcg_override": "dl_maxcg_override",
    "dl_small_burst_bytes": "dl_small_burst_bytes",
    "dl_small_burst_mul": "dl_small_burst_mul",
}

UL_POSTURE_LABELS = {k: POSTURE_PARAM_LABELS[k] for k in UL_POSTURE_PARAM_KEYS}

POSTURE_PARAM_EFFECTS = {
    "ul_max_mcs": "控制上行编码激进度；更低更稳，但峰值可能下降。",
    "min_grant_prb": "控制上行单次最小 grant；更大时突发上传更利索，但更占资源。",
    "ulsch_max_frame_inactivity": "控制空闲后重新积极调度的等待；更小时恢复发送更快。",
    "pusch_target_snrx10": "控制上行目标接收质量；更高时更偏稳、更容易拉高接收质量。",
    "ul_sched_mul": "在 slice 之后再给上行排序加一层 per-UE 乘子；更大时更容易先被调度。",
    "ul_maxcg_override": "覆盖该 UE 的上行连续 grants 上限；更大更适合 burst/冲刺上传。",
    "ul_small_burst_bytes": "定义上行小 burst 的判定门槛。",
    "ul_small_burst_mul": "命中上行小 burst 后额外加权；更大时小控制包/小上传更容易先发。",
    "dl_max_mcs": "控制下行编码激进度；更低更稳，但下载峰值可能下降。",
    "dl_min_grant_prb": "控制下行单次最小 grant；更大时首包/小块下发更利索。",
    "dl_sched_mul": "在 slice 之后再给下行排序加一层 per-UE 乘子；更大时更容易优先下发。",
    "dl_maxcg_override": "覆盖该 UE 的下行连续 grants 上限；更大更适合连续下发。",
    "dl_small_burst_bytes": "定义下行小 burst 的判定门槛。",
    "dl_small_burst_mul": "命中下行小 burst 后额外加权；更大时首包/小结果回传更容易优先送达。",
}

FULL_POSTURE_PRESETS: Dict[str, Dict[str, Any]] = {
    "posture_default": {
        "label": "默认通信姿态",
        "clear": True,
        "human_summary": "清除显式 per-UE posture override，回到 gNB 默认姿态。",
        "effect_summary": "不再额外偏置上下行链路和调度参数，适合作为基线或回退状态。",
    },
    "posture_interactive": {
        "label": "交互优先姿态",
        "ul_max_mcs": 10,
        "min_grant_prb": 8,
        "ulsch_max_frame_inactivity": 1,
        "pusch_target_snrx10": 220,
        "ul_sched_mul": 1.30,
        "ul_maxcg_override": 8,
        "ul_small_burst_bytes": 4096,
        "ul_small_burst_mul": 1.80,
        "dl_max_mcs": 18,
        "dl_min_grant_prb": 8,
        "dl_sched_mul": 1.50,
        "dl_maxcg_override": 6,
        "dl_small_burst_bytes": 4096,
        "dl_small_burst_mul": 1.60,
        "human_summary": "适合实时互动、控制流、首包敏感场景。",
        "effect_summary": "同时提升上下行小 burst 的优先级，并缩短上行空闲恢复时间，让交互更灵敏。",
    },
    "posture_stable": {
        "label": "稳定优先姿态",
        "ul_max_mcs": 12,
        "min_grant_prb": 8,
        "ulsch_max_frame_inactivity": 4,
        "pusch_target_snrx10": 220,
        "ul_sched_mul": 1.15,
        "ul_maxcg_override": 6,
        "ul_small_burst_bytes": 4096,
        "ul_small_burst_mul": 1.40,
        "dl_max_mcs": 16,
        "dl_min_grant_prb": 8,
        "dl_sched_mul": 1.30,
        "dl_maxcg_override": 5,
        "dl_small_burst_bytes": 4096,
        "dl_small_burst_mul": 1.40,
        "human_summary": "适合视频会议、直播、弱覆盖和希望更稳的链路。",
        "effect_summary": "上下行都会更保守，更偏稳定和连续可用性，而不是极限峰值。",
    },
    "posture_background_safe": {
        "label": "后台保守姿态",
        "ul_max_mcs": 12,
        "min_grant_prb": 4,
        "ulsch_max_frame_inactivity": 10,
        "pusch_target_snrx10": 180,
        "ul_sched_mul": 0.85,
        "ul_maxcg_override": 2,
        "ul_small_burst_bytes": 2048,
        "ul_small_burst_mul": 1.10,
        "dl_max_mcs": 18,
        "dl_min_grant_prb": 5,
        "dl_sched_mul": 0.90,
        "dl_maxcg_override": 2,
        "dl_small_burst_bytes": 2048,
        "dl_small_burst_mul": 1.10,
        "human_summary": "适合后台上传、夜间同步、不想抢资源的场景。",
        "effect_summary": "调度偏置更保守、连续 grants 更短，优先减少对其他 UE 的干扰。",
    },
    "posture_aggressive": {
        "label": "吞吐偏置姿态",
        "ul_max_mcs": 20,
        "min_grant_prb": 8,
        "ulsch_max_frame_inactivity": 2,
        "pusch_target_snrx10": 200,
        "ul_sched_mul": 1.35,
        "ul_maxcg_override": 10,
        "ul_small_burst_bytes": 8192,
        "ul_small_burst_mul": 1.60,
        "dl_max_mcs": 24,
        "dl_min_grant_prb": 8,
        "dl_sched_mul": 1.40,
        "dl_maxcg_override": 8,
        "dl_small_burst_bytes": 8192,
        "dl_small_burst_mul": 1.40,
        "human_summary": "适合连续下载/上传、素材传输、吞吐优先场景。",
        "effect_summary": "上下行都更偏持续吞吐，同时仍对中小 burst 维持一定优先级。",
    },
    "posture_plain_text": {
        "label": "普通文字姿态",
        "ul_max_mcs": 12,
        "min_grant_prb": 6,
        "ulsch_max_frame_inactivity": 6,
        "pusch_target_snrx10": 200,
        "ul_sched_mul": 1.00,
        "ul_maxcg_override": 4,
        "ul_small_burst_bytes": 3072,
        "ul_small_burst_mul": 1.20,
        "dl_max_mcs": 18,
        "dl_min_grant_prb": 6,
        "dl_sched_mul": 1.05,
        "dl_maxcg_override": 4,
        "dl_small_burst_bytes": 3072,
        "dl_small_burst_mul": 1.20,
        "human_summary": "适合普通文字传输、轻量聊天和不强调抢首包的轻交互。",
        "effect_summary": "上下行都保持轻量和温和调度，比交互优先姿态更克制，比后台保守姿态更灵敏。",
    },
    "posture_agentic_loop": {
        "label": "Agentic Loop 姿态",
        "ul_max_mcs": 10,
        "min_grant_prb": 8,
        "ulsch_max_frame_inactivity": 1,
        "pusch_target_snrx10": 220,
        "ul_sched_mul": 1.45,
        "ul_maxcg_override": 6,
        "ul_small_burst_bytes": 2048,
        "ul_small_burst_mul": 2.00,
        "dl_max_mcs": 18,
        "dl_min_grant_prb": 8,
        "dl_sched_mul": 1.65,
        "dl_maxcg_override": 4,
        "dl_small_burst_bytes": 2048,
        "dl_small_burst_mul": 1.90,
        "human_summary": "适合多轮 agent 交互、工具调用和双向小包回环。",
        "effect_summary": "同时强化上下行的小 burst 优先级和空闲后恢复速度，更适合短周期请求-结果回环，而不是长 burst 连续占用。",
    },
    "posture_anti_jitter": {
        "label": "抗抖动姿态",
        "ul_max_mcs": 11,
        "min_grant_prb": 6,
        "ulsch_max_frame_inactivity": 3,
        "pusch_target_snrx10": 230,
        "ul_sched_mul": 1.05,
        "ul_maxcg_override": 3,
        "ul_small_burst_bytes": 3072,
        "ul_small_burst_mul": 1.25,
        "dl_max_mcs": 16,
        "dl_min_grant_prb": 6,
        "dl_sched_mul": 1.10,
        "dl_maxcg_override": 3,
        "dl_small_burst_bytes": 3072,
        "dl_small_burst_mul": 1.25,
        "human_summary": "适合压 jitter、收敛 RTT 波动和保护交互尾时延。",
        "effect_summary": "调度偏置保持适中、连续 grants 更短、链路更保守，目标是减小波动而不是追求极限峰值。",
    },
}

UL_POSTURE_PRESETS: Dict[str, Dict[str, Any]] = {}
for _full_name, _preset in FULL_POSTURE_PRESETS.items():
    _ul_name = _full_name.replace("posture_", "ul_") if _full_name.startswith("posture_") else _full_name
    _one: Dict[str, Any] = {
        k: v for k, v in _preset.items()
        if k in UL_POSTURE_PARAM_KEYS or k in ("label", "clear", "human_summary", "effect_summary")
    }
    if _one.get("label"):
        _one["label"] = str(_one["label"]).replace("通信", "上行")
    UL_POSTURE_PRESETS[_ul_name] = _one

del _full_name, _preset, _ul_name, _one

MODE_TO_POSTURE = {
    "default": "posture_default",
    "text": "posture_interactive",
    "image": "posture_aggressive",
    "video": "posture_stable",
    "high_throughput_boost": "posture_aggressive",
    "low_latency_guard": "posture_interactive",
    "burst_uplink": "posture_interactive",
    "background_upload": "posture_background_safe",
    "night_idle": "posture_background_safe",
    "fairness_guard": "posture_background_safe",
    "plain_text_guard": "posture_plain_text",
    "agentic_loop": "posture_agentic_loop",
    "anti_jitter_guard": "posture_anti_jitter",
}

MODE_TO_UL_POSTURE = {
    k: v.replace("posture_", "ul_") if v.startswith("posture_") else v
    for k, v in MODE_TO_POSTURE.items()
}

VALID_MODE_NAMES = tuple(MODE_TO_POSTURE.keys())

def _normalize_mode_alias(mode: str) -> str:
    m = (mode or "").strip().lower()
    alias = {
        "background": "background_upload",
        "bg": "background_upload",
        "background_mode": "background_upload",
        "background upload": "background_upload",
        "background-upload": "background_upload",
        "night": "night_idle",
        "idle": "night_idle",
        "night mode": "night_idle",
        "fair": "fairness_guard",
        "fairness": "fairness_guard",
        "low latency": "low_latency_guard",
        "latency": "low_latency_guard",
        "uplink": "burst_uplink",
        "upload": "burst_uplink",
        "plain_text": "plain_text_guard",
        "plain-text": "plain_text_guard",
        "plain text": "plain_text_guard",
        "normal_text": "plain_text_guard",
        "normal-text": "plain_text_guard",
        "normal text": "plain_text_guard",
        "agent loop": "agentic_loop",
        "agentic": "agentic_loop",
        "anti-jitter": "anti_jitter_guard",
        "jitter_guard": "anti_jitter_guard",
        "jitter guard": "anti_jitter_guard",
        "jitter": "anti_jitter_guard",
    }
    return alias.get(m, mode)

def backend_call(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    更健壮的后端调用：
    - 连接/收包总超时由 BACKEND_TIMEOUT_S 控制
    - 收到任何数据就尝试解析 JSON（不强依赖 newline / 对端 close）
    """
    data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(BACKEND_TIMEOUT_S)

    try:
        s.connect((BACKEND_HOST, BACKEND_PORT))
        s.sendall(data)

        deadline = time.time() + BACKEND_TIMEOUT_S
        buf = b""

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError("timed out")

            r, _, _ = select.select([s], [], [], remaining)
            if not r:
                raise TimeoutError("timed out")

            chunk = s.recv(65535)
            if not chunk:
                break
            buf += chunk

            # 优先遇到换行就截断（后端常见是 \n 结尾）
            if b"\n" in buf:
                buf = buf.split(b"\n", 1)[0]
                break

            # 即使没换行，也尝试当完整 JSON 解析一次（避免对端 keep-alive 导致二次 recv 超时）
            try:
                _ = json.loads(buf.decode("utf-8", errors="replace").strip())
                break
            except Exception:
                pass

        text = buf.decode("utf-8", errors="replace").strip()
        return json.loads(text) if text else {"ok": False, "error": "empty backend response"}

    except Exception as e:
        return {
            "ok": False,
            "error": "backend_call failed",
            "detail": repr(e),
            "backend": f"{BACKEND_HOST}:{BACKEND_PORT}",
            "payload": payload,
        }
    finally:
        try: s.close()
        except: pass

# -------- MCP helpers --------
def mcp_result(idv, result):
    return {"jsonrpc": "2.0", "id": idv, "result": result}

def mcp_error(idv, message, data=None):
    e = {"jsonrpc": "2.0", "id": idv, "error": {"message": message}}
    if data is not None:
        e["error"]["data"] = data
    return e

# -------- local helpers (Jetson traffic burst for claim) --------
def _get_default_gateway() -> Optional[str]:
    """
    Return default gateway IP like "10.x.x.x" from `ip route show default`.
    """
    try:
        out = subprocess.check_output(["ip", "route", "show", "default"], text=True, stderr=subprocess.STDOUT)
        # example: "default via 10.0.0.1 dev rmnet_data0 ..."
        for line in out.splitlines():
            parts = line.strip().split()
            if len(parts) >= 3 and parts[0] == "default" and parts[1] == "via":
                return parts[2]
    except Exception:
        return None
    return None

def _udp_burst(dst_ip: str, dst_port: int = 9, duration_s: float = 1.0, payload_bytes: int = 1200, pps_limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Send UDP packets as fast as possible for duration_s.
    - dst_ip: default gateway is recommended.
    - pps_limit: if provided, approximate max packets per second.
    """
    payload_bytes = max(16, int(payload_bytes))
    duration_s = max(0.05, float(duration_s))
    dst_port = int(dst_port)

    payload = os.urandom(payload_bytes)
    sent = 0
    start = time.time()
    end = start + duration_s

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(True)

    # pps control
    min_interval = None
    if pps_limit and int(pps_limit) > 0:
        min_interval = 1.0 / float(int(pps_limit))

    next_send = time.time()

    try:
        while time.time() < end:
            if min_interval is not None:
                now = time.time()
                if now < next_send:
                    time.sleep(next_send - now)
                next_send = time.time() + min_interval

            sock.sendto(payload, (dst_ip, dst_port))
            sent += 1

        return {
            "ok": True,
            "method": "udp",
            "dst": f"{dst_ip}:{dst_port}",
            "duration_s": duration_s,
            "payload_bytes": payload_bytes,
            "packets_sent": sent,
            "bytes_sent": sent * payload_bytes,
        }
    except Exception as e:
        return {"ok": False, "error": "udp_burst failed", "detail": repr(e)}
    finally:
        try: sock.close()
        except: pass

def _snapshot_ues() -> Tuple[Dict[str, Any], Dict[int, int]]:
    """
    gNB-only snapshot:
    - raw response
    - map: rnti -> ul_event_count
    """
    raw = backend_call({
        "cmd": "ue_list",
        "active_only": True,
        "max_age_s": 15,
        "src": "gnb_rnti_watcher"
    })
    m: Dict[int, int] = {}
    if raw.get("ok"):
        for u in (raw.get("ues") or []):
            try:
                rnti = int(u.get("rnti"))
            except Exception:
                continue
            cnt = u.get("ul_event_count")
            if cnt is None:
                continue
            try:
                m[rnti] = int(cnt)
            except Exception:
                pass
    return raw, m

def _compute_deltas(before: Dict[int, int], after_raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Compare UL scheduler event counters.
    Each item: {rnti, delta_ul_events, ul_events_before, ul_events_after}
    """
    out: List[Dict[str, Any]] = []
    if not after_raw.get("ok"):
        return out

    by_rnti: Dict[int, Dict[str, Any]] = {}
    for u in (after_raw.get("ues") or []):
        try:
            rnti = int(u.get("rnti"))
        except Exception:
            continue
        by_rnti[rnti] = u

    for rnti, cnt_before in before.items():
        u = by_rnti.get(rnti) or {}
        cnt_after = u.get("ul_event_count")
        if cnt_after is None:
            continue
        try:
            cnt_after_i = int(cnt_after)
        except Exception:
            continue
        out.append({
            "rnti": rnti,
            "ul_events_before": int(cnt_before),
            "ul_events_after": cnt_after_i,
            "delta_ul_events": cnt_after_i - int(cnt_before),
        })

    out.sort(key=lambda x: x["delta_ul_events"], reverse=True)
    return out

def _tag_ue(rnti: int, role: str) -> Dict[str, Any]:
    return backend_call({"cmd": "tag_ue", "rnti": int(rnti), "role": str(role)})

def _active_rntis(raw_ue_list: Dict[str, Any]) -> List[int]:
    rntis = []
    if raw_ue_list.get("ok"):
        for u in (raw_ue_list.get("ues") or []):
            try:
                r = int(u.get("rnti"))
                if r > 0:
                    rntis.append(r)
            except Exception:
                pass
    return sorted(set(rntis))

def _clear_roles(raw_ue_list: Dict[str, Any], keep_roles: Optional[Dict[int, str]] = None) -> Dict[str, Any]:
    """
    将当前 ue_list 里的 UE 全部清成 unknown（排他式 claim 的关键）
    keep_roles: 可选 {rnti: role}
    """
    keep_roles = keep_roles or {}
    rntis = _active_rntis(raw_ue_list)
    cleared = []
    for r in rntis:
        _tag_ue(r, "unknown")
        cleared.append(r)

    restored = []
    for r, role in keep_roles.items():
        if r in rntis:
            _tag_ue(r, role)
            restored.append((r, role))

    return {"ok": True, "cleared": cleared, "restored": restored}

def _wait_for_counter_update(before_map: Dict[int, int],
                             settle_s: float = 1.2,
                             poll_retries: int = 6,
                             poll_interval_s: float = 0.4) -> Tuple[Dict[str, Any], Dict[int, int], List[Dict[str, Any]]]:
    """
    burst 之后等待/轮询，直到看到任意 UE 的 UL 调度事件计数发生变化
    返回: (after_raw, after_map, deltas)
    """
    time.sleep(max(0.0, float(settle_s)))

    last_raw, last_map = _snapshot_ues()
    last_deltas = _compute_deltas(before_map, last_raw) if last_raw.get("ok") else []

    for _ in range(max(1, int(poll_retries))):
        raw, mp = _snapshot_ues()
        deltas = _compute_deltas(before_map, raw) if raw.get("ok") else []

        if any(int(d.get("delta_ul_events", 0)) != 0 for d in deltas):
            return raw, mp, deltas

        last_raw, last_map, last_deltas = raw, mp, deltas
        time.sleep(max(0.05, float(poll_interval_s)))

    return last_raw, last_map, last_deltas

def _resolve_target_rnti(target: str, auto_claim: bool = True) -> Dict[str, Any]:
    target = (target or "").strip().lower()
    if target not in ("agent", "competitor"):
        return {"ok": False, "error": "target must be agent|competitor"}

    raw = backend_call({
        "cmd": "ue_list",
        "active_only": True,
        "max_age_s": 15,
        "src": "gnb_rnti_watcher"
    })
    if not raw.get("ok"):
        return {"ok": False, "error": "ue_list failed", "detail": raw}

    for u in (raw.get("ues") or []):
        if (u.get("role") or "").strip().lower() == target:
            return {"ok": True, "rnti": int(u["rnti"]), "ue": u, "ue_list": raw}

    if auto_claim and target in ("agent", "competitor"):
        claim = _claim_agent(exclusive=True)
        if not claim.get("ok"):
            return {"ok": False, "error": "auto claim_agent failed", "detail": claim}
        raw2 = backend_call({
            "cmd": "ue_list",
            "active_only": True,
            "max_age_s": 15,
            "src": "gnb_rnti_watcher"
        })
        if raw2.get("ok"):
            for u in (raw2.get("ues") or []):
                if (u.get("role") or "").strip().lower() == target:
                    return {"ok": True, "rnti": int(u["rnti"]), "ue": u, "ue_list": raw2, "claim": claim}
        return {"ok": False, "error": f"{target} not found after claim", "detail": {"claim": claim, "ue_list": raw2}}

    return {"ok": False, "error": f"role not found: {target}", "ue_list": raw}

def _claim_agent(duration_s: float = 1.0,
                 dst_ip: Optional[str] = None,
                 dst_port: int = 9,
                 payload_bytes: int = 1200,
                 pps_limit: Optional[int] = None,
                 min_delta: int = 50,
                 ratio: float = 1.2,
                 settle_s: float = 1.2,
                 poll_retries: int = 6,
                 poll_interval_s: float = 0.4,
                 exclusive: bool = True) -> Dict[str, Any]:
    """
    Traffic-based claim (robust + exclusive)
    """
    before_raw, before_map = _snapshot_ues()
    if not before_raw.get("ok"):
        return {"ok": False, "error": "ue_list before failed", "detail": before_raw}
    if len(before_map) == 0:
        return {"ok": False, "error": "no usable ul_event_count in ue_list (before)", "detail": before_raw}

    if dst_ip is None:
        dst_ip = _get_default_gateway()
    if not dst_ip:
        return {"ok": False, "error": "no dst_ip and cannot find default gateway (ip route default)"}

    burst = _udp_burst(dst_ip=dst_ip, dst_port=dst_port, duration_s=duration_s,
                       payload_bytes=payload_bytes, pps_limit=pps_limit)

    after_raw, _after_map, deltas = _wait_for_counter_update(
        before_map=before_map,
        settle_s=settle_s,
        poll_retries=poll_retries,
        poll_interval_s=poll_interval_s
    )

    if not after_raw.get("ok"):
        return {"ok": False, "error": "ue_list after failed", "detail": after_raw, "burst": burst}

    if len(deltas) == 0:
        return {"ok": False, "error": "no deltas computed (after)", "before": before_raw, "after": after_raw, "burst": burst}

    for d in deltas:
        if int(d.get("delta_ul_events", 0)) < 0:
            d["delta_ul_events"] = 0

    winner = deltas[0]
    runner = deltas[1] if len(deltas) > 1 else {"delta_ul_events": 0}
    winner_delta = int(winner.get("delta_ul_events", 0))
    runner_delta = int(runner.get("delta_ul_events", 0))

    ambiguous = False
    if len(deltas) > 1:
        if winner_delta < max(int(min_delta), int(runner_delta * float(ratio))):
            ambiguous = True

    winner_rnti = int(winner["rnti"])
    active_rntis = _active_rntis(after_raw)

    cleared_info = None
    if exclusive:
        cleared_info = _clear_roles(after_raw)

    _tag_ue(winner_rnti, "agent")

    competitor_rnti = None
    if len(active_rntis) == 2:
        competitor_rnti = active_rntis[0] if active_rntis[1] == winner_rnti else active_rntis[1]
        _tag_ue(competitor_rnti, "competitor")

    final_list = backend_call({
        "cmd": "ue_list",
        "active_only": True,
        "max_age_s": 15,
        "src": "gnb_rnti_watcher"
    })

    return {
        "ok": True,
        "ambiguous": ambiguous,
        "agent_rnti": winner_rnti,
        "agent_rnti_hex": f"0x{winner_rnti:x}",
        "competitor_rnti": competitor_rnti,
        "burst": burst,
        "deltas": deltas,
        "ue_list_before": before_raw,
        "ue_list_after": after_raw,
        "roles_cleared": cleared_info,
        "ue_list_final": final_list,
        "note": (
            "claim is based on gNB watcher UL scheduler event deltas (ul_order/ul_alloc), "
            "not monitor_xapp ul_bytes"
        ),
    }

# -------- local speedtest helpers --------
def _extract_iperf_end_rates(obj: Dict[str, Any]) -> Dict[str, Optional[float]]:
    result = {"receiver_mbps": None, "sender_mbps": None}
    try:
        end = obj.get("end") or {}
        for key in ("sum_received", "sum"):
            part = end.get(key)
            if isinstance(part, dict) and part.get("bits_per_second") is not None and result["receiver_mbps"] is None:
                result["receiver_mbps"] = float(part["bits_per_second"]) / 1e6
        for key in ("sum_sent", "sum"):
            part = end.get(key)
            if isinstance(part, dict) and part.get("bits_per_second") is not None and result["sender_mbps"] is None:
                result["sender_mbps"] = float(part["bits_per_second"]) / 1e6
        streams = end.get("streams") or []
        recv_vals, send_vals = [], []
        for s in streams:
            rec = s.get("receiver") or {}
            sen = s.get("sender") or {}
            if rec.get("bits_per_second") is not None:
                recv_vals.append(float(rec["bits_per_second"]) / 1e6)
            if sen.get("bits_per_second") is not None:
                send_vals.append(float(sen["bits_per_second"]) / 1e6)
        if result["receiver_mbps"] is None and recv_vals:
            result["receiver_mbps"] = sum(recv_vals)
        if result["sender_mbps"] is None and send_vals:
            result["sender_mbps"] = sum(send_vals)
    except Exception:
        pass
    return result


def _extract_iperf_rate_from_text(text: str, prefer_sender: bool = False) -> Optional[float]:
    if not text:
        return None
    lines = text.splitlines()
    target = [ln for ln in lines if ("sender" in ln if prefer_sender else "receiver" in ln)]
    sum_lines = [ln for ln in target if "[SUM]" in ln]
    target_lines = sum_lines if sum_lines else target
    pattern = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s+([KMG])bits/sec")
    for ln in reversed(target_lines):
        m = pattern.search(ln)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            if unit == "K":
                return val / 1000.0
            if unit == "M":
                return val
            if unit == "G":
                return val * 1000.0
    return None


def _quick_speedtest(direction: str = "dl",
                     host: Optional[str] = None,
                     port: Optional[int] = None,
                     duration_s: Optional[int] = None,
                     parallel: Optional[int] = None) -> Dict[str, Any]:
    direction = str(direction or "dl").lower()
    if direction not in ("dl", "ul"):
        direction = "dl"
    host = host or SPEEDTEST_HOST
    port = int(port or SPEEDTEST_PORT)
    duration_s = int(duration_s or SPEEDTEST_DURATION_S)
    parallel = int(parallel or SPEEDTEST_PARALLEL)

    base_cmd = ["iperf3", "-c", str(host), "-p", str(port), "-P", str(parallel), "-t", str(duration_s)]
    if direction == "dl":
        base_cmd.append("-R")
    prefer_sender = (direction == "ul")

    try:
        cmd = base_cmd + ["-J"]
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=duration_s + 8)
        out = p.stdout or ""
        err = p.stderr or ""
        if p.returncode == 0:
            try:
                obj = json.loads(out)
                rates = _extract_iperf_end_rates(obj)
                chosen = rates.get("sender_mbps") if prefer_sender else rates.get("receiver_mbps")
                if chosen is None:
                    chosen = rates.get("receiver_mbps") if prefer_sender else rates.get("sender_mbps")
                if chosen is not None:
                    return {
                        "ok": True,
                        "direction": direction,
                        "mbps": round(chosen, 2),
                        "receiver_mbps": None if rates.get("receiver_mbps") is None else round(rates.get("receiver_mbps"), 2),
                        "sender_mbps": None if rates.get("sender_mbps") is None else round(rates.get("sender_mbps"), 2),
                        "host": host,
                        "port": port,
                        "duration_s": duration_s,
                        "parallel": parallel,
                        "parser": "json",
                    }
            except Exception:
                pass

        merged = (out + "\n" + err).strip()
        mbps = _extract_iperf_rate_from_text(merged, prefer_sender=prefer_sender)
        if mbps is not None:
            return {
                "ok": True,
                "direction": direction,
                "mbps": round(mbps, 2),
                "host": host,
                "port": port,
                "duration_s": duration_s,
                "parallel": parallel,
                "parser": "text-fallback",
                "returncode": p.returncode,
            }
        return {
            "ok": False,
            "direction": direction,
            "error": "iperf3 parse failed",
            "host": host,
            "port": port,
            "duration_s": duration_s,
            "parallel": parallel,
            "returncode": p.returncode,
            "stdout": out[-1000:],
            "stderr": err[-1000:],
        }
    except FileNotFoundError:
        return {"ok": False, "direction": direction, "error": "iperf3 not found on agent device", "hint": "please install iperf3 on the Jetson/agent side"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "direction": direction, "error": "iperf3 timeout", "host": host, "port": port, "duration_s": duration_s, "parallel": parallel}
    except Exception as e:
        return {"ok": False, "direction": direction, "error": "iperf3 failed", "detail": repr(e), "host": host, "port": port}


def _quick_speedtest_pair(duration_s: Optional[int] = None) -> Dict[str, Any]:
    return {"dl": _quick_speedtest(direction="dl", duration_s=duration_s), "ul": _quick_speedtest(direction="ul", duration_s=duration_s)}

def _pct(values: List[float], q: float) -> Optional[float]:
    vals = sorted(float(v) for v in values if v is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    q = max(0.0, min(1.0, float(q)))
    pos = (len(vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def _series_stats(values: List[float], unit: str = "") -> Dict[str, Any]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return {"count": 0, "unit": unit}
    out = {
        "count": len(vals),
        "min": round(min(vals), 2),
        "max": round(max(vals), 2),
        "mean": round(sum(vals) / len(vals), 2),
        "p50": round(_pct(vals, 0.50), 2),
        "p95": round(_pct(vals, 0.95), 2),
        "unit": unit,
    }
    if len(vals) >= 2:
        try:
            out["stdev"] = round(statistics.stdev(vals), 2)
        except Exception:
            pass
    return out


def _run_iperf_once(direction: str = "ul",
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    duration_s: Optional[int] = None,
                    parallel: int = 1,
                    bytes_to_send: Optional[int] = None,
                    timeout_cap_s: Optional[float] = None) -> Dict[str, Any]:
    direction = str(direction or "ul").lower()
    host = host or SPEEDTEST_HOST
    port = int(port or SPEEDTEST_PORT)
    parallel = max(1, int(parallel))

    cmd = ["iperf3", "-c", str(host), "-p", str(port), "-P", str(parallel), "-J"]
    if direction == "dl":
        cmd.append("-R")
    if bytes_to_send is not None:
        cmd += ["-n", str(int(bytes_to_send))]
        timeout_s = 15
    else:
        duration_s = int(duration_s or EFFECT_QUICK_UL_SECONDS)
        cmd += ["-t", str(duration_s)]
        timeout_s = duration_s + 10

    if timeout_cap_s is not None:
        try:
            timeout_s = min(float(timeout_s), max(2.0, float(timeout_cap_s)))
        except Exception:
            pass

    started = time.perf_counter()
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout_s)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        out = p.stdout or ""
        err = p.stderr or ""

        if p.returncode != 0:
            return {
                "ok": False,
                "error": "iperf3 failed",
                "returncode": p.returncode,
                "stdout": out[-800:],
                "stderr": err[-800:],
                "elapsed_ms": round(elapsed_ms, 2),
                "cmd": cmd,
            }

        obj = json.loads(out)
        rates = _extract_iperf_end_rates(obj)
        end = obj.get("end") or {}

        retransmits = None
        try:
            sum_sent = end.get("sum_sent") or {}
            if sum_sent.get("retransmits") is not None:
                retransmits = int(sum_sent["retransmits"])
        except Exception:
            pass

        mbps = rates.get("sender_mbps") if direction == "ul" else rates.get("receiver_mbps")
        if mbps is None:
            mbps = rates.get("receiver_mbps") if direction == "ul" else rates.get("sender_mbps")

        return {
            "ok": True,
            "direction": direction,
            "mbps": None if mbps is None else round(float(mbps), 2),
            "sender_mbps": None if rates.get("sender_mbps") is None else round(float(rates["sender_mbps"]), 2),
            "receiver_mbps": None if rates.get("receiver_mbps") is None else round(float(rates["receiver_mbps"]), 2),
            "retransmits": retransmits,
            "elapsed_ms": round(elapsed_ms, 2),
            "parallel": parallel,
            "bytes_to_send": bytes_to_send,
            "duration_s": duration_s if bytes_to_send is None else None,
            "host": host,
            "port": port,
        }
    except FileNotFoundError:
        return {"ok": False, "error": "iperf3 not found", "cmd": cmd}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "iperf3 timeout", "cmd": cmd}
    except Exception as e:
        return {"ok": False, "error": "iperf3 exception", "detail": repr(e), "cmd": cmd}



def _parse_ping_output(text: str) -> Dict[str, Any]:
    times: List[float] = []
    for line in (text or "").splitlines():
        m = re.search(r"time[=<]([0-9]+(?:\.[0-9]+)?)\s*ms", line)
        if m:
            times.append(float(m.group(1)))

    stats = _series_stats(times, unit="ms")
    if times:
        stats["ok"] = True
    else:
        stats["ok"] = False

    m2 = re.search(r"=\s*([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+)\s*ms", text or "")
    if m2:
        stats["min_summary"] = float(m2.group(1))
        stats["avg_summary"] = float(m2.group(2))
        stats["max_summary"] = float(m2.group(3))
        stats["mdev_summary"] = float(m2.group(4))
    return stats


def _run_ping_probe(host: Optional[str] = None,
                    count: int = 8,
                    interval_s: float = 0.2) -> Dict[str, Any]:
    host = host or EFFECT_PING_HOST
    count = max(2, int(count))
    interval_s = max(0.2, float(interval_s))
    cmd = ["ping", "-c", str(count), "-i", str(interval_s), str(host)]
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=count * interval_s + 8)
        text = (p.stdout or "") + "\n" + (p.stderr or "")
        stats = _parse_ping_output(text)
        stats["host"] = host
        stats["count_requested"] = count
        stats["interval_s"] = interval_s
        stats["returncode"] = p.returncode
        return stats
    except Exception as e:
        return {"ok": False, "error": "ping failed", "detail": repr(e), "host": host}


def _run_ping_under_load(direction: str = "ul",
                         host: Optional[str] = None,
                         ping_count: int = 10,
                         interval_s: float = 0.2,
                         load_seconds: int = 2,
                         load_parallel: int = 2) -> Dict[str, Any]:
    direction = str(direction or "ul").lower()
    if direction not in ("ul", "dl"):
        direction = "ul"
    host = host or EFFECT_PING_HOST
    ping_cmd = ["ping", "-c", str(max(3, int(ping_count))), "-i", str(max(0.2, float(interval_s))), str(host)]
    try:
        ping_p = subprocess.Popen(ping_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.2)
        load = _run_iperf_once(direction=direction, duration_s=load_seconds, parallel=load_parallel)
        try:
            out, err = ping_p.communicate(timeout=load_seconds + 10)
        except subprocess.TimeoutExpired:
            ping_p.kill()
            out, err = ping_p.communicate()
        ping_stats = _parse_ping_output((out or "") + "\n" + (err or ""))
        return {
            "ok": bool(load.get("ok")) or bool(ping_stats.get("ok")),
            "host": host,
            "direction": direction,
            f"{direction}_load": load,
            "ping": ping_stats,
        }
    except Exception as e:
        return {"ok": False, "error": "ping_under_load failed", "detail": repr(e), "host": host, "direction": direction}


def _run_ping_under_ul_load(host: Optional[str] = None,
                            ping_count: int = 10,
                            interval_s: float = 0.2,
                            load_seconds: int = 2,
                            load_parallel: int = 2) -> Dict[str, Any]:
    return _run_ping_under_load(
        direction="ul",
        host=host,
        ping_count=ping_count,
        interval_s=interval_s,
        load_seconds=load_seconds,
        load_parallel=load_parallel,
    )


def _run_burst_probe(direction: str = "ul",
                     host: Optional[str] = None,
                     port: Optional[int] = None,
                     bytes_to_send: int = EFFECT_QUICK_BURST_BYTES,
                     repeat: int = 3,
                     deadline_ts: Optional[float] = None) -> Dict[str, Any]:
    direction = str(direction or "ul").lower()
    runs = []
    elapsed_vals: List[float] = []
    mbps_vals: List[float] = []
    retrans_vals: List[float] = []
    truncated = False

    for _ in range(max(1, int(repeat))):
        if _should_stop_for_budget(deadline_ts, reserve_s=1.5):
            truncated = True
            break
        one = _run_iperf_once(
            direction=direction,
            host=host,
            port=port,
            bytes_to_send=bytes_to_send,
            parallel=1,
            timeout_cap_s=_timeout_cap_from_deadline(deadline_ts),
        )
        runs.append(one)
        if one.get("ok"):
            if one.get("elapsed_ms") is not None:
                elapsed_vals.append(float(one["elapsed_ms"]))
            if one.get("mbps") is not None:
                mbps_vals.append(float(one["mbps"]))
            if one.get("retransmits") is not None:
                retrans_vals.append(float(one["retransmits"]))

    return {
        "ok": any(bool(r.get("ok")) for r in runs),
        "direction": direction,
        "bytes_to_send": int(bytes_to_send),
        "repeat": int(repeat),
        "truncated": truncated,
        "elapsed_ms_stats": _series_stats(elapsed_vals, unit="ms"),
        "mbps_stats": _series_stats(mbps_vals, unit="Mbps"),
        "retransmits_stats": _series_stats(retrans_vals, unit="count"),
        "runs": runs,
    }



def _run_idle_resume_probe(direction: str = "ul",
                           host: Optional[str] = None,
                           port: Optional[int] = None,
                           bytes_to_send: int = EFFECT_QUICK_BURST_BYTES,
                           idle_s: float = EFFECT_IDLE_SLEEP_S,
                           repeat: int = 3,
                           deadline_ts: Optional[float] = None) -> Dict[str, Any]:
    direction = str(direction or "ul").lower()
    first_elapsed: List[float] = []
    second_elapsed: List[float] = []
    runs = []
    truncated = False

    for _ in range(max(1, int(repeat))):
        if _should_stop_for_budget(deadline_ts, reserve_s=float(idle_s) + 2.0):
            truncated = True
            break
        first = _run_iperf_once(
            direction=direction,
            host=host,
            port=port,
            bytes_to_send=bytes_to_send,
            parallel=1,
            timeout_cap_s=_timeout_cap_from_deadline(deadline_ts),
        )
        if _should_stop_for_budget(deadline_ts, reserve_s=float(idle_s) + 1.0):
            truncated = True
            runs.append({"first": first, "second": {"ok": False, "error": "stage budget exhausted before second burst"}})
            break
        time.sleep(max(0.2, float(idle_s)))
        second = _run_iperf_once(
            direction=direction,
            host=host,
            port=port,
            bytes_to_send=bytes_to_send,
            parallel=1,
            timeout_cap_s=_timeout_cap_from_deadline(deadline_ts),
        )
        runs.append({"first": first, "second": second})
        if first.get("ok") and first.get("elapsed_ms") is not None:
            first_elapsed.append(float(first["elapsed_ms"]))
        if second.get("ok") and second.get("elapsed_ms") is not None:
            second_elapsed.append(float(second["elapsed_ms"]))

    return {
        "ok": any(bool((r.get("second") or {}).get("ok")) for r in runs),
        "direction": direction,
        "bytes_to_send": int(bytes_to_send),
        "idle_s": float(idle_s),
        "repeat": int(repeat),
        "truncated": truncated,
        "first_elapsed_ms_stats": _series_stats(first_elapsed, unit="ms"),
        "second_elapsed_ms_stats": _series_stats(second_elapsed, unit="ms"),
        "runs": runs,
    }



def _snapshot_runtime_ue(rnti: Optional[int]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    raw = backend_call({
        "cmd": "ue_list",
        "active_only": True,
        "max_age_s": 15,
        "src": "gnb_rnti_watcher",
    })
    ue = _find_ue_by_rnti(raw, rnti)
    return raw, ue


def _format_posture_param_value(v: Any) -> str:
    return "默认" if v is None else str(v)


def _normalize_posture_entry(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(entry, dict):
        return out
    for k in FULL_POSTURE_PARAM_KEYS:
        if entry.get(k) is not None:
            out[k] = entry.get(k)
    return out


def _normalize_ul_posture_entry(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(entry, dict):
        return out
    for k in UL_POSTURE_PARAM_KEYS:
        if entry.get(k) is not None:
            out[k] = entry.get(k)
    return out


def _guess_posture_name(entry: Optional[Dict[str, Any]]) -> str:
    norm = _normalize_posture_entry(entry)
    if not norm:
        return "posture_default"
    for name, preset in FULL_POSTURE_PRESETS.items():
        if preset.get("clear"):
            continue
        keys = [k for k in FULL_POSTURE_PARAM_KEYS if k in preset]
        if keys and all(norm.get(k) == preset.get(k) for k in keys):
            return name
    return "custom"


def _guess_ul_posture_name(entry: Optional[Dict[str, Any]]) -> str:
    norm = _normalize_ul_posture_entry(entry)
    if not norm:
        return "ul_default"
    for name, preset in UL_POSTURE_PRESETS.items():
        if preset.get("clear"):
            continue
        keys = [k for k in UL_POSTURE_PARAM_KEYS if k in preset]
        if keys and all(norm.get(k) == preset.get(k) for k in keys):
            return name
    return "custom"


def _posture_group_lines(before_entry: Dict[str, Any], after_entry: Dict[str, Any], include_effects: bool = True) -> List[str]:
    lines: List[str] = []
    for group_key in ("ul_link", "ul_sched", "dl_link", "dl_sched"):
        keys = POSTURE_PARAM_GROUPS[group_key]
        lines.append(f"- {POSTURE_GROUP_LABELS[group_key]}:")
        for k in keys:
            b = before_entry.get(k)
            a = after_entry.get(k)
            effect = POSTURE_PARAM_EFFECTS.get(k, "") if include_effects else ""
            suffix = f"；{effect}" if effect else ""
            lines.append(f"  - {POSTURE_PARAM_LABELS[k]}: {_format_posture_param_value(b)} -> {_format_posture_param_value(a)}{suffix}")
    return lines


def _runtime_posture_snapshot(ue: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(ue, dict):
        return []
    lines = ["- 最近 gNB 观测:"]
    lines.append(
        f"  - UL rbSize/current_rbs = {ue.get('ul_rbSize')}/{ue.get('ul_current_rbs')}，DL rbSize/current_rbs = {ue.get('dl_rbSize')}/{ue.get('dl_current_rbs')}"
    )
    lines.append(
        f"  - UL tpc0={ue.get('tpc0')}，pusch_snrx10={ue.get('pusch_snrx10')}"
    )
    lines.append(
        f"  - UL throttled_count={ue.get('ul_throttled_count', 0)}，DL throttled_count={ue.get('dl_throttled_count', 0)}"
    )
    lines.append(
        f"  - UL grants={ue.get('ul_grant_count', 0)}，DL grants={ue.get('dl_grant_count', 0)}"
    )
    return lines


def _summarize_run_metric(label: str, obj: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    if not isinstance(obj, dict):
        return lines
    if obj.get("ok") and obj.get("mbps") is not None:
        tail = []
        if obj.get("retransmits") is not None:
            tail.append(f"retransmits={obj.get('retransmits')}")
        if obj.get("elapsed_ms") is not None:
            tail.append(f"耗时={obj.get('elapsed_ms')} ms")
        suffix = "，" + "，".join(tail) if tail else ""
        lines.append(f"- {label}: {obj.get('mbps')} Mbps{suffix}")
        return lines
    stats = obj.get("mbps_stats") or {}
    if stats.get("count"):
        rt = obj.get("retransmits_stats") or {}
        rtxt = f"，retransmits mean={rt.get('mean')}" if rt.get("count") else ""
        lines.append(f"- {label}: p50={stats.get('p50')} Mbps, p95={stats.get('p95')} Mbps, mean={stats.get('mean')} Mbps{rtxt}")
    return lines


def _summarize_burst_metric(label: str, obj: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    if not isinstance(obj, dict):
        return lines
    s = obj.get("elapsed_ms_stats") or {}
    if s.get("count"):
        kb = int((obj.get("bytes_to_send") or 0) / 1024)
        lines.append(f"- {label}（{kb}KB）完成时间: p50={s.get('p50')} ms, p95={s.get('p95')} ms, mean={s.get('mean')} ms")
    return lines


def _summarize_idle_metric(label: str, obj: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    if not isinstance(obj, dict):
        return lines
    s = obj.get("second_elapsed_ms_stats") or {}
    if s.get("count"):
        lines.append(f"- {label}: p50={s.get('p50')} ms, p95={s.get('p95')} ms, mean={s.get('mean')} ms")
    return lines


def _summarize_ping_load(label: str, obj: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    if not isinstance(obj, dict):
        return lines
    ping_stats = obj.get("ping") or {}
    if ping_stats.get("count"):
        jitter = ping_stats.get("stdev", ping_stats.get("mdev_summary"))
        lines.append(f"- {label}: p50={ping_stats.get('p50')} ms, p95={ping_stats.get('p95')} ms, mean={ping_stats.get('mean')} ms, jitter={jitter} ms")
    return lines


def build_effect_measure_summary(pack: str,
                                 target_rnti: Optional[int],
                                 posture_name: str,
                                 posture_resp: Dict[str, Any],
                                 latest_ue: Optional[Dict[str, Any]],
                                 measure: Dict[str, Any]) -> str:
    preset = FULL_POSTURE_PRESETS.get(posture_name) or UL_POSTURE_PRESETS.get(posture_name) or {}
    pack_cn = PACK_LABELS.get(pack, pack)
    entry = _normalize_posture_entry((posture_resp or {}).get("entry"))

    lines: List[str] = []
    lines.append(f"已完成 **{pack_cn}** 实际测量。")
    if target_rnti is not None:
        lines.append(f"- 目标 UE: RNTI={target_rnti} (0x{int(target_rnti):x})")
    lines.append(f"- 当前通信姿态: {preset.get('label', posture_name)}")
    if preset.get("human_summary"):
        lines.append(f"- 场景含义: {preset.get('human_summary')}")
    if preset.get("effect_summary"):
        lines.append(f"- 总体影响: {preset.get('effect_summary')}")

    if entry:
        lines.append("- 当前参数分组:")
        lines.extend(_posture_group_lines(entry, entry, include_effects=False))
    else:
        lines.append("- 当前参数: 默认（未显式 override）")

    if measure.get("stage_truncated"):
        lines.append("- 注意: 为避免 MCP 60 秒超时，本阶段在预算内提前结束；当前结果仍可用于趋势判断。")

    lines.extend(_summarize_run_metric("持续上行 goodput", measure.get("sustained_ul") or {}))
    lines.extend(_summarize_run_metric("持续下行 goodput", measure.get("sustained_dl") or {}))
    lines.extend(_summarize_burst_metric("上行小 burst", measure.get("burst_ul_small") or {}))
    lines.extend(_summarize_burst_metric("下行小 burst", measure.get("burst_dl_small") or {}))
    lines.extend(_summarize_burst_metric("上行较大 burst", measure.get("burst_ul_large") or {}))
    lines.extend(_summarize_burst_metric("下行较大 burst", measure.get("burst_dl_large") or {}))
    lines.extend(_summarize_idle_metric("上行空闲后恢复发送", measure.get("idle_resume_ul") or {}))
    lines.extend(_summarize_idle_metric("下行空闲后恢复发送", measure.get("idle_resume_dl") or {}))
    lines.extend(_summarize_ping_load("上行负载下 RTT", measure.get("ping_under_ul_load") or {}))
    lines.extend(_summarize_ping_load("下行负载下 RTT", measure.get("ping_under_dl_load") or {}))
    lines.extend(_runtime_posture_snapshot(latest_ue))

    lines.append("- 指标怎么看:")
    lines.append("  - 持续 UL/DL goodput 与 retransmits 更能体现 ul_max_mcs / dl_max_mcs 与目标接收质量带来的吞吐-稳定性取舍。")
    lines.append("  - 小 burst 完成时间更能体现 min_grant_prb、dl_min_grant_prb 以及 small_burst_bytes/small_burst_mul 对首包和小结果回传的影响。")
    lines.append("  - 空闲后恢复发送更能体现 ulsch_max_frame_inactivity，以及 UL/DL maxcg override 对断续业务的影响。")
    lines.append("  - 负载下 RTT/jitter 更能体现 ul_sched_mul、dl_sched_mul 和连续 grants 上限对交互体验的副作用。")

    if pack == "quick":
        lines.append("")
        lines.append("如果你想知道更准确的效果，我可以继续进行 **full pack 分阶段测量**；为了避免超时，我会依次执行持续吞吐、burst、空闲恢复和负载下 RTT 四个阶段。")

    return "\n".join(lines)



def build_ul_effect_measure_summary(pack: str,
                                    target_rnti: Optional[int],
                                    posture_name: str,
                                    posture_resp: Dict[str, Any],
                                    latest_ue: Optional[Dict[str, Any]],
                                    measure: Dict[str, Any]) -> str:
    return build_effect_measure_summary(pack, target_rnti, posture_name, posture_resp, latest_ue, measure)


def _measure_ul_effect(target_rnti: Optional[int], pack: str = "quick") -> Dict[str, Any]:
    pack = str(pack or "quick").lower()
    if pack == "full":
        return {
            "ok": False,
            "error": "full pack is now staged",
            "summary_zh": "full pack 现在改成分阶段执行，以避免 MCP 60 秒超时。请依次调用 ric.measure_ul_effect(pack=full_sustained)、full_burst、full_resume、full_rtt，然后再综合四个阶段结果。",
            "suggested_packs": list(STAGED_FULL_PACKS),
        }
    if pack not in (("quick",) + STAGED_FULL_PACKS):
        return {"ok": False, "error": "pack must be quick|full_sustained|full_burst|full_resume|full_rtt"}

    posture_resp = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
    posture_name = _guess_posture_name((posture_resp or {}).get("entry") if posture_resp.get("ok") else {})

    measure: Dict[str, Any] = {}
    stage_deadline = None if pack == "quick" else (time.time() + EFFECT_STAGE_BUDGET_S)
    stage_truncated = False

    if pack == "quick":
        sustained_ul = _run_iperf_once(direction="ul", duration_s=EFFECT_QUICK_UL_SECONDS, parallel=EFFECT_QUICK_UL_PARALLEL)
        sustained_dl = _run_iperf_once(direction="dl", duration_s=EFFECT_QUICK_UL_SECONDS, parallel=EFFECT_QUICK_UL_PARALLEL)
        burst_ul_small = _run_burst_probe(direction="ul", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_QUICK_BURST_BYTES, repeat=EFFECT_QUICK_BURST_REPEAT)
        burst_dl_small = _run_burst_probe(direction="dl", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_QUICK_BURST_BYTES, repeat=EFFECT_QUICK_BURST_REPEAT)
        idle_resume_ul = _run_idle_resume_probe(direction="ul", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_QUICK_BURST_BYTES, idle_s=EFFECT_IDLE_SLEEP_S, repeat=EFFECT_QUICK_IDLE_RESUME_REPEAT)
        idle_resume_dl = _run_idle_resume_probe(direction="dl", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_QUICK_BURST_BYTES, idle_s=EFFECT_IDLE_SLEEP_S, repeat=EFFECT_QUICK_IDLE_RESUME_REPEAT)
        ping_under_ul_load = _run_ping_under_load(direction="ul", host=EFFECT_PING_HOST, ping_count=10, interval_s=EFFECT_PING_INTERVAL_S, load_seconds=max(2, EFFECT_QUICK_UL_SECONDS), load_parallel=EFFECT_QUICK_UL_PARALLEL)
        ping_under_dl_load = _run_ping_under_load(direction="dl", host=EFFECT_PING_HOST, ping_count=10, interval_s=EFFECT_PING_INTERVAL_S, load_seconds=max(2, EFFECT_QUICK_UL_SECONDS), load_parallel=EFFECT_QUICK_UL_PARALLEL)
        burst_ul_large = {}
        burst_dl_large = {}
    elif pack == "full_sustained":
        sustained_ul_runs = []
        sustained_dl_runs = []
        for _ in range(3):
            if _should_stop_for_budget(stage_deadline, reserve_s=4.0):
                stage_truncated = True
                break
            sustained_ul_runs.append(_run_iperf_once(direction="ul", duration_s=EFFECT_FULL_UL_SECONDS, parallel=EFFECT_FULL_UL_PARALLEL, timeout_cap_s=_timeout_cap_from_deadline(stage_deadline)))
            if _should_stop_for_budget(stage_deadline, reserve_s=2.0):
                stage_truncated = True
                break
            sustained_dl_runs.append(_run_iperf_once(direction="dl", duration_s=EFFECT_FULL_UL_SECONDS, parallel=EFFECT_FULL_UL_PARALLEL, timeout_cap_s=_timeout_cap_from_deadline(stage_deadline)))
        sustained_ul_vals = [float(x["mbps"]) for x in sustained_ul_runs if x.get("ok") and x.get("mbps") is not None]
        sustained_dl_vals = [float(x["mbps"]) for x in sustained_dl_runs if x.get("ok") and x.get("mbps") is not None]
        ul_retx = [float(x["retransmits"]) for x in sustained_ul_runs if x.get("ok") and x.get("retransmits") is not None]
        dl_retx = [float(x["retransmits"]) for x in sustained_dl_runs if x.get("ok") and x.get("retransmits") is not None]
        sustained_ul = {"ok": any(bool(x.get("ok")) for x in sustained_ul_runs), "runs": sustained_ul_runs, "mbps_stats": _series_stats(sustained_ul_vals, unit="Mbps"), "retransmits_stats": _series_stats(ul_retx, unit="count")}
        sustained_dl = {"ok": any(bool(x.get("ok")) for x in sustained_dl_runs), "runs": sustained_dl_runs, "mbps_stats": _series_stats(sustained_dl_vals, unit="Mbps"), "retransmits_stats": _series_stats(dl_retx, unit="count")}
        burst_ul_small = {}
        burst_dl_small = {}
        burst_ul_large = {}
        burst_dl_large = {}
        idle_resume_ul = {}
        idle_resume_dl = {}
        ping_under_ul_load = {}
        ping_under_dl_load = {}
    elif pack == "full_burst":
        sustained_ul = {}
        sustained_dl = {}
        burst_ul_small = _run_burst_probe(direction="ul", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_FULL_BURST_SMALL_BYTES, repeat=EFFECT_FULL_BURST_REPEAT, deadline_ts=stage_deadline)
        burst_dl_small = _run_burst_probe(direction="dl", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_FULL_BURST_SMALL_BYTES, repeat=EFFECT_FULL_BURST_REPEAT, deadline_ts=stage_deadline)
        burst_ul_large = _run_burst_probe(direction="ul", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_FULL_BURST_LARGE_BYTES, repeat=EFFECT_FULL_BURST_REPEAT, deadline_ts=stage_deadline)
        burst_dl_large = _run_burst_probe(direction="dl", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_FULL_BURST_LARGE_BYTES, repeat=EFFECT_FULL_BURST_REPEAT, deadline_ts=stage_deadline)
        idle_resume_ul = {}
        idle_resume_dl = {}
        ping_under_ul_load = {}
        ping_under_dl_load = {}
        stage_truncated = any(bool((x or {}).get("truncated")) for x in (burst_ul_small, burst_dl_small, burst_ul_large, burst_dl_large))
    elif pack == "full_resume":
        sustained_ul = {}
        sustained_dl = {}
        burst_ul_small = {}
        burst_dl_small = {}
        burst_ul_large = {}
        burst_dl_large = {}
        idle_resume_ul = _run_idle_resume_probe(direction="ul", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_FULL_BURST_SMALL_BYTES, idle_s=EFFECT_IDLE_SLEEP_S, repeat=EFFECT_FULL_IDLE_RESUME_REPEAT, deadline_ts=stage_deadline)
        idle_resume_dl = _run_idle_resume_probe(direction="dl", host=SPEEDTEST_HOST, port=SPEEDTEST_PORT, bytes_to_send=EFFECT_FULL_BURST_SMALL_BYTES, idle_s=EFFECT_IDLE_SLEEP_S, repeat=EFFECT_FULL_IDLE_RESUME_REPEAT, deadline_ts=stage_deadline)
        ping_under_ul_load = {}
        ping_under_dl_load = {}
        stage_truncated = any(bool((x or {}).get("truncated")) for x in (idle_resume_ul, idle_resume_dl))
    elif pack == "full_rtt":
        sustained_ul = {}
        sustained_dl = {}
        burst_ul_small = {}
        burst_dl_small = {}
        burst_ul_large = {}
        burst_dl_large = {}
        idle_resume_ul = {}
        idle_resume_dl = {}
        ping_under_ul_load = _run_ping_under_load(direction="ul", host=EFFECT_PING_HOST, ping_count=20, interval_s=EFFECT_PING_INTERVAL_S, load_seconds=max(3, EFFECT_FULL_UL_SECONDS), load_parallel=EFFECT_FULL_UL_PARALLEL)
        ping_under_dl_load = _run_ping_under_load(direction="dl", host=EFFECT_PING_HOST, ping_count=20, interval_s=EFFECT_PING_INTERVAL_S, load_seconds=max(3, EFFECT_FULL_UL_SECONDS), load_parallel=EFFECT_FULL_UL_PARALLEL)

    _raw, latest_ue = _snapshot_runtime_ue(target_rnti)

    measure = {
        "sustained_ul": sustained_ul,
        "sustained_dl": sustained_dl,
        "burst_ul_small": burst_ul_small,
        "burst_dl_small": burst_dl_small,
        "burst_ul_large": burst_ul_large,
        "burst_dl_large": burst_dl_large,
        "idle_resume_ul": idle_resume_ul,
        "idle_resume_dl": idle_resume_dl,
        "ping_under_ul_load": ping_under_ul_load,
        "ping_under_dl_load": ping_under_dl_load,
        "stage_truncated": stage_truncated,
    }

    summary = build_effect_measure_summary(
        pack=pack,
        target_rnti=target_rnti,
        posture_name=posture_name,
        posture_resp=posture_resp,
        latest_ue=latest_ue,
        measure=measure,
    )

    stage_index = 0
    stage_total = 0
    if pack in STAGED_FULL_PACKS:
        stage_index = STAGED_FULL_PACKS.index(pack) + 1
        stage_total = len(STAGED_FULL_PACKS)

    return {
        "ok": True,
        "pack": pack,
        "target_rnti": target_rnti,
        "posture_name": posture_name,
        "posture": posture_resp,
        "latest_ue": latest_ue,
        "measure": measure,
        "stage_truncated": stage_truncated,
        "full_pack_stage_index": stage_index,
        "full_pack_stage_total": stage_total,
        "summary_zh": summary,
    }


# -------- Tools list --------# -------- Tools list --------
def tools_list():
    return [
        {
            "name": "ric.ping",
            "description": "Ping the RIC control backend (control_xapp).",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "ric.get_state",
            "description": "Get backend state: node info, last_rnti/profile, last control timestamps.",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "ric.ue_list",
            "description": "List current UEs from backend ue_list (includes role, slice runtime, and posture runtime fields if available).",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "ric.tag_ue",
            "description": "Tag UE role in backend: agent/competitor/unknown.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rnti": {"type": "integer"},
                    "role": {"type": "string", "enum": ["agent", "competitor", "unknown"]},
                },
                "required": ["rnti", "role"],
                "additionalProperties": False,
            },
        },
        {
            "name": "ric.claim_agent",
            "description": "Traffic-based claim: Jetson sends a short UL burst, compares gNB UL scheduler event deltas, then tags the winner as agent.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "duration_s": {"type": "number", "default": 1.0},
                    "dst_ip": {"type": "string", "description": "Destination IP for UL burst. Default: system default gateway."},
                    "dst_port": {"type": "integer", "default": 9},
                    "payload_bytes": {"type": "integer", "default": 1200},
                    "pps_limit": {"type": "integer", "description": "Optional packets-per-second cap."},
                    "min_delta": {"type": "integer", "default": 50},
                    "ratio": {"type": "number", "default": 1.2},
                    "settle_s": {"type": "number", "default": 1.2},
                    "poll_retries": {"type": "integer", "default": 6},
                    "poll_interval_s": {"type": "number", "default": 0.4},
                    "exclusive": {"type": "boolean", "default": True}
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "ric.apply_profiles",
            "description": "Load /xapp/profiles.json and apply slice configuration via slice_sm control.",
            "inputSchema": {
                "type": "object",
                "properties": {"force_reload": {"type": "boolean", "default": False}},
                "additionalProperties": False,
            },
        },
        {
            "name": "ric.set_profile",
            "description": "Low-level / debug only. Bind a profile directly without the higher-level scene logic.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "profile": {"type": "string"},
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor", "all"]}
                },
                "required": ["profile"],
                "additionalProperties": True
            },
        },
        {
            "name": "ric.set_mode",
            "description": "Preferred slice-mode control. Applies policy + binds UE. Use for scenario control such as video meeting / low-latency / fairness / background upload.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": list(VALID_MODE_NAMES)},
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest","only","last_rnti"], "default": "latest"},
                    "force_reload": {"type": "boolean", "default": False},
                    "target": {"type": "string", "enum": ["agent", "competitor", "all"]}
                },
                "required": ["mode"],
                "additionalProperties": False
            },
        },
        {
            "name": "ric.set_scene",
            "description": "ALWAYS call this first for communication/network preparation. It chooses a slice mode and a full per-UE communication posture, resolves/claims the target UE if needed, and returns a human-readable summary.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scene": {"type": "string", "description": "Free-form scenario description, Chinese/English ok."},
                    "force_reload": {"type": "boolean", "default": False},
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest","only","last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor", "all"]},
                    "measure_speed": {"type": "boolean", "default": True},
                    "speedtest_seconds": {"type": "integer", "default": 3}
                },
                "required": ["scene"],
                "additionalProperties": False
             }
         },
         {
            "name": "ric.set_posture",
            "description": "Apply a full per-UE communication posture template, including UL link + UL scheduling + DL link + DL scheduling parameters.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "posture": {
                        "type": "string",
                        "enum": ["posture_default", "posture_interactive", "posture_stable", "posture_background_safe", "posture_aggressive", "posture_plain_text", "posture_agentic_loop", "posture_anti_jitter"]
                    },
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]},
                    "measure_speed": {"type": "boolean", "default": False},
                    "speedtest_seconds": {"type": "integer", "default": 3}
                },
                "required": ["posture"],
                "additionalProperties": False
            },
        },
        {
            "name": "ric.get_posture",
            "description": "Read the current full per-UE communication posture and latest runtime metrics.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]}
                },
                "additionalProperties": False
            },
        },
        {
            "name": "ric.clear_posture",
            "description": "Clear the full per-UE communication posture override and return to gNB defaults.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]}
                },
                "additionalProperties": False
            },
        },
        {
            "name": "ric.set_ul_posture",
            "description": "Apply only the UL subset of posture parameters (UL link + UL scheduling), independent from slice mode.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "posture": {
                        "type": "string",
                        "enum": ["ul_default", "ul_interactive", "ul_stable", "ul_background_safe", "ul_aggressive", "ul_plain_text", "ul_agentic_loop", "ul_anti_jitter"]
                    },
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]},
                    "measure_speed": {"type": "boolean", "default": False},
                    "speedtest_seconds": {"type": "integer", "default": 3}
                },
                "required": ["posture"],
                "additionalProperties": False
            },
        },
        {
            "name": "ric.get_ul_posture",
            "description": "Read current UL-only posture view and latest UL runtime metrics.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]}
                },
                "additionalProperties": False
            },
        },
        {
            "name": "ric.clear_ul_posture",
            "description": "Clear only the UL subset of posture override and return to gNB default UL behavior.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]}
                },
                "additionalProperties": False
            },
        },
         {
            "name": "ric.measure_ul_effect",
            "description": "Run a communication validation pack for the target UE. quick is a compact baseline; full pack is now staged into four smaller packs to avoid MCP timeout: full_sustained / full_burst / full_resume / full_rtt. Despite the name, it still covers both UL and DL metrics.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pack": {"type": "string", "enum": ["quick", "full_sustained", "full_burst", "full_resume", "full_rtt"], "default": "quick"},
                    "rnti": {"type": "integer"},
                    "rnti_strategy": {"type": "string", "enum": ["latest", "only", "last_rnti"], "default": "latest"},
                    "target": {"type": "string", "enum": ["agent", "competitor"]}
                },
                "additionalProperties": False
            },
        },
    ]

def classify_scene_to_mode(scene: str) -> str:
    s = (scene or "").lower()

    video_kw = [
        "视频会议", "会议", "开会", "zoom", "teams", "meet", "google meet", "腾讯会议", "钉钉", "webex",
        "直播", "stream", "streaming", "video call", "conference", "screen share", "共享屏幕",
        "视频通话", "连麦", "直播带货", "4k", "8k"
    ]

    image_kw = [
        "图片", "相册", "照片", "修图", "发图", "朋友圈", "instagram", "ins", "微博", "小红书",
        "social", "feed", "scroll", "刷图", "看图", "发照片"
    ]

    throughput_kw = [
        "高吞吐", "吞吐", "大带宽", "高带宽", "高速下载", "高速传输",
        "throughput", "high throughput", "download boost", "boost throughput"
    ]

    latency_kw = [
        "低时延", "低延迟", "latency", "real-time", "实时", "互动", "交互",
        "游戏", "gaming", "电竞", "voip", "web rtc", "remote control", "远程控制"
    ]

    uplink_kw = [
        "上行", "上传", "upload", "backup", "同步", "sync", "发送素材", "上传视频", "云盘备份"
    ]

    background_kw = [
        "后台", "background", "night", "idle", "夜间", "待机", "低优先级", "不着急"
    ]

    fairness_kw = [
        "公平", "别独占", "共享带宽", "让别人也能用", "fair", "fairness"
    ]

    plain_text_kw = [
        "普通文字", "普通文本", "文字传输", "纯文字", "一般聊天", "轻量文本", "plain text", "normal text"
    ]

    agentic_loop_kw = [
        "agentic loop", "agent loop", "多轮agent", "多轮 agent", "agent 间交互", "agent之间交互",
        "多智能体", "multi-agent", "tool call", "tool calling", "工具调用", "回环", "loop"
    ]

    anti_jitter_kw = [
        "jitter", "抖动", "时延波动", "尾时延", "波动小", "更稳的交互", "降低波动", "anti-jitter"
    ]

    text_kw = [
        "文字", "文本", "聊天", "消息", "发消息",
        "短信", "文档", "简讯",
        "传感器", "iot", "telemetry", "遥测", "小数据包", "heartbeat", "心跳", "告警"
    ]

    if any(k in s for k in anti_jitter_kw):
        return "anti_jitter_guard"

    if any(k in s for k in agentic_loop_kw):
        return "agentic_loop"

    if any(k in s for k in plain_text_kw):
        return "plain_text_guard"

    if any(k in s for k in fairness_kw):
        return "fairness_guard"

    if any(k in s for k in background_kw):
        if any(k in s for k in uplink_kw):
            return "background_upload"
        return "night_idle"

    if any(k in s for k in uplink_kw):
        return "burst_uplink"

    if any(k in s for k in throughput_kw):
        return "high_throughput_boost"

    if any(k in s for k in latency_kw):
        return "low_latency_guard"

    if any(k in s for k in video_kw):
        return "video"

    if any(k in s for k in image_kw):
        return "image"

    if any(k in s for k in text_kw):
        return "text"

    return "text"

def _rnti_hex(rnti: int) -> str:
    try:
        return f"0x{int(rnti):x}"
    except Exception:
        return "n/a"
        
def classify_scene_to_posture(scene: str, chosen_mode: Optional[str] = None) -> str:
    s = (scene or "").lower()

    plain_text_kw = ["普通文字", "普通文本", "文字传输", "纯文字", "一般聊天", "轻量文本", "plain text", "normal text"]
    agentic_loop_kw = [
        "agentic loop", "agent loop", "多轮agent", "多轮 agent", "agent 间交互", "agent之间交互",
        "多智能体", "multi-agent", "tool call", "tool calling", "工具调用", "回环", "loop"
    ]
    anti_jitter_kw = ["jitter", "抖动", "时延波动", "尾时延", "波动小", "降低波动", "anti-jitter"]
    stable_kw = ["更稳", "稳定", "保守", "弱覆盖", "边缘", "不要太激进", "视频会议", "直播", "连续下发"]
    interactive_kw = ["突发上传", "首包", "交互上传", "响应快", "恢复快", "低时延上行", "低时延", "互动", "实时控制", "游戏"]
    bg_kw = ["后台", "夜间", "低优先级", "别抢资源", "不要独占", "保守上传", "公平"]
    throughput_kw = ["高吞吐", "素材传输", "持续上传", "持续下载", "大带宽", "高速下载", "高带宽"]

    if any(k in s for k in plain_text_kw):
        return "posture_plain_text"
    if any(k in s for k in agentic_loop_kw):
        return "posture_agentic_loop"
    if any(k in s for k in anti_jitter_kw):
        return "posture_anti_jitter"
    if any(k in s for k in stable_kw):
        return "posture_stable"
    if any(k in s for k in interactive_kw):
        return "posture_interactive"
    if any(k in s for k in bg_kw):
        return "posture_background_safe"
    if any(k in s for k in throughput_kw):
        return "posture_aggressive"

    return MODE_TO_POSTURE.get(str(chosen_mode or ""), "posture_default")


def classify_scene_to_ul_posture(scene: str, chosen_mode: Optional[str] = None) -> str:
    full_name = classify_scene_to_posture(scene, chosen_mode)
    return full_name.replace("posture_", "ul_") if full_name.startswith("posture_") else full_name


def build_posture_summary(posture_name: str,
                          action: Optional[Dict[str, Any]],
                          before_posture: Optional[Dict[str, Any]],
                          after_posture: Optional[Dict[str, Any]],
                          before_ue: Optional[Dict[str, Any]] = None,
                          after_ue: Optional[Dict[str, Any]] = None,
                          speed_before: Optional[Dict[str, Any]] = None,
                          speed_after: Optional[Dict[str, Any]] = None,
                          ul_only: bool = False) -> str:
    preset_map = UL_POSTURE_PRESETS if ul_only else FULL_POSTURE_PRESETS
    preset = preset_map.get(posture_name) or {"label": posture_name, "human_summary": "", "effect_summary": ""}

    before_entry = _normalize_ul_posture_entry((before_posture or {}).get("entry")) if ul_only else _normalize_posture_entry((before_posture or {}).get("entry"))
    after_entry = _normalize_ul_posture_entry((after_posture or {}).get("entry")) if ul_only else _normalize_posture_entry((after_posture or {}).get("entry"))

    lines: List[str] = []
    if preset.get("clear"):
        lines.append(f"{'上行' if ul_only else '通信'}姿态已恢复为 **{preset.get('label', '默认姿态')}**。")
    else:
        lines.append(f"{'上行' if ul_only else '通信'}姿态已切换为 **{preset.get('label', posture_name)}**。")
    if preset.get("human_summary"):
        lines.append(str(preset.get("human_summary")))
    if preset.get("effect_summary"):
        lines.append(f"这意味着：{preset.get('effect_summary')}")

    rnti = None
    try:
        if action and action.get("rnti") is not None:
            rnti = int(action["rnti"])
    except Exception:
        rnti = None
    if rnti is not None:
        lines.append(f"- 目标 UE: RNTI={rnti} (0x{rnti:x})")

    lines.append(f"- {'UE-LA' if ul_only else 'per-UE posture'} 参数变化:")
    if ul_only:
        lines.append("- 上行链路姿态:")
        for k in UL_LINK_PARAM_KEYS:
            lines.append(f"  - {POSTURE_PARAM_LABELS[k]}: {_format_posture_param_value(before_entry.get(k))} -> {_format_posture_param_value(after_entry.get(k))}；{POSTURE_PARAM_EFFECTS[k]}")
        lines.append("- 上行调度姿态:")
        for k in UL_SCHED_PARAM_KEYS:
            lines.append(f"  - {POSTURE_PARAM_LABELS[k]}: {_format_posture_param_value(before_entry.get(k))} -> {_format_posture_param_value(after_entry.get(k))}；{POSTURE_PARAM_EFFECTS[k]}")
    else:
        lines.extend(_posture_group_lines(before_entry, after_entry, include_effects=True))

    lines.extend(_runtime_posture_snapshot(after_ue))

    def _speed_val(pack: Optional[Dict[str, Any]], direction: str) -> Optional[float]:
        if not isinstance(pack, dict):
            return None
        part = pack.get(direction) or {}
        if part.get("ok"):
            try:
                return float(part.get("mbps"))
            except Exception:
                return None
        return None

    ul_before = _speed_val(speed_before, "ul")
    ul_after = _speed_val(speed_after, "ul")
    dl_before = _speed_val(speed_before, "dl")
    dl_after = _speed_val(speed_after, "dl")
    if ul_before is not None or ul_after is not None:
        lines.append(f"- 上行测速: {('N/A' if ul_before is None else round(ul_before, 2))} -> {('N/A' if ul_after is None else round(ul_after, 2))} Mbps")
    if (not ul_only) and (dl_before is not None or dl_after is not None):
        lines.append(f"- 下行测速: {('N/A' if dl_before is None else round(dl_before, 2))} -> {('N/A' if dl_after is None else round(dl_after, 2))} Mbps")

    return "\n".join(lines)


def build_ul_posture_summary(posture_name: str,
                             action: Optional[Dict[str, Any]],
                             before_posture: Optional[Dict[str, Any]],
                             after_posture: Optional[Dict[str, Any]],
                             before_ue: Optional[Dict[str, Any]] = None,
                             after_ue: Optional[Dict[str, Any]] = None,
                             speed_before: Optional[Dict[str, Any]] = None,
                             speed_after: Optional[Dict[str, Any]] = None) -> str:
    return build_posture_summary(
        posture_name=posture_name,
        action=action,
        before_posture=before_posture,
        after_posture=after_posture,
        before_ue=before_ue,
        after_ue=after_ue,
        speed_before=speed_before,
        speed_after=speed_after,
        ul_only=True,
    )

def build_scene_summary(scene: str,
                        chosen_mode: str,
                        action: dict,
                        before: dict,
                        after: dict,
                        speed_before: Optional[Dict[str, Any]] = None,
                        speed_after: Optional[Dict[str, Any]] = None,
                        mode_changed: bool = True,
                        before_ue: Optional[Dict[str, Any]] = None,
                        after_ue: Optional[Dict[str, Any]] = None) -> str:
    mode_cn = {
        "text": "文本/IoT", "image": "图片", "video": "视频/会议", "high_throughput_boost": "高吞吐",
        "low_latency_guard": "低时延偏置", "burst_uplink": "上行加强", "background_upload": "后台上传",
        "night_idle": "夜间低优先级", "fairness_guard": "公平受限",
        "plain_text_guard": "普通文字传输", "agentic_loop": "Agentic Loop", "anti_jitter_guard": "抗抖动保护",
    }.get(chosen_mode, chosen_mode)

    bind = (action or {}).get("bind", {}) if isinstance(action, dict) else {}
    rnti = bind.get("rnti") or (after_ue or {}).get("rnti") or (before_ue or {}).get("rnti") or (after or {}).get("last_rnti") or (before or {}).get("last_rnti")
    profile_meta = (action or {}).get("profile_meta") or {}
    policy_before = (action or {}).get("policy_before") or {}
    policy_after = (action or {}).get("policy") or {}
    policy_diff = (action or {}).get("policy_diff") or {}

    before_mode = (before or {}).get("active_mode")
    after_mode = (after or {}).get("active_mode")
    before_alg = (before or {}).get("active_alg")
    after_alg = (after or {}).get("active_alg")
    last_ctrl = (after or {}).get("last_ctrl")

    def _speed_val(pack: Optional[Dict[str, Any]], direction: str) -> Optional[float]:
        if not isinstance(pack, dict):
            return None
        part = pack.get(direction) or {}
        if part.get("ok"):
            try:
                return float(part.get("mbps"))
            except Exception:
                return None
        return None

    def _fmt_num(v):
        return f"{v:.2f}" if isinstance(v, (int, float)) else "N/A"

    def _fmt_delta(before_v, after_v):
        if before_v is None or after_v is None:
            return "N/A"
        d = float(after_v) - float(before_v)
        return ("+" if d >= 0 else "") + f"{d:.2f}"

    dl_before = _speed_val(speed_before, "dl")
    dl_after = _speed_val(speed_after, "dl")
    ul_before = _speed_val(speed_before, "ul")
    ul_after = _speed_val(speed_after, "ul")

    lines: List[str] = []
    lines.append(f"已{'切换到' if mode_changed else '保持在'} **{mode_cn}** 模式，场景是「{scene}」。")
    if profile_meta.get("human_summary"):
        lines.append(str(profile_meta.get("human_summary")))
    if profile_meta.get("what_changes"):
        lines.append(f"这次策略变化意味着：{profile_meta.get('what_changes')}")
    if profile_meta.get("when_to_use"):
        lines.append(f"适用场景：{profile_meta.get('when_to_use')}")

    lines.append("")
    lines.append("| 项目 | 切换前 | 切换后 |")
    lines.append("|---|---:|---:|")
    lines.append(f"| 模式 | {before_mode or 'unknown'} | {after_mode or 'unknown'} |")
    lines.append(f"| 算法 | {before_alg or 'unknown'} | {after_alg or 'unknown'} |")
    lines.append(f"| 下行测速 (Mbps) | {_fmt_num(dl_before)} | {_fmt_num(dl_after)} |")
    lines.append(f"| 上行测速 (Mbps) | {_fmt_num(ul_before)} | {_fmt_num(ul_after)} |")
    lines.append(f"| 下行变化 (Mbps) | - | {_fmt_delta(dl_before, dl_after)} |")
    lines.append(f"| 上行变化 (Mbps) | - | {_fmt_delta(ul_before, ul_after)} |")

    if rnti is not None:
        lines.append("")
        lines.append(f"- 目标 UE: RNTI={rnti} ({_rnti_hex(rnti)})")

    def _fmt_param_change(half, key, label):
        d = (policy_diff.get(half) or {}).get(key)
        if d:
            return f"{label}: {d.get('before')} -> {d.get('after')}"
        return f"{label}: {(policy_before.get(half) or {}).get(key)} -> {(policy_after.get(half) or {}).get(key)}"

    if policy_after:
        lines.append("")
        lines.append("- 底层 9 参数变化:")
        for half, title in (("dl", "DL"), ("ul", "UL")):
            lines.append(
                f"  - {title}: " + "; ".join([
                    _fmt_param_change(half, "slice_id", "slice_id"),
                    _fmt_param_change(half, "weight_mul", "weight_mul"),
                    _fmt_param_change(half, "rb_cap", "rb_cap"),
                    _fmt_param_change(half, "rb_floor", "rb_floor"),
                    _fmt_param_change(half, "max_consecutive_grants", "max_consecutive_grants"),
                ])
            )
        lines.append(f"- 当前绑定: DL={(policy_after.get('dl') or {}).get('name')} / UL={(policy_after.get('ul') or {}).get('name')}")

    runtime = (after_ue or {}).copy() if isinstance(after_ue, dict) else {}
    if runtime:
        lines.append("")
        lines.append("- 最近运行观测:")
        lines.append(f"  - 最近一次 grant: DL rbSize/current_rbs={runtime.get('dl_rbSize')}/{runtime.get('dl_current_rbs')}, UL rbSize/current_rbs={runtime.get('ul_rbSize')}/{runtime.get('ul_current_rbs')}")
        lines.append(f"  - throttled 次数: DL={runtime.get('dl_throttled_count', 0)}, UL={runtime.get('ul_throttled_count', 0)}")
        lines.append(f"  - grant 计数: DL={runtime.get('dl_grant_count', 0)}, UL={runtime.get('ul_grant_count', 0)}")
        def _cg_text(v):
            return "不限" if v in (None, 0) else f"最多连续 {v} 拍后更容易被抑制"
        lines.append(f"  - 连续占用解释: DL={_cg_text(runtime.get('dl_maxcg'))}, UL={_cg_text(runtime.get('ul_maxcg'))}")

    emphasize = profile_meta.get("emphasize_direction")
    if emphasize == "ul":
        lines.append("- 当前模式更偏向上行，请重点关注上行测速和 UL rbSize。")
    elif emphasize == "dl":
        lines.append("- 当前模式更偏向下行，请重点关注下行测速和 DL rbSize。")
    else:
        lines.append("- 当前模式同时关注上下行，建议结合 DL/UL 测速一起看。")

    if isinstance(last_ctrl, dict) and last_ctrl.get("cmd") in ("set_mode", "set_profile"):
        lines.append("- 验收: 后端已记录本次模式/策略切换。")
    return "\n".join(lines)

def _find_ue_by_rnti(raw_ue_list: Dict[str, Any], rnti: Optional[int]) -> Optional[Dict[str, Any]]:
    if rnti is None or not raw_ue_list or not raw_ue_list.get("ok"):
        return None
    for u in (raw_ue_list.get("ues") or []):
        try:
            if int(u.get("rnti")) == int(rnti):
                return u
        except Exception:
            pass
    return None
    
def tools_call(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tool dispatcher:
      - prefers target=agent by default in multi-UE scenarios
      - supports full posture tools in addition to UL-only posture tools
      - set_scene now applies slice mode + full posture together
      - measure_ul_effect keeps the old tool name but now measures both UL and DL effects
    """

    def _ue_list_raw() -> Dict[str, Any]:
        raw = backend_call({
            "cmd": "ue_list",
            "active_only": True,
            "max_age_s": 15,
            "src": "gnb_rnti_watcher"
        })
        if raw.get("ok"):
            return raw
        if "_best_effort_ue_list" in globals() and callable(globals()["_best_effort_ue_list"]):
            return globals()["_best_effort_ue_list"]()
        return raw

    def _resolve_target(target: str) -> Dict[str, Any]:
        fn = globals().get("_resolve_target_rnti")
        if not callable(fn):
            return {"ok": False, "error": "_resolve_target_rnti() not found; please add the helper first"}
        return fn(target, auto_claim=True)

    def _multi_ue_present() -> bool:
        raw = _ue_list_raw()
        if not raw.get("ok"):
            return False
        return len(raw.get("ues") or []) >= 2

    def _build_set_payload(cmd: str, mode_or_profile_key: str, mode_or_profile_val: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"cmd": cmd, mode_or_profile_key: mode_or_profile_val}

        if "force_reload" in arguments and arguments["force_reload"] is not None:
            payload["force_reload"] = bool(arguments["force_reload"])

        tgt = arguments.get("target")
        rnti = arguments.get("rnti", None)
        rnti_strategy = arguments.get("rnti_strategy", None)

        if (tgt is None) and (rnti is None) and _multi_ue_present():
            tgt = "agent"

        if tgt in ("agent", "competitor"):
            res = _resolve_target(str(tgt))
            if not res.get("ok"):
                return {"__resolve_failed__": True, "detail": res}
            payload["rnti"] = int(res["rnti"])
            payload["rnti_strategy"] = "only"
            payload["target"] = str(tgt)
            return payload

        if rnti is not None:
            payload["rnti"] = int(rnti)
        if rnti_strategy is not None:
            payload["rnti_strategy"] = str(rnti_strategy)
        if tgt is not None:
            payload["target"] = str(tgt)
        return payload

    def _build_target_payload(cmd: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"cmd": cmd}
        tgt = arguments.get("target")
        rnti = arguments.get("rnti", None)
        rnti_strategy = arguments.get("rnti_strategy", None)

        if (tgt is None) and (rnti is None) and _multi_ue_present():
            tgt = "agent"

        if tgt in ("agent", "competitor"):
            res = _resolve_target(str(tgt))
            if not res.get("ok"):
                return {"__resolve_failed__": True, "detail": res}
            payload["rnti"] = int(res["rnti"])
            payload["rnti_strategy"] = "only"
            payload["target"] = str(tgt)
            return payload

        if rnti is not None:
            payload["rnti"] = int(rnti)
        if rnti_strategy is not None:
            payload["rnti_strategy"] = str(rnti_strategy)
        if tgt is not None:
            payload["target"] = str(tgt)
        return payload

    def _speed_pair(measure_speed: bool, seconds: int) -> Dict[str, Any]:
        return _quick_speedtest_pair(duration_s=seconds) if measure_speed else {
            "dl": {"ok": False, "skipped": True},
            "ul": {"ok": False, "skipped": True},
        }

    def _resolve_posture_rnti() -> Tuple[Optional[int], Dict[str, Any], Optional[Dict[str, Any]]]:
        payload = _build_target_payload("get_ue_posture")
        if payload.get("__resolve_failed__"):
            return None, payload, None
        target_rnti = payload.get("rnti")
        ue_list = _ue_list_raw()
        ue = _find_ue_by_rnti(ue_list, target_rnti)
        return target_rnti, ue_list, ue

    # ---------------- basic tools ----------------
    if name == "ric.ping":
        return backend_call({"cmd": "ping"})

    if name == "ric.get_state":
        return backend_call({"cmd": "get_state"})

    if name == "ric.ue_list":
        return _ue_list_raw()

    # ---------------- role tools ----------------
    if name == "ric.tag_ue":
        rnti = int(arguments.get("rnti"))
        role = str(arguments.get("role"))
        return backend_call({"cmd": "tag_ue", "rnti": rnti, "role": role})

    if name == "ric.claim_agent":
        fn = globals().get("_claim_agent")
        if not callable(fn):
            return {"ok": False, "error": "_claim_agent() not found; please patch server.py to add robust claim_agent first"}

        dur = float(arguments.get("duration_s", 1.0))
        dst_ip = arguments.get("dst_ip", None)
        dst_port = int(arguments.get("dst_port", 9))
        payload_bytes = int(arguments.get("payload_bytes", 1200))
        pps_limit = arguments.get("pps_limit", None)
        pps_limit = int(pps_limit) if pps_limit is not None else None
        min_delta = int(arguments.get("min_delta", 50))
        ratio = float(arguments.get("ratio", 1.2))
        settle_s = float(arguments.get("settle_s", 1.2))
        poll_retries = int(arguments.get("poll_retries", 6))
        poll_interval_s = float(arguments.get("poll_interval_s", 0.4))
        exclusive = bool(arguments.get("exclusive", True))

        return fn(
            duration_s=dur,
            dst_ip=dst_ip,
            dst_port=dst_port,
            payload_bytes=payload_bytes,
            pps_limit=pps_limit,
            min_delta=min_delta,
            ratio=ratio,
            settle_s=settle_s,
            poll_retries=poll_retries,
            poll_interval_s=poll_interval_s,
            exclusive=exclusive,
        )

    # ---------------- policy tools ----------------
    if name == "ric.apply_profiles":
        force = bool(arguments.get("force_reload", False))
        return backend_call({"cmd": "apply_profiles", "force_reload": force})

    if name == "ric.set_profile":
        profile = str(arguments.get("profile", "default"))
        payload = _build_set_payload(cmd="set_profile", mode_or_profile_key="profile", mode_or_profile_val=profile)
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        return backend_call(payload)

    if name == "ric.set_mode":
        mode_raw = str(arguments.get("mode", "default"))
        mode = _normalize_mode_alias(mode_raw)
        payload = _build_set_payload(cmd="set_mode", mode_or_profile_key="mode", mode_or_profile_val=mode)
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        return backend_call(payload)

    if name == "ric.get_posture":
        payload = _build_target_payload("get_ue_posture")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        resp = backend_call(payload)
        target_rnti = payload.get("rnti")
        ue_list = _ue_list_raw()
        ue = _find_ue_by_rnti(ue_list, target_rnti)
        posture_name = _guess_posture_name((resp or {}).get("entry") if resp.get("ok") else {})
        summary = build_posture_summary(
            posture_name=posture_name,
            action={"rnti": target_rnti},
            before_posture=resp,
            after_posture=resp,
            before_ue=ue,
            after_ue=ue,
            ul_only=False,
        )
        resp["posture_name"] = posture_name
        resp["summary_zh"] = summary
        resp["ue_list"] = ue_list
        return resp

    if name == "ric.clear_posture":
        payload = _build_target_payload("clear_ue_posture")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        target_rnti = payload.get("rnti")
        before_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        before_ue_list = _ue_list_raw()
        before_ue = _find_ue_by_rnti(before_ue_list, target_rnti)
        action = backend_call(payload)
        time.sleep(max(0.0, SPEEDTEST_SETTLE_AFTER_MODE_S))
        after_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        after_ue_list = _ue_list_raw()
        after_ue = _find_ue_by_rnti(after_ue_list, target_rnti)
        summary = build_posture_summary(
            posture_name="posture_default",
            action={"rnti": target_rnti},
            before_posture=before_pose,
            after_posture=after_pose,
            before_ue=before_ue,
            after_ue=after_ue,
            ul_only=False,
        )
        return {"ok": bool(action.get("ok")), "summary_zh": summary, "posture_name": "posture_default", "action": action, "before_posture": before_pose, "after_posture": after_pose, "ue_list": after_ue_list}

    if name == "ric.set_posture":
        posture_name = str(arguments.get("posture", "posture_default"))
        preset = FULL_POSTURE_PRESETS.get(posture_name)
        if not preset:
            return {"ok": False, "error": "unknown posture", "allowed": list(FULL_POSTURE_PRESETS.keys())}
        payload = _build_target_payload("set_ue_posture")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        target_rnti = payload.get("rnti")
        before_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        before_ue_list = _ue_list_raw()
        before_ue = _find_ue_by_rnti(before_ue_list, target_rnti)
        measure_speed = bool(arguments.get("measure_speed", False))
        speedtest_seconds = int(arguments.get("speedtest_seconds", SPEEDTEST_DURATION_S))
        speed_before = _speed_pair(measure_speed, speedtest_seconds)
        if preset.get("clear"):
            action = backend_call({"cmd": "clear_ue_posture", "rnti": int(target_rnti)})
        else:
            for k in FULL_POSTURE_PARAM_KEYS:
                if k in preset:
                    payload[k] = preset[k]
            action = backend_call(payload)
        time.sleep(max(0.0, SPEEDTEST_SETTLE_AFTER_MODE_S))
        speed_after = _speed_pair(measure_speed, speedtest_seconds)
        after_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        after_ue_list = _ue_list_raw()
        after_ue = _find_ue_by_rnti(after_ue_list, target_rnti)
        summary = build_posture_summary(
            posture_name=posture_name,
            action={"rnti": target_rnti},
            before_posture=before_pose,
            after_posture=after_pose,
            before_ue=before_ue,
            after_ue=after_ue,
            speed_before=speed_before,
            speed_after=speed_after,
            ul_only=False,
        )
        return {
            "ok": bool(action.get("ok")),
            "summary_zh": summary,
            "posture_name": posture_name,
            "preset": preset,
            "action": action,
            "before_posture": before_pose,
            "after_posture": after_pose,
            "speed_before": speed_before,
            "speed_after": speed_after,
            "ue_list": after_ue_list,
        }

    if name == "ric.get_ul_posture":
        payload = _build_target_payload("get_ue_posture")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        resp = backend_call(payload)
        target_rnti = payload.get("rnti")
        ue_list = _ue_list_raw()
        ue = _find_ue_by_rnti(ue_list, target_rnti)
        posture_name = _guess_ul_posture_name((resp or {}).get("entry") if resp.get("ok") else {})
        summary = build_ul_posture_summary(
            posture_name=posture_name,
            action={"rnti": target_rnti},
            before_posture=resp,
            after_posture=resp,
            before_ue=ue,
            after_ue=ue,
        )
        resp["posture_name"] = posture_name
        resp["summary_zh"] = summary
        resp["ue_list"] = ue_list
        return resp

    if name == "ric.clear_ul_posture":
        payload = _build_target_payload("clear_ue_ul_la")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        target_rnti = payload.get("rnti")
        before_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        before_ue_list = _ue_list_raw()
        before_ue = _find_ue_by_rnti(before_ue_list, target_rnti)
        action = backend_call(payload)
        time.sleep(max(0.0, SPEEDTEST_SETTLE_AFTER_MODE_S))
        after_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        after_ue_list = _ue_list_raw()
        after_ue = _find_ue_by_rnti(after_ue_list, target_rnti)
        summary = build_ul_posture_summary(
            posture_name="ul_default",
            action={"rnti": target_rnti},
            before_posture=before_pose,
            after_posture=after_pose,
            before_ue=before_ue,
            after_ue=after_ue,
        )
        return {"ok": bool(action.get("ok")), "summary_zh": summary, "posture_name": "ul_default", "action": action, "before_posture": before_pose, "after_posture": after_pose, "ue_list": after_ue_list}

    if name == "ric.set_ul_posture":
        posture_name = str(arguments.get("posture", "ul_default"))
        preset = UL_POSTURE_PRESETS.get(posture_name)
        if not preset:
            return {"ok": False, "error": "unknown posture", "allowed": list(UL_POSTURE_PRESETS.keys())}
        payload = _build_target_payload("set_ue_ul_la")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        target_rnti = payload.get("rnti")
        before_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        before_ue_list = _ue_list_raw()
        before_ue = _find_ue_by_rnti(before_ue_list, target_rnti)
        measure_speed = bool(arguments.get("measure_speed", False))
        speedtest_seconds = int(arguments.get("speedtest_seconds", SPEEDTEST_DURATION_S))
        speed_before = _speed_pair(measure_speed, speedtest_seconds)
        if preset.get("clear"):
            action = backend_call({"cmd": "clear_ue_ul_la", "rnti": int(target_rnti)})
        else:
            for k in UL_POSTURE_PARAM_KEYS:
                if k in preset:
                    payload[k] = preset[k]
            action = backend_call(payload)
        time.sleep(max(0.0, SPEEDTEST_SETTLE_AFTER_MODE_S))
        speed_after = _speed_pair(measure_speed, speedtest_seconds)
        after_pose = backend_call({"cmd": "get_ue_posture", "rnti": int(target_rnti)}) if target_rnti is not None else {"ok": False}
        after_ue_list = _ue_list_raw()
        after_ue = _find_ue_by_rnti(after_ue_list, target_rnti)
        summary = build_ul_posture_summary(
            posture_name=posture_name,
            action={"rnti": target_rnti},
            before_posture=before_pose,
            after_posture=after_pose,
            before_ue=before_ue,
            after_ue=after_ue,
            speed_before=speed_before,
            speed_after=speed_after,
        )
        return {
            "ok": bool(action.get("ok")),
            "summary_zh": summary,
            "posture_name": posture_name,
            "preset": preset,
            "action": action,
            "before_posture": before_pose,
            "after_posture": after_pose,
            "speed_before": speed_before,
            "speed_after": speed_after,
            "ue_list": after_ue_list,
        }

    if name == "ric.measure_ul_effect":
        payload = _build_target_payload("get_ue_posture")
        if payload.get("__resolve_failed__"):
            return {"ok": False, "error": "resolve target failed", "detail": payload["detail"]}
        target_rnti = payload.get("rnti")
        pack = str(arguments.get("pack", "quick")).lower()
        if pack not in (("quick",) + STAGED_FULL_PACKS + ("full",)):
            return {"ok": False, "error": "pack must be quick|full_sustained|full_burst|full_resume|full_rtt"}
        return _measure_ul_effect(target_rnti=target_rnti, pack=pack)

    if name == "ric.set_scene":
        scene = str(arguments.get("scene", ""))
        chosen_mode = classify_scene_to_mode(scene)
        chosen_posture = classify_scene_to_posture(scene, chosen_mode)
        speedtest_seconds = int(arguments.get("speedtest_seconds", SPEEDTEST_DURATION_S))
        measure_speed = bool(arguments.get("measure_speed", True))

        set_mode_args = dict(arguments)
        if set_mode_args.get("target") is None and set_mode_args.get("rnti") is None:
            set_mode_args["target"] = "agent"

        target_res = _resolve_target("agent")
        target_rnti = int(target_res["rnti"]) if target_res.get("ok") else None

        before = backend_call({"cmd": "get_state"})
        before_ue_list = _ue_list_raw()
        before_ue = _find_ue_by_rnti(before_ue_list, target_rnti)
        current_mode = (before or {}).get("active_mode")
        speed_before = _speed_pair(measure_speed, speedtest_seconds)

        posture_args = dict(arguments)
        posture_args["posture"] = chosen_posture
        posture_args["measure_speed"] = False
        if posture_args.get("target") is None and posture_args.get("rnti") is None:
            posture_args["target"] = "agent"

        if current_mode == chosen_mode:
            posture_action = tools_call("ric.set_posture", posture_args)
            time.sleep(max(0.0, SPEEDTEST_SETTLE_AFTER_MODE_S))
            speed_after = _speed_pair(measure_speed, speedtest_seconds)
            after = backend_call({"cmd": "get_state"})
            after_ue_list = _ue_list_raw()
            after_ue = _find_ue_by_rnti(after_ue_list, target_rnti)
            summary_mode = build_scene_summary(scene, chosen_mode, {"ok": True, "bind": {}, "note": "already_in_target_mode"}, before, after, speed_before, speed_after, False, before_ue, after_ue)
            summary = summary_mode
            if posture_action.get("summary_zh"):
                summary += "\n\n" + str(posture_action["summary_zh"])
            return {
                "ok": bool(after.get("ok")) and bool(posture_action.get("ok", True)),
                "summary_zh": summary,
                "_scene": scene,
                "_chosen_mode": chosen_mode,
                "_chosen_posture": chosen_posture,
                "action": {"ok": True, "skipped_switch": True},
                "posture_action": posture_action,
                "state_before": before,
                "state_after": after,
                "speed_before": speed_before,
                "speed_after": speed_after,
                "ue_list": after_ue_list,
            }

        set_mode_args["mode"] = chosen_mode
        action = tools_call("ric.set_mode", set_mode_args)
        posture_action = tools_call("ric.set_posture", posture_args)
        time.sleep(max(0.0, SPEEDTEST_SETTLE_AFTER_MODE_S))
        speed_after = _speed_pair(measure_speed, speedtest_seconds)
        after = backend_call({"cmd": "get_state"})
        after_ue_list = _ue_list_raw()
        bind_rnti = None
        try:
            bind_rnti = int(((action or {}).get("bind") or {}).get("rnti"))
        except Exception:
            bind_rnti = None
        after_rnti = bind_rnti or target_rnti
        after_ue = _find_ue_by_rnti(after_ue_list, after_rnti)
        summary_mode = build_scene_summary(scene, chosen_mode, action, before, after, speed_before, speed_after, True, before_ue, after_ue)
        summary = summary_mode
        if posture_action.get("summary_zh"):
            summary += "\n\n" + str(posture_action["summary_zh"])
        return {
            "ok": bool(action.get("ok")) and bool(after.get("ok")) and bool(posture_action.get("ok", True)),
            "summary_zh": summary,
            "_scene": scene,
            "_chosen_mode": chosen_mode,
            "_chosen_posture": chosen_posture,
            "action": action,
            "posture_action": posture_action,
            "state_before": before,
            "state_after": after,
            "speed_before": speed_before,
            "speed_after": speed_after,
            "ue_list": after_ue_list,
        }

    return {"ok": False, "error": f"unknown tool: {name}"}

# -------- stdio protocol --------
def write_framed(obj: Dict[str, Any]):
    raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    hdr = f"Content-Length: {len(raw)}\r\n\r\n".encode("utf-8")
    sys.stdout.buffer.write(hdr + raw)
    sys.stdout.buffer.flush()

def write_line(obj: Dict[str, Any]):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()

def read_exact(n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sys.stdin.buffer.read(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return buf

def mcp_result_text(idv, out_obj: Dict[str, Any]):
    if isinstance(out_obj, dict) and out_obj.get("summary_zh"):
        text = str(out_obj["summary_zh"])
    else:
        text = json.dumps(out_obj, ensure_ascii=False)
    return mcp_result(idv, {"content": [{"type": "text", "text": text}]})

def handle_request(req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    idv = req.get("id", None)
    method = req.get("method", "")

    try:
        if method == "initialize":
            params = req.get("params", {}) or {}
            pv = params.get("protocolVersion") or "2024-11-05"
            return mcp_result(idv, {
                "protocolVersion": pv,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "ric-mcp", "version": "0.4.0"},
            })

        if method in ("notifications/initialized", "initialized"):
            return None

        if method == "tools/list":
            return mcp_result(idv, {"tools": tools_list()})

        if method == "tools/call":
            params = req.get("params", {}) or {}
            tool_name = params.get("name")
            args = params.get("arguments", {}) or {}
            if not tool_name:
                return mcp_error(idv, "missing params.name")
            out = tools_call(tool_name, args)
            return mcp_result_text(idv, out)

        return mcp_error(idv, f"unknown method: {method}")

    except Exception as e:
        return mcp_error(idv, "server exception", {"detail": str(e), "traceback": traceback.format_exc()})

def main():
    while True:
        first = sys.stdin.buffer.readline()
        if not first:
            break

        # framing mode
        if first.lower().startswith(b"content-length:"):
            try:
                headers = {"content-length": int(first.split(b":", 1)[1].strip())}
                while True:
                    line = sys.stdin.buffer.readline()
                    if not line:
                        return
                    if line in (b"\r\n", b"\n"):
                        break
                    k, v = line.split(b":", 1)
                    kl = k.decode("utf-8", "ignore").strip().lower()
                    if kl == "content-length":
                        headers["content-length"] = int(v.strip())

                n = int(headers.get("content-length", 0))
                body = read_exact(n)
                req = json.loads(body.decode("utf-8", "replace"))

                resp = handle_request(req)
                if resp is not None and "id" in req:
                    write_framed(resp)

            except Exception as e:
                write_framed(mcp_error(None, "invalid framed request", {"detail": str(e)}))
            continue

        # legacy line-json mode
        line = first.strip()
        if not line:
            continue
        try:
            req = json.loads(line.decode("utf-8", "replace"))
            resp = handle_request(req)
            if resp is not None and "id" in req:
                write_line(resp)
        except Exception as e:
            write_line(mcp_error(None, "invalid json line", {"detail": str(e)}))

if __name__ == "__main__":
    main()
