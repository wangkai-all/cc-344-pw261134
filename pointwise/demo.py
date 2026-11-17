#!/usr/bin/env python3
"""End-to-end demonstration, built from scratch with real HTTP pagination.

Scenario chain (each step waits for reconciliation against the SOURCE manifest):

  0. boot source / scheduler / receiver / web; generate local material
  1. two initial shards, collectors drain them -> reconciled, zero missing
  2. new material keeps arriving -> idle-polling collectors pick it up
  3. source temporarily refuses requests (429 + Retry-After) -> backoff, no loss
  4. SPLIT a range while it is owned -> old collector is fenced and exits;
     two new collectors take the children
  5. OVERLAP handoff on the other shard -> bounded duplicate fetches,
     receiver dedups: duplicates counted, zero new-record loss
  6. MERGE two shards -> merged shard drained -> globally reconciled
  7. old-process recovery: an old token is explicitly replayed against the
     receiver and rejected (fencing proof)
  8. report comparing the source's authoritative record list with the receiver,
     including request-cost evidence from the source access log
"""
from __future__ import annotations

import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from urllib.request import urlopen

from common import http, setup_logging

HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = {"source": 8091, "scheduler": 8092, "receiver": 8093, "web": 8080}
URLS = {k: f"http://127.0.0.1:{p}" for k, p in PORTS.items()}


def wait_healthy(url: str, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, _ = http("GET", f"{url}/health")
        if status == 200:
            return True
        time.sleep(0.3)
    return False


class Harness:
    def __init__(self, data_dir: str, log_dir: str):
        self.data_dir = data_dir
        self.log_dir = log_dir
        self.procs: dict[str, subprocess.Popen] = {}
        self.deaths: dict[str, int] = {}
        self._stopping = threading.Event()
        self._watcher = threading.Thread(target=self._watch, daemon=True)
        self._watcher.start()

    def _watch(self):
        """Record any service that exits on its own (with the signal/exit code)."""
        while not self._stopping.is_set():
            for name, proc in list(self.procs.items()):
                code = proc.poll()
                if code is not None and name not in self.deaths:
                    self.deaths[name] = code
                    print(f"  ✗✗ 服务 {name} 自行退出！returncode={code} "
                          f"(负数=被信号 {-code} 杀死)")
            time.sleep(0.4)

    def start(self, name: str, script: str, extra: list[str]):
        os.makedirs(self.log_dir, exist_ok=True)
        log_fh = open(os.path.join(self.log_dir, f"{name}.log"), "ab")
        cmd = [sys.executable, "-I", os.path.join(HERE, script), *extra]
        # own session/process group: signals target this tree only, never siblings
        proc = subprocess.Popen(cmd, cwd=HERE, stdout=log_fh, stderr=log_fh,
                                start_new_session=True)
        self.procs[name] = proc
        if not wait_healthy(URLS[name]):
            raise RuntimeError(f"service {name} did not become healthy")
        print(f"  ✓ {name:9s} pid={proc.pid}  {URLS[name]}")

    def assert_healthy(self):
        for name, code in self.deaths.items():
            raise AssertionError(f"服务 {name} 已退出 (returncode={code})，见 logs/{name}.log")

    def stop_all(self):
        self._stopping.set()
        for name, proc in reversed(self.procs.items()):
            if proc.poll() is None:
                # kill the whole process group so collector grandchildren cannot
                # outlive the demo as orphans
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except ProcessLookupError:
                    proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    except ProcessLookupError:
                        proc.kill()


def wait_reconciled(label: str, timeout: float = 40.0) -> dict:
    """Poll global receiver evidence until it reconciles with the source."""
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = http("GET", f"{URLS['receiver']}/evidence")
        if last.get("reconciled"):
            print(f"  ✓ {label}: 对账一致 ({last['receiver_distinct_records']} 条, "
                  f"checksum={last['receiver_checksum']})")
            return last
        time.sleep(0.7)
    raise AssertionError(f"{label}: 对账超时，最后证据={json.dumps(last, ensure_ascii=False)}")


def wait_collector_fenced(collector_id: str, timeout: float = 15.0) -> bool:
    """A fenced collector exits; once gone from the registry it has fenced.

    We accept two observations: an explicit `fenced` runtime status, or the
    collector disappearing from the supervisor (the reaper removes exited
    processes every 2s, which can outrun our polling).
    """
    deadline = time.time() + timeout
    saw_present = False
    while time.time() < deadline:
        _, body = http("GET", f"{URLS['web']}/api/collectors")
        match = next((c for c in body.get("collectors", [])
                      if c["collector_id"] == collector_id), None)
        if match is None:
            if saw_present:
                return True  # was running, now reaped after exiting
            time.sleep(0.3)
            continue
        saw_present = True
        if not match["alive"] or match["runtime"].get("status") == "fenced":
            return True
        time.sleep(0.4)
    return False


def post(path: str, body=None):
    status, resp = http("POST", f"{URLS['web']}{path}", body or {})
    return {"status": status, "data": resp}


SCRIPT_NAMES = ("source.py", "scheduler.py", "receiver.py", "web.py", "collector.py")


def kill_leftover_processes():
    """Kill stale pointwise processes from earlier (possibly crashed) runs.

    Matches by script-name suffix so both `python3` and `/usr/bin/python3`
    invocations are caught; never matches this driver (it runs as demo.py).
    """
    import glob
    killed = []
    for entry in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            raw = open(entry, "rb").read()
        except OSError:
            continue
        parts = raw.split(b"\0")
        argv = [p.decode(errors="replace") for p in parts if p]
        if not argv or not argv[0].endswith("python3"):
            continue
        if not any(arg.endswith(SCRIPT_NAMES) for arg in argv[1:]):
            continue
        pid = int(entry.split("/")[2])
        if pid == os.getpid():
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            killed.append(pid)
        except (ProcessLookupError, PermissionError):
            pass
    if killed:
        time.sleep(1.2)
        for pid in killed:  # second pass for stubborn survivors
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        print(f"  清理上次运行遗留进程 {len(killed)} 个：{killed}")


def section(title: str):
    print(f"\n{'─' * 72}\n▶ {title}\n{'─' * 72}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave services running afterwards")
    ap.add_argument("--data-dir", default=os.path.join(HERE, "data"))
    ap.add_argument("--log-dir", default=os.path.join(HERE, "logs"))
    ap.add_argument("--report", default=os.path.join(HERE, "data", "demo-report.json"))
    args = ap.parse_args()
    setup_logging()
    report = {"steps": []}

    # clean slate: no stale services/collectors from a previous crashed run
    kill_leftover_processes()

    # fresh start: remove old databases so the run is genuinely from zero
    for fname in os.listdir(args.data_dir) if os.path.isdir(args.data_dir) else []:
        if fname.endswith((".db", ".db-wal", ".db-shm")) or fname.endswith(".state.json"):
            os.remove(os.path.join(args.data_dir, fname))
    os.makedirs(os.path.join(args.data_dir, "run"), exist_ok=True)

    harness = Harness(args.data_dir, args.log_dir)
    try:
        section("步骤 0  从零启动四个独立服务")
        harness.start("source", "source.py",
                      ["--db", f"{args.data_dir}/source.db", "--seed", "400"])
        harness.start("scheduler", "scheduler.py", ["--db", f"{args.data_dir}/scheduler.db"])
        harness.start("receiver", "receiver.py", ["--db", f"{args.data_dir}/receiver.db"])
        harness.start("web", "web.py", ["--db", f"{args.data_dir}/web.db"])

        section("步骤 1  建立两个初始分片并启动采集器（真实分页抓取）")
        _, s1 = http("POST", f"{URLS['scheduler']}/shards", {"lo": 1, "hi": 200})
        _, s2 = http("POST", f"{URLS['scheduler']}/shards", {"lo": 200})
        shard_a, shard_b = s1["id"], s2["id"]
        r1 = post("/api/collectors/start", {"shard_id": shard_a})
        r2 = post("/api/collectors/start", {"shard_id": shard_b})
        col_a, col_b = r1["data"]["collector_id"], r2["data"]["collector_id"]
        print(f"  分片 A={shard_a} [1,200)  采集器={col_a}")
        print(f"  分片 B={shard_b} [200,open) 采集器={col_b}")
        ev1 = wait_reconciled("初始两片抓取完成")
        report["steps"].append({"step": 1, "name": "initial_drain", "evidence": ev1})

        section("步骤 2  迁移期间持续有新资料到达（再注入 120 条）")
        _, inj = http("POST", f"{URLS['source']}/control/inject", {"count": 120})
        print(f"  新资料 {inj['injected']} 条，max_id 现在是 {inj['max_id']}；空闲轮询自动续抓")
        ev2 = wait_reconciled("新资料全部到齐")
        report["steps"].append({"step": 2, "name": "live_injection", "evidence": ev2})

        section("步骤 3  来源暂时拒绝请求（429 + Retry-After，窗口 10 秒）")
        _, ref = http("POST", f"{URLS['source']}/control/refuse",
                      {"seconds": 10, "retry_after": 2})
        print(f"  拒绝窗口至 epoch={ref['refuse_until']:.0f}；采集器应退避且不丢进度")
        time.sleep(12)
        _, col_status = http("GET", f"{URLS['web']}/api/collectors")
        refusals = sum(c["runtime"].get("stats", {}).get("refusals", 0)
                       for c in col_status["collectors"] if c.get("alive"))
        print(f"  窗口内采集器记录的 429 退避次数：{refusals}")
        ev3 = wait_reconciled("拒绝窗口结束后继续对账")
        assert refusals > 0, "未观察到任何 429 退避"
        report["steps"].append({"step": 3, "name": "source_refusal",
                                "refusals_observed": refusals, "evidence": ev3})

        section("步骤 4  拆分分片 A（旧进程恢复 → 被 fence 退出，新任务接管两片）")
        _, split = http("POST", f"{URLS['scheduler']}/shards/{shard_a}/split", {})
        left, right = split["left"]["id"], split["right"]["id"]
        print(f"  父片 {shard_a} epoch+1 退役；子片 {left} [1,mid) + {right} [mid,200)")
        fenced = wait_collector_fenced(col_a)
        print(f"  旧采集器 {col_a} 被 fence 并自动退出：{fenced}")
        assert fenced, "旧采集器未按预期被 fence"
        c_left = post("/api/collectors/start", {"shard_id": left})
        c_right = post("/api/collectors/start", {"shard_id": right})
        print(f"  新采集器：{c_left['data']['collector_id']} / {c_right['data']['collector_id']}")
        ev4 = wait_reconciled("拆分后两个子片接管完成")
        report["steps"].append({"step": 4, "name": "split_and_fence",
                                "old_collector_fenced": fenced,
                                "children": [left, right], "evidence": ev4})

        section("步骤 5  分片 B 重叠交接（短暂双跑，验证重复不被当新增）")
        # Deterministically create overlap: stop the current B task, rewind its
        # resume hint so the replacement re-fetches already-stored pages, then run
        # an overlap handoff. The two tasks touch overlapping records; the receiver
        # must count that overlap as duplicates and never as new inserts.
        _, before = http("GET", f"{URLS['receiver']}/stats")
        _, current_b = http("GET", f"{URLS['web']}/api/collectors")
        for c in current_b.get("collectors", []):
            if c["shard_id"] == shard_b and c["alive"]:
                http("POST", f"{URLS['web']}/api/collectors/{c['collector_id']}/stop", {})
        time.sleep(1.0)
        # rewind the (hint-only) cursor so re-scanning re-delivers stored records
        rw_status, rw = http("POST", f"{URLS['scheduler']}/shards/{shard_b}/rewind",
                             {"cursor": 0})
        assert rw_status == 200, f"续点回退失败: {rw}"
        print(f"  分片 B 续点提示回退到 {rw['resume_cursor']}（重抓将产生确定性重复）")
        first = post("/api/collectors/start", {"shard_id": shard_b})
        assert first["data"].get("ok"), first["data"]
        first_id = first["data"]["collector_id"]
        print(f"  重抓采集器启动：{first_id}，立即在其抓取途中重叠交接")
        # overlap-handoff while it is actively fetching the re-wound pages
        handoff = post("/api/handoff", {"shard_id": shard_b, "mode": "overlap"})
        print(f"  交接结果：{json.dumps(handoff['data'], ensure_ascii=False)}")
        ev5 = wait_reconciled("重叠交接后仍然完整")
        _, after = http("GET", f"{URLS['receiver']}/stats")
        dup = after["batches"]["duplicates"] - before["batches"]["duplicates"]
        inserted = after["batches"]["inserted"] - before["batches"]["inserted"]
        print(f"  重叠期：重复提交={dup}，新增={inserted}（去重以记录 ID 为准，重复不计新增）")
        assert dup > 0, "未观察到任何重复投递，重叠场景未真正发生"
        # distinct count must still equal the source despite duplicate deliveries
        assert after["distinct_records"] == ev5["receiver_distinct_records"]
        assert after["distinct_records"] == ev5["source"]["count"]
        report["steps"].append({"step": 5, "name": "overlap_handoff",
                                "handoff": handoff["data"],
                                "duplicates_during_overlap": dup, "evidence": ev5})

        section("步骤 6  合并分片（两个子片合并为一个，重新接管）")
        _, merged = http("POST", f"{URLS['scheduler']}/shards/merge",
                         {"shard_a": left, "shard_b": right})
        merged_id = merged["merged"]["id"]
        print(f"  合并片 {merged_id} 范围={merged['merged']['lo']}..{merged['merged']['hi']}")
        # children's collectors should be fenced soon; start a collector for the merge
        c_merge = post("/api/collectors/start", {"shard_id": merged_id})
        print(f"  合并片采集器：{c_merge['data']['collector_id']}")
        ev6 = wait_reconciled("合并后全量对账")
        report["steps"].append({"step": 6, "name": "merge",
                                "merged_shard": merged_id, "evidence": ev6})

        section("步骤 7  旧进程恢复演练：重放一个已失效令牌，必须被拒收")
        _, shards = http("GET", f"{URLS['scheduler']}/shards")
        # ensure no task currently owns shard_b, then register a throwaway
        # credential, capture its exact token, retire it via handoff, and replay
        _, current = http("GET", f"{URLS['web']}/api/collectors")
        for c in current.get("collectors", []):
            if c["shard_id"] == shard_b and c["alive"]:
                http("POST", f"{URLS['web']}/api/collectors/{c['collector_id']}/stop", {})
                print(f"  先停掉分片 B 现有采集器 {c['collector_id']}")
        time.sleep(1.0)
        victim = post("/api/collectors/start", {"shard_id": shard_b})
        assert victim["data"].get("ok"), f"victim 启动失败: {victim['data']}"
        victim_id = victim["data"]["collector_id"]
        print(f"  一次性 victim 采集器：{victim_id}")
        time.sleep(1.5)
        _, live = http("GET", f"{URLS['web']}/api/collectors")
        victim_row = next(c for c in live["collectors"]
                          if c["collector_id"] == victim_id and c["alive"])
        _, cred_resp = http("GET",
                            f"http://127.0.0.1:{victim_row['control_port']}/credential")
        retired_token = cred_resp["credential"]["token"]
        retired_epoch = cred_resp["credential"]["epoch"]
        # handover retires the epoch
        post("/api/handoff", {"shard_id": shard_b, "mode": "pause"})
        time.sleep(1.0)
        replay_status, replay = http("POST", f"{URLS['receiver']}/ingest", {
            "token": retired_token, "shard_id": shard_b, "epoch": retired_epoch,
            "records": [{"id": 1, "payload": "x"}],
        })
        print(f"  旧令牌重放 → HTTP {replay_status} {replay}")
        assert replay_status == 409 and replay.get("error") == "fenced", "fencing 失效！"
        # also prove the scheduler itself rejects the stale token
        cp_status, cp = http("POST", f"{URLS['scheduler']}/checkpoint",
                             {"token": retired_token, "cursor": 999})
        print(f"  旧令牌提交 checkpoint → HTTP {cp_status} {cp}")
        assert cp_status == 409
        report["steps"].append({"step": 7, "name": "stale_token_replay",
                                "ingest_status": replay_status, "checkpoint_status": cp_status,
                                "retired_token": retired_token[:14] + "…"})

        section("步骤 8  最终对照：来源权威清单 vs 接收库（含请求成本）")
        _, evidence = http("GET", f"{URLS['receiver']}/evidence")
        _, coverage = http("GET", f"{URLS['receiver']}/coverage")
        _, reqlog = http("GET", f"{URLS['source']}/control/requests")
        _, stats = http("GET", f"{URLS['receiver']}/stats")
        report["final_evidence"] = evidence
        report["final_coverage"] = coverage
        report["request_cost_summary"] = reqlog["summary"]
        report["receiver_totals"] = stats
        print("  来源清单：", json.dumps(evidence["source"], ensure_ascii=False))
        print("  接收库  ：", {
            "distinct_records": evidence["receiver_distinct_records"],
            "checksum": evidence["receiver_checksum"],
            "missing": evidence["missing_ids_sample"],
            "extra": evidence["extra_ids_sample"],
        })
        print("  接收批次累计：", json.dumps(stats["batches"], ensure_ascii=False))
        print("  来源真实请求汇总：")
        for row in reqlog["summary"]:
            print(f"    {row['method']} {row['path']} → {row['status']} × {row['n']}")
        assert evidence["reconciled"], "最终全局对账失败"
        # Authority is the GLOBAL source-manifest reconciliation: records are keyed
        # by (shard_id, record_id), so after split/merge older rows remain stored
        # under retired shard ids and a per-shard count is intentionally not
        # expected to be complete. Per-shard coverage is ownership/distribution
        # diagnostics; the global distinct-id + checksum match proves no record is
        # missing and no duplicate was ever counted as new.
        active_ids = {s["id"] for s in shards["shards"] if s["state"] in ("active", "done")}
        print("  当前分片归属与覆盖（诊断）：")
        for c in coverage["coverage"]:
            if c["shard_id"] in active_ids:
                print(f"    {c['shard_id']} 范围{c['range']} 本片名下存储="
                      f"{c['stored_count']}（全局去重后无缺失，见上方清单对账）")
        report["result"] = "PASS"
        print(f"\n{'★' * 72}\n★ 全部断言通过：不缺记录、重复未计新增、过期进度全部被 fence 拒收\n{'★' * 72}")

    except Exception:
        report["result"] = "FAIL"
        import traceback
        report["error"] = traceback.format_exc()
        print("\n演示失败：\n" + report["error"])
        raise
    finally:
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
        print(f"\n报告已写入 {args.report}")
        if not args.keep:
            harness.stop_all()
            print("服务已全部停止（加 --keep 可保留运行，手动浏览 http://127.0.0.1:8080）")
        else:
            print(f"服务保留运行中：网页控制台 {URLS['web']}")


if __name__ == "__main__":
    main()
