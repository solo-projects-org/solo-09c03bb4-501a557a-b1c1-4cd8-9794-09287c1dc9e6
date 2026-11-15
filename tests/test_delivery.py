"""Focused, dependency-light tests for the delivery/outbox guarantees.

Run:  python3 -I tests/test_delivery.py

These exercise the SQLite layer and the receiver store directly (no HTTP), so
they stay fast and deterministic. End-to-end HTTP behaviour is covered by
demo/demo.py.
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "web"))
sys.path.insert(0, str(ROOT))

from app import db  # noqa: E402
from receiver.app import store  # noqa: E402

RESULTS: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    RESULTS.append((ok, label))
    print(f"[{'PASS' if ok else 'FAIL'}] {label}" + (f" -- {detail}" if detail else ""))
    if not ok:
        raise AssertionError(label)


def rows(value: float = 1.0) -> list[dict]:
    return [{"time": f"{h:02d}:00", "nh3n": value, "source_page": 1} for h in range(24)]


def tmp_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    return path


def cleanup(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        Path(path + suffix).unlink(missing_ok=True)


def publish(path: str, section: str, date: str, revision: int, value: float = 1.0) -> None:
    rid = db.start_run(path, section, date, "test")
    db.publish_version(path, section, date, revision, rows(value), "test", rid)


def test_outbox_is_transactional() -> None:
    print("\n== outbox 与发布同事务 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        publish(path, "S", "2026-01-01", 1)
        out = db.get_outbox(path, "S", "2026-01-01", 1)
        check(out is not None and out["status"] == "pending",
              "发布后同一事务留下 pending 待送行")
        check(out["attempts"] == 0 and out["sent_at"] is None,
              "待送行初始未尝试、未发送")

        # A stale publish must roll back entirely: no outbox mutation either.
        rid = db.start_run(path, "S", "2026-01-02", "test")
        db.publish_version(path, "S", "2026-01-02", 2, rows(), "test", rid)
        rid2 = db.start_run(path, "S", "2026-01-02", "test")
        raised = False
        try:
            db.publish_version(path, "S", "2026-01-02", 1, rows(), "test", rid2)
        except db.StaleRevisionError:
            raised = True
        check(raised, "迟到的旧修订发布被拒绝")
        check(db.get_outbox(path, "S", "2026-01-02", 1) is None,
              "被拒绝的发布不会产生待送行")
    finally:
        cleanup(path)


def test_newer_revision_supersedes_older_queue() -> None:
    print("\n== 只有最新本地修订可发送 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        publish(path, "S", "2026-01-01", 1)
        publish(path, "S", "2026-01-01", 2)
        old = db.get_outbox(path, "S", "2026-01-01", 1)
        new = db.get_outbox(path, "S", "2026-01-01", 2)
        check(old["status"] == "superseded", "旧修订被标记 superseded", old["status"])
        check(new["status"] == "pending", "新修订仍是 pending", new["status"])
        pending = db.pending_deliveries(path)
        check([(p["revision"]) for p in pending] == [2],
              "发送队列只含最新修订", str(pending))
        check(db.claim_delivery(path, "S", "2026-01-01", 1, "w1") is None,
              "无法认领已被取代的旧修订")
    finally:
        cleanup(path)


def test_claim_is_exclusive() -> None:
    print("\n== 跨进程发送租约互斥 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        publish(path, "S", "2026-01-01", 1)
        check(db.claim_delivery(path, "S", "2026-01-01", 1, "w1") == 1,
              "w1 成功认领")
        check(db.claim_delivery(path, "S", "2026-01-01", 1, "w2") is None,
              "w2 在租约内无法重复认领")
        db.mark_delivery_failed(path, "S", "2026-01-01", 1, "w1", 1, "network down")
        check(db.claim_delivery(path, "S", "2026-01-01", 1, "w2") == 2,
              "失败后 w2 可接手并累计尝试次数")
        db.mark_delivery_sent(path, "S", "2026-01-01", 1, "w2", 2, {"status": "created"})
        check(db.claim_delivery(path, "S", "2026-01-01", 1, "w1") is None,
              "已发送后不可再次认领")
        check(db.get_outbox(path, "S", "2026-01-01", 1)["status"] == "sent",
              "发送成功状态已持久化")
    finally:
        cleanup(path)


def test_concurrent_claims_one_winner() -> None:
    print("\n== 并发认领只有一个赢家 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        publish(path, "S", "2026-01-01", 1)
        results: list[int | None] = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker(i: int) -> None:
            barrier.wait()
            got = db.claim_delivery(path, "S", "2026-01-01", 1, f"w{i}")
            with lock:
                results.append(got)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        winners = [r for r in results if r is not None]
        check(len(results) == 8 and len(winners) == 1,
              "8 个并发认领中恰有 1 个成功", str(results))
    finally:
        cleanup(path)


def test_failed_send_keeps_published() -> None:
    print("\n== 发送失败不撤销本地已发布日档 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        publish(path, "S", "2026-01-01", 1, value=2.5)
        claimed = db.claim_delivery(path, "S", "2026-01-01", 1, "w1")
        check(claimed == 1, "认领发送")
        db.mark_delivery_failed(path, "S", "2026-01-01", 1, "w1", 1,
                                "connection refused")
        published = db.get_published(path, "S", "2026-01-01")
        check(published is not None and published["revision"] == 1
              and len(published["rows"]) == 24,
              "失败后完整日档仍在")
        check(published["rows"][0]["nh3n"] == 2.5, "数值未被改动")
        out = db.get_outbox(path, "S", "2026-01-01", 1)
        check(out["status"] == "failed" and "refused" in out["last_error"],
              "失败原因记录在投递行")
    finally:
        cleanup(path)


def test_restart_releases_claim_and_recovers() -> None:
    print("\n== 进程重启后租约释放、待送补发 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        publish(path, "S", "2026-01-01", 1)
        db.claim_delivery(path, "S", "2026-01-01", 1, "web1")
        # Simulate web1 being killed mid-send and restarted under the same name.
        released = db.release_worker_claims(path, "web1")
        check(released == 1, "重启的 worker 释放自己的未完成租约", str(released))
        out = db.get_outbox(path, "S", "2026-01-01", 1)
        check(out["status"] == "failed" and out["claimed_by"] is None,
              "租约释放后回到可重试状态")
        check(db.claim_delivery(path, "S", "2026-01-01", 1, "web2") == 2,
              "另一实例可接手补送")
    finally:
        cleanup(path)


def test_restart_does_not_interrupt_other_worker() -> None:
    print("\n== 重启 web2 不影响 web1 的活动任务 ==")
    path = tmp_db()
    try:
        db.init_db(path)
        rid = db.start_run(path, "S", "2026-01-01", "web1")
        db.recover_interrupted(path, worker="web2")
        check(db.get_run(path, rid)["status"] == "running",
              "web2 重启不会把 web1 的 running 任务标为 interrupted")
        db.recover_interrupted(path, worker="web1")
        check(db.get_run(path, rid)["status"] == "interrupted",
              "web1 自身重启时恢复其遗留任务")
    finally:
        cleanup(path)


def test_receiver_guards() -> None:
    print("\n== 接收端完整日档约束 ==")
    path = tmp_db()
    try:
        store.init_db(path)
        base_rows = rows(1.0)
        payload = {
            "section": "S", "date": "2026-01-01", "revision": 2,
            "complete": True, "row_count": 24,
            "digest": store.content_digest("S", "2026-01-01", 2, base_rows),
            "rows": base_rows,
        }
        result = store.receive_day(path, payload)
        check(result["status"] == "created", "接收完整日档", str(result))
        check(store.get_day(path, "S", "2026-01-01")["row_count"] == 24,
              "只公布完整 24 行")

        check(store.receive_day(path, payload)["status"] == "duplicate",
              "同版同内容重送幂等")
        check(len(store.get_day(path, "S", "2026-01-01")["rows"]) == 24,
              "幂等重送不产生重复行")

        stale = dict(payload, revision=1,
                     digest=store.content_digest("S", "2026-01-01", 1, base_rows))
        try:
            store.receive_day(path, stale)
            check(False, "旧修订应被拒绝")
        except store.DeliveryRejected as exc:
            check(exc.error == "stale_revision", "旧修订被拒绝", exc.error)
        check(store.get_day(path, "S", "2026-01-01")["revision"] == 2,
              "拒绝后仍保留较新修订")

        conflict = dict(payload, digest=store.content_digest(
            "S", "2026-01-01", 2, rows(9.9)), rows=rows(9.9))
        try:
            store.receive_day(path, conflict)
            check(False, "同修订不同内容应被拒绝")
        except store.DeliveryRejected as exc:
            check(exc.error == "revision_conflict", "同版不同内容被拒绝为冲突", exc.error)
        check(store.get_day(path, "S", "2026-01-01")["rows"][0]["nh3n"] == 1.0,
              "冲突内容未写入")

        partial_rows = base_rows[:23]
        partial = {
            "section": "S", "date": "2026-01-02", "revision": 1,
            "complete": True, "row_count": 23,
            "digest": store.content_digest("S", "2026-01-02", 1, partial_rows),
            "rows": partial_rows,
        }
        try:
            store.receive_day(path, partial)
            check(False, "不完整日档应被拒绝")
        except store.DeliveryRejected as exc:
            check(exc.error == "incomplete_day", "不完整日档被拒绝", exc.error)
        check(store.get_day(path, "S", "2026-01-02") is None,
              "不完整日档永远查不到")

        bad_digest = dict(payload, revision=3, digest="0" * 64)
        try:
            store.receive_day(path, bad_digest)
            check(False, "摘要不符应被拒绝")
        except store.DeliveryRejected as exc:
            check(exc.error == "digest_mismatch", "摘要不符被拒绝", exc.error)
    finally:
        cleanup(path)


def test_digest_parity() -> None:
    print("\n== 收发两端摘要算法一致 ==")
    sample = [
        {"time": "00:00", "nh3n": 0.1, "source_page": 1},
        {"time": "01:00", "nh3n": 0.2, "source_page": 2},
    ]
    a = db.delivery_digest("S", "2026-01-01", 3, sample)
    b = store.content_digest("S", "2026-01-01", 3, list(reversed(sample)))
    check(a == b, "同一日档（行序无关）摘要一致")
    c = db.delivery_digest("S", "2026-01-01", 4, sample)
    check(a != c, "不同修订号摘要不同")


def main() -> int:
    tests = [
        test_outbox_is_transactional,
        test_newer_revision_supersedes_older_queue,
        test_claim_is_exclusive,
        test_concurrent_claims_one_winner,
        test_failed_send_keeps_published,
        test_restart_releases_claim_and_recovers,
        test_restart_does_not_interrupt_other_worker,
        test_receiver_guards,
        test_digest_parity,
    ]
    try:
        for test in tests:
            test()
    except Exception:
        traceback.print_exc()
        print("\n测试中断")
        return 1
    passed = sum(1 for ok, _ in RESULTS if ok)
    print(f"\n==== 结果: {passed}/{len(RESULTS)} 项检查通过 ====")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
