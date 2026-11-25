"""作业队列的领域不变量测试。

覆盖：合批与拆批、急检插队、台位归属钉住、release 移交、
修订与共享事实、违约留痕、重启幂等、等待原因区分、候选/配对语义。
"""

import unittest

from jobqueue import Engine, QueueReject


def bench(rec_id, fixture="f", skills=("inspect",), duration=4):
    return {"bench": rec_id, "fixture": fixture,
            "skills": list(skills), "duration": duration}


def item(item_id, rev=1, skill="inspect", fixture="f",
         urgency="regular", deadline=50, depends=None):
    rec = {"item": item_id, "rev": rev, "skill": skill, "fixture": fixture,
           "urgency": urgency, "deadline": deadline}
    if depends:
        rec["depends"] = depends
    return rec


def job(jid, requires):
    return {"id": jid, "requires": requires}


class QueueTestBase(unittest.TestCase):
    def setUp(self):
        self.engine = Engine()
        self.t = 0

    def send(self, kind, record, at=None):
        if at is not None:
            self.t = max(at, self.t)
        receipt = self.engine.handle(self.t, kind, record)
        self.t += 1
        return receipt

    def statuses(self, kind):
        return [(r.detail.get("work") or r.detail.get("id") or r.detail.get("batch"),
                 r.status, r.detail.get("reason"))
                for r in self.engine.ledger if r.kind == kind]


class BatchingTest(QueueTestBase):
    def test_same_fixture_tasks_batch_in_priority_order(self):
        self.send("bench", bench("b1", duration=4))
        self.send("item", item("lo", deadline=40))
        self.send("item", item("hi", urgency="due", deadline=20))
        receipt = self.send("pull", {"bench": "b1", "batch": "g1", "max": 5})
        self.assertEqual(receipt.detail["assigned"], ["hi@1", "lo@1"])

    def test_batch_rejected_when_own_deadline_missed(self):
        # 台位上的长队列会让晚截止的工作无法在期限前开工
        self.send("bench", bench("b1", duration=10))
        for i, d in enumerate((12, 22, 32)):
            self.send("item", item(f"w{i}", deadline=d))
        receipt = self.send("pull", {"bench": "b1", "batch": "g1", "max": 5})
        # w0 从 t3 开工，w1 在 13、w2 在 23；w2 截止 32 仍可行 -> 三个都入批
        self.assertEqual(receipt.detail["assigned"], ["w0@1", "w1@1", "w2@1"])
        tight = self.send("item", item("tight", deadline=25))
        receipt = self.send("pull", {"bench": "b1", "batch": "g2", "max": 5})
        # tight 的投影开工时刻为 33，晚于截止 25，被跳过
        self.assertIn(
            ("tight@1", "own_deadline"),
            [(s["work"], s["reason"]) for s in receipt.detail["skipped"]])

    def test_more_urgent_waiting_work_is_not_squeezed(self):
        self.send("bench", bench("b1", duration=8))
        self.send("bench", bench("b2", duration=8))
        self.send("item", item("reg", deadline=40))
        # b1 先把常规件领走并开工
        first = self.send("pull", {"bench": "b1", "batch": "p1", "max": 5})
        self.assertEqual(first.detail["assigned"], ["reg@1"])
        self.send("start", {"bench": "b1", "batch": "p1", "work": "reg@1"})
        # 急检件随后到池，两台都能做；b2 直接领取，b1 已开始的工作不被拆
        self.send("item", item("urg", urgency="stopped", deadline=10))
        second = self.send("pull", {"bench": "b2", "batch": "p2", "max": 5})
        self.assertEqual(second.detail["assigned"], ["urg@1"])
        self.assertEqual(self.engine.items["reg@1"].bench, "b1")
        self.assertEqual(self.engine.items["reg@1"].state, "started")


class SplitAndOwnershipTest(QueueTestBase):
    def test_urgent_split_revokes_old_batch_for_unstarted_piece(self):
        self.send("bench", bench("b1", duration=5))
        self.send("bench", bench("b2", duration=4))
        self.send("item", item("a", deadline=30))
        self.send("item", item("b", deadline=33))
        self.send("job", job("repair", [["a@1"], ["b@1"]]))
        self.send("pull", {"bench": "b1", "batch": "first", "max": 2})
        self.send("start", {"bench": "b1", "batch": "first", "work": "a@1"})
        self.send("item", item("u", urgency="stopped", deadline=12))
        self.send("job", job("down", [["u@1"]]))
        self.send("pull", {"bench": "b1", "batch": "urgent", "max": 1})
        reroute = self.send("pull", {"bench": "b2", "batch": "reroute", "max": 1})
        self.assertEqual(reroute.detail["rerouted"], ["b@1"])
        # 已开始的 a 仍属 b1；未开始的 b 已拆给 b2
        self.assertEqual(self.engine.items["a@1"].bench, "b1")
        self.assertEqual(self.engine.items["b@1"].bench, "b2")
        # 处理端拿旧批号申请被拆走的部件 -> 拒绝
        stale = self.send(
            "start", {"bench": "b1", "batch": "first", "work": "b@1"})
        self.assertEqual(stale.status, "rejected")
        self.assertEqual(stale.detail["reason"], "batch_split")
        # 新批号可以开工
        ok = self.send(
            "start", {"bench": "b2", "batch": "reroute", "work": "b@1"})
        self.assertEqual(ok.status, "accepted")

    def test_started_work_pinned_until_original_bench_releases(self):
        self.send("bench", bench("b1"))
        self.send("bench", bench("b2"))
        self.send("item", item("a", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("start", {"bench": "b1", "batch": "p", "work": "a@1"})
        # b2 无法把已开始的工作拆走：可移动列表里没有它
        self.send("item", item("c", deadline=40))
        pull = self.send("pull", {"bench": "b2", "batch": "q", "max": 5})
        self.assertNotIn("a@1", pull.detail["rerouted"])
        # 没有停止声明时 release 已开始的工作被拒
        bad = self.send("release", {"bench": "b1", "work": "a@1"})
        self.assertEqual(bad.detail["reason"],
                         "started_work_needs_stop_confirmation")
        # 原台确认停止后，工作回到可领取池
        good = self.send("release",
                         {"bench": "b1", "work": "a@1", "note": "stopped"})
        self.assertTrue(good.detail["returned_to_pool"])
        self.assertIsNone(self.engine.items["a@1"].bench)
        again = self.send("pull", {"bench": "b2", "batch": "r", "max": 1})
        self.assertEqual(again.detail["assigned"], ["a@1"])

    def test_release_unstarted_returns_to_pool_and_revokes_batch(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        receipt = self.send("release", {"bench": "b1", "work": "a@1"})
        self.assertEqual(receipt.status, "accepted")
        self.assertNotIn("a@1", self.engine.batches["p"].active_works)
        self.assertIn("a@1", self.engine.batches["p"].revoked)

    def test_stopped_batch_is_never_rerouted(self):
        self.send("bench", bench("b1"))
        self.send("bench", bench("b2"))
        self.send("item", item("u", urgency="stopped", deadline=12))
        self.send("job", job("down", [["u@1"]]))
        self.send("pull", {"bench": "b1", "batch": "urgent", "max": 1})
        # b2 即使空闲也不能拆走 b1 急检批次中未开始的工作
        pull = self.send("pull", {"bench": "b2", "batch": "grab", "max": 1})
        self.assertEqual(pull.detail["rerouted"], [])
        self.assertEqual(self.engine.items["u@1"].bench, "b1")

    def test_bench_busy_rejects_second_start(self):
        self.send("bench", bench("b1", duration=5))
        self.send("item", item("a", deadline=30))
        self.send("item", item("b", deadline=33))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 2})
        self.send("start", {"bench": "b1", "batch": "p", "work": "a@1"})
        busy = self.send("start", {"bench": "b1", "batch": "p", "work": "b@1"})
        self.assertEqual(busy.detail["reason"], "bench_busy")


class SharedFactAndRevisionTest(QueueTestBase):
    def test_one_result_serves_two_jobs_and_cancel_keeps_fact(self):
        self.send("bench", bench("b1", skills=["test"]))
        self.send("item", item("seal", skill="test"))
        self.send("job", job("A", [["seal@1"]]))
        self.send("job", job("B", [["seal@1"]]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("start", {"bench": "b1", "batch": "p", "work": "seal@1"})
        self.send("cancel", {"id": "A"})
        finish = self.send(
            "finish", {"bench": "b1", "batch": "p",
                       "work": "seal@1", "passed": True})
        # 事实保留，B 仍被这份结果放行
        self.assertEqual([r["id"] for r in finish.detail["released"]], ["B"])
        inspect = self.send("inspect", {"item": "seal@1"})
        # 两个工单的引用都可追查；A 已取消且不再是当前要求，B 仍生效
        users = {u["job"]: u for u in inspect.detail["used_by"]}
        self.assertEqual(set(users), {"A", "B"})
        self.assertEqual(users["A"]["state"], "cancelled")
        self.assertFalse(users["A"]["current"])
        self.assertTrue(users["B"]["current"])

    def test_old_verdict_does_not_release_amended_requirement(self):
        self.send("bench", bench("b1", skills=["test", "review"]))
        self.send("item", item("seal", skill="test"))
        self.send("item", item("check", skill="review",
                               depends=["seal@1"], deadline=60))
        self.send("item", item("seal", rev=2, skill="test", deadline=60))
        self.send("item", item("check", rev=2, skill="review",
                               depends=["seal@2"], deadline=60))
        self.send("job", job("B", [["check@1"]]))
        self.send("amend", {"id": "B", "requires": [["check@2"]]})
        # 旧修订全部合格也不能放行新修订要求
        self.send("pull", {"bench": "b1", "batch": "old", "max": 4})
        for work in ("seal@1", "check@1"):
            self.send("start", {"bench": "b1", "batch": "old", "work": work})
            self.send("finish", {"bench": "b1", "batch": "old",
                                 "work": work, "passed": True})
        inspect = self.send("inspect", {"job": "B"})
        self.assertEqual(inspect.detail["state"], "open")
        waiting = inspect.detail["waiting"][0][0]
        self.assertEqual(waiting["ref"], "check@2")
        self.assertEqual(waiting["wait"], "dependency")
        # 历史快照可供追查
        self.assertEqual(len(inspect.detail["history"]), 2)

    def test_duplicate_finish_after_restart_is_idempotent_fact(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        self.send("job", job("J", [["a@1"]]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("start", {"bench": "b1", "batch": "p", "work": "a@1"})
        self.send("restart", {})
        first = self.send("finish", {"bench": "b1", "batch": "p",
                                     "work": "a@1", "passed": True})
        self.assertEqual(first.status, "accepted")
        # 重启语义已在 restart 内重放；再来一次重放并重复回报
        rebuilt = Engine.replay(self.engine.events)
        dup = rebuilt.handle(self.t, "finish",
                             {"bench": "b1", "batch": "p",
                              "work": "a@1", "passed": True})
        self.assertEqual(dup.status, "duplicate")
        self.assertTrue(dup.detail["same_passed"])
        self.assertTrue(dup.detail["same_bench"])
        # 冲突判定只入历史，不覆盖事实
        conflict = rebuilt.handle(self.t + 1, "finish",
                                  {"bench": "b1", "batch": "p",
                                   "work": "a@1", "passed": False})
        self.assertEqual(conflict.status, "duplicate")
        self.assertFalse(conflict.detail["same_passed"])
        self.assertTrue(rebuilt.items["a@1"].passed)


class BreachTest(QueueTestBase):
    def test_missed_start_keeps_reason_and_late_pass_does_not_clear_it(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=5))
        self.send("job", job("J", [["a@1"]]))
        # 期限内领取并可开工，但没有真的开工
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1}, at=5)
        self.assertIsNone(self.engine.items["a@1"].breach_reason)
        # 时间越过截止点仍未开工 -> 违约留痕
        self.send("inspect", {}, at=9)
        work = self.engine.items["a@1"]
        self.assertEqual(work.breach_reason, "deadline_passed_without_start")
        # 违约锁定：调度不再允许开工
        locked = self.send("start",
                           {"bench": "b1", "batch": "p", "work": "a@1"})
        self.assertEqual(locked.detail["reason"], "breach_locked")
        # 即使绕过队列直接制造合格，违约记录仍然保留
        work.state = "started"
        self.engine.benches["b1"].current = "a@1"
        self.send("finish", {"bench": "b1", "batch": "p",
                             "work": "a@1", "passed": True})
        self.assertIsNotNone(self.engine.items["a@1"].breach_reason)
        inspect = self.send("inspect", {"job": "J"})
        # 结果能在事实层面放行，但带着违约标记
        self.assertEqual(inspect.detail["state"], "released")
        self.assertIn("a@1", inspect.detail["released_by"]["with_breach"])

    def test_starting_before_deadline_avoids_breach(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=5))
        # 截止点同秒领取并开工（同秒内按行序，与输入契约一致）
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1}, at=5)
        self.t -= 1  # 下一条消息保持在 t=5
        ok = self.send("start",
                       {"bench": "b1", "batch": "p", "work": "a@1"}, at=5)
        self.assertEqual(ok.status, "accepted")
        self.assertIsNone(self.engine.items["a@1"].breach_reason)

class WaitReasonTest(QueueTestBase):
    def test_distinguishes_capability_dependency_and_free_bench(self):
        self.send("bench", bench("b1", fixture="f8", skills=("inspect",)))
        self.send("item", item("base", fixture="f8", deadline=40))
        self.send("item", item("child", fixture="f8", deadline=40,
                               depends=["base@1"]))
        self.send("item", item("other_fixture", fixture="g2", deadline=40))
        reason_child, _ = self.engine._waiting_reason(
            self.engine.items["child@1"])
        reason_g2, extra_g2 = self.engine._waiting_reason(
            self.engine.items["other_fixture@1"])
        reason_base, extra_base = self.engine._waiting_reason(
            self.engine.items["base@1"])
        self.assertEqual(reason_child, "dependency")
        self.assertEqual(reason_g2, "no_capability")
        self.assertEqual(extra_g2["fixture"], "g2")
        self.assertEqual(reason_base, "eligible")
        # 台位被占后，原因变为等待空闲台位
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("start", {"bench": "b1", "batch": "p", "work": "base@1"})
        reason_child2, _ = self.engine._waiting_reason(
            self.engine.items["child@1"])
        self.assertEqual(reason_child2, "dependency")  # 依赖仍优先
        self.send("item", item("free_one", fixture="f8", deadline=40))
        reason_busy, extra_busy = self.engine._waiting_reason(
            self.engine.items["free_one@1"])
        self.assertEqual(reason_busy, "waiting_bench")
        self.assertEqual(extra_busy["benches"], ["b1"])

    def test_failed_dependency_blocks_and_is_explained(self):
        self.send("bench", bench("b1"))
        self.send("item", item("base", deadline=40))
        self.send("item", item("child", deadline=40, depends=["base@1"]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 2})
        self.send("start", {"bench": "b1", "batch": "p", "work": "base@1"})
        self.send("finish", {"bench": "b1", "batch": "p",
                             "work": "base@1", "passed": False})
        reason, extra = self.engine._waiting_reason(
            self.engine.items["child@1"])
        self.assertEqual(reason, "dependency_failed")
        self.assertEqual(extra["failed"], ["base@1"])


class CandidateAndPairTest(QueueTestBase):
    def test_alternative_group_needs_only_one_passing_candidate(self):
        self.send("bench", bench("b1", skills=("inspect",)))
        self.send("item", item("spare1"))
        self.send("item", item("spare2"))
        self.send("job", job("J", [["spare1@1", "spare2@1"]]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 2})
        self.send("start", {"bench": "b1", "batch": "p", "work": "spare1@1"})
        finish = self.send("finish", {"bench": "b1", "batch": "p",
                                      "work": "spare1@1", "passed": True})
        self.assertEqual([r["id"] for r in finish.detail["released"]], ["J"])

    def test_mandatory_pair_requires_both_results(self):
        self.send("bench", bench("b1", skills=("inspect",)))
        self.send("item", item("left"))
        self.send("item", item("right"))
        # 两个单元素组 = 与关系，即必须配成一组
        self.send("job", job("J", [["left@1"], ["right@1"]]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 2})
        self.send("start", {"bench": "b1", "batch": "p", "work": "left@1"})
        first = self.send("finish", {"bench": "b1", "batch": "p",
                                     "work": "left@1", "passed": True})
        self.assertEqual(first.detail["released"], [])
        self.send("start", {"bench": "b1", "batch": "p", "work": "right@1"})
        second = self.send("finish", {"bench": "b1", "batch": "p",
                                      "work": "right@1", "passed": True})
        self.assertEqual([r["id"] for r in second.detail["released"]], ["J"])


class InspectionAndValidationTest(QueueTestBase):
    def test_global_inspect_lists_owner_and_reason(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        self.send("item", item("b", fixture="zz", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        view = self.send("inspect", {})
        entries = {e["work"]: e for e in view.detail["unstarted"]}
        self.assertEqual(entries["a@1"]["owner"], "b1")
        self.assertEqual(entries["b@1"]["wait_reason"], "no_capability")

    def test_unknown_reference_is_invalid_message(self):
        with self.assertRaises(QueueReject):
            self.send("job", job("X", [["ghost@9"]]))

    def test_unknown_batch_start_rejected(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        receipt = self.send(
            "start", {"bench": "b1", "batch": "nope", "work": "a@1"})
        self.assertEqual(receipt.detail["reason"], "unknown_batch")

    def test_duplicate_entity_registration_rejected(self):
        self.send("bench", bench("b1"))
        again = self.send("bench", bench("b1"))
        self.assertEqual(again.status, "rejected")
        self.send("item", item("a"))
        again_item = self.send("item", item("a"))
        self.assertEqual(again_item.status, "rejected")

    def test_finish_from_non_owner_rejected(self):
        self.send("bench", bench("b1"))
        self.send("bench", bench("b2"))
        self.send("item", item("a", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("start", {"bench": "b1", "batch": "p", "work": "a@1"})
        bad = self.send("finish", {"bench": "b2", "batch": "p",
                                   "work": "a@1", "passed": True})
        self.assertEqual(bad.status, "rejected")
        self.assertEqual(bad.detail["reason"], "belongs_other_bench")

    def test_replay_rebuilds_identical_state(self):
        self.send("bench", bench("b1", duration=5))
        self.send("bench", bench("b2", duration=4))
        self.send("item", item("a", deadline=30))
        self.send("item", item("u", urgency="stopped", deadline=12))
        self.send("job", job("J", [["a@1"], ["u@1"]]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 2})
        self.send("start", {"bench": "b1", "batch": "p", "work": "a@1"})
        self.send("restart", {})
        rebuilt = Engine.replay(self.engine.events)
        self.assertEqual(set(rebuilt.items), set(self.engine.items))
        self.assertEqual(rebuilt.items["a@1"].state, "started")
        self.assertEqual(rebuilt.items["a@1"].bench, "b1")
        self.assertEqual(rebuilt.benches["b1"].current, "a@1")

    def test_amend_reopens_released_job_when_new_requirement_unmet(self):
        self.send("bench", bench("b1", skills=("test",)))
        self.send("item", item("v1", skill="test"))
        self.send("item", item("v2", rev=2, skill="test"))
        self.send("job", job("J", [["v1@1"]]))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("start", {"bench": "b1", "batch": "p", "work": "v1@1"})
        self.send("finish", {"bench": "b1", "batch": "p",
                             "work": "v1@1", "passed": True})
        self.assertEqual(self.engine.jobs["J"].state, "released")
        # 新修订要求 v2：旧判定不再放开，工单重新等待
        amend = self.send("amend", {"id": "J", "requires": [["v2@2"]]})
        self.assertTrue(amend.detail["reopened"])
        self.assertIsNone(amend.detail["released"])
        self.assertEqual(self.engine.jobs["J"].state, "open")
        # v2 合格后重新放行
        self.send("pull", {"bench": "b1", "batch": "q", "max": 1})
        self.send("start", {"bench": "b1", "batch": "q", "work": "v2@2"})
        finish = self.send("finish", {"bench": "b1", "batch": "q",
                                      "work": "v2@2", "passed": True})
        self.assertEqual([r["id"] for r in finish.detail["released"]], ["J"])

    def test_restart_event_logged_once_and_state_rebuilds(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("restart", {})
        restart_events = [e for e in self.engine.events
                          if e["kind"] == "restart"]
        self.assertEqual(len(restart_events), 1)
        # 再重放一次也不会产生重复 restart 事件
        rebuilt = Engine.replay(self.engine.events)
        rebuilt_restarts = [e for e in rebuilt.events
                            if e["kind"] == "restart"]
        self.assertEqual(len(rebuilt_restarts), 1)
        self.assertEqual(rebuilt.items["a@1"].bench, "b1")

    def test_amend_rejected_after_cancel_but_history_kept(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a"))
        self.send("item", item("b"))
        self.send("job", job("J", [["a@1"]]))
        self.send("cancel", {"id": "J"})
        rejected = self.send("amend", {"id": "J", "requires": [["b@1"]]})
        self.assertEqual(rejected.status, "rejected")
        self.assertEqual(rejected.detail["reason"], "job_cancelled")
        self.assertEqual(self.engine.jobs["J"].requires, [["a@1"]])
        self.assertEqual(len(self.engine.jobs["J"].history), 1)

    def test_bench_without_capability_cannot_pull_or_reroute(self):
        self.send("bench", bench("b1", fixture="f8"))
        self.send("bench", bench("b3", fixture="g2"))
        self.send("item", item("a", fixture="f8", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        pull = self.send("pull", {"bench": "b3", "batch": "q", "max": 1})
        self.assertEqual(pull.detail["assigned"], [])
        self.assertEqual(pull.detail["rerouted"], [])

    def test_release_absent_operation_is_audited(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        self.send("release", {"bench": "b1", "work": "a@1",
                              "note": "never_touched"})
        self.assertIn("a@1", self.engine.benches["b1"].absent)
        # 释放后任何台位都可重新领取
        ok = self.send("pull", {"bench": "b1", "batch": "q", "max": 1})
        self.assertEqual(ok.detail["assigned"], ["a@1"])

    def test_finish_on_never_started_work_rejected(self):
        self.send("bench", bench("b1"))
        self.send("item", item("a", deadline=30))
        self.send("pull", {"bench": "b1", "batch": "p", "max": 1})
        bad = self.send("finish", {"bench": "b1", "batch": "p",
                                   "work": "a@1", "passed": True})
        self.assertEqual(bad.detail["reason"], "not_started_assigned")


if __name__ == "__main__":
    unittest.main()
