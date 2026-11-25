"""runner 端到端：示例文件全量回放、回执序列与非法消息隔离。"""

import json
from pathlib import Path
import unittest

from jobqueue.runner import run_events, run_file

EXAMPLES = Path(__file__).parent / "examples"
KINDS = set(json.loads((EXAMPLES / "message_types.json").read_text(
    encoding="utf-8")))


class RunnerTest(unittest.TestCase):
    def test_each_example_produces_receipt_per_event(self):
        for path in sorted(EXAMPLES.glob("*.jsonl")):
            with self.subTest(path=path.name):
                engine, receipts = run_file(path, KINDS)
                lines = path.read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(receipts), len(lines))
                self.assertTrue(all(r["status"] != "invalid" for r in receipts))

    def test_urgent_scenario_key_decisions(self):
        _, receipts = run_file(
            EXAMPLES / "urgent-after-first-start.jsonl", KINDS)
        starts = [r for r in receipts if r["kind"] == "start"]
        # t7 台位忙、t12 旧批号拆批失效
        reasons = {(r["at"], r["detail"]["reason"]) for r in starts
                   if r["status"] == "rejected"}
        self.assertIn((7, "bench_busy"), reasons)
        self.assertIn((12, "batch_split"), reasons)
        # t8 的 pull 完成拆批
        reroute = next(r for r in receipts
                       if r["kind"] == "pull" and r["detail"]["batch"] == "reroute")
        self.assertEqual(reroute["detail"]["rerouted"], ["b@1"])
        # 重启后 finish 仍是有效事实
        finish = next(r for r in receipts if r["kind"] == "finish")
        self.assertEqual(finish["status"], "accepted")
        self.assertTrue(finish["detail"]["passed"])

    def test_invalid_message_is_quarantined_not_fatal(self):
        events = [
            {"at": 0, "kind": "bench",
             "record": {"bench": "b1", "fixture": "f",
                        "skills": ["s"], "duration": 2}},
            {"at": 1, "kind": "job",
             "record": {"id": "X", "requires": [["ghost@1"]]}},
            {"at": 2, "kind": "item",
             "record": {"item": "a", "rev": 1, "skill": "s",
                        "fixture": "f", "urgency": "regular", "deadline": 9}},
        ]
        engine, receipts = run_events(events)
        self.assertEqual(receipts[1]["status"], "invalid")
        # 非法消息之后引擎仍继续处理
        self.assertIn("a@1", engine.items)

    def test_shared_scenario_amend_blocks_old_verdict(self):
        engine, receipts = run_file(
            EXAMPLES / "shared-work-revision.jsonl", KINDS)
        # seal@1 合格但 B 已修订到 check@2，没有工单被它放行
        finish = next(r for r in receipts if r["kind"] == "finish")
        self.assertEqual(finish["detail"]["released"], [])
        self.assertEqual(engine.jobs["B"].state, "open")
        # A 取消后事实仍在
        self.assertEqual(engine.items["seal@1"].state, "passed")
        self.assertEqual(engine.jobs["A"].state, "cancelled")


if __name__ == "__main__":
    unittest.main()
