"""把演练记录（JSONL）送入作业队列并输出回执。

用法::

    python3 -m jobqueue.runner examples/urgent-after-first-start.jsonl
    python3 -m jobqueue.runner examples --pretty
"""

import argparse
import json
import sys
from pathlib import Path

from scenario_reader import load_events

from .engine import Engine, QueueReject


def run_events(events):
    """顺序投递事件，返回 (引擎, 回执列表)。"""
    engine = Engine()
    receipts = []
    for event in events:
        try:
            receipt = engine.handle(event["at"], event["kind"], event["record"])
        except QueueReject as error:
            receipts.append({
                "seq": len(receipts) + 1, "at": event["at"],
                "kind": event["kind"], "status": "invalid",
                "detail": {"error": str(error)},
            })
            continue
        receipts.append({
            "seq": receipt.seq, "at": receipt.at, "kind": receipt.kind,
            "status": receipt.status, "detail": receipt.detail,
        })
    return engine, receipts


def run_file(path, kinds):
    events = list(load_events(path, kinds))
    return run_events(events)


def main(argv=None):
    parser = argparse.ArgumentParser(description="运行维修作业队列演练")
    parser.add_argument("path", type=Path, help="JSONL 文件或包含它的目录")
    parser.add_argument("--kinds", type=Path, default=None,
                        help="message_types.json，默认取演练目录下的文件")
    parser.add_argument("--pretty", action="store_true", help="缩进输出")
    args = parser.parse_args(argv)

    target = args.path
    if target.is_dir():
        kinds_path = args.kinds or target / "message_types.json"
        files = sorted(target.glob("*.jsonl"))
    else:
        kinds_path = args.kinds or target.parent / "message_types.json"
        files = [target]
    kinds = set(json.loads(kinds_path.read_text(encoding="utf-8")))

    indent = 2 if args.pretty else None
    for path in files:
        _, receipts = run_file(path, kinds)
        for row in receipts:
            print(json.dumps(row, ensure_ascii=False, indent=indent))
        print(f"# {path.name}: {len(receipts)} 条回执", file=sys.stderr)


if __name__ == "__main__":
    main()
