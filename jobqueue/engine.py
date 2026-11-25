"""作业队列引擎。

状态约定
========

* 一个 ``item@rev`` 是一项具体工作（某修订版本的检验/复核/放行）。
* 工作的归属分两层：批次（batch）记录谁领取过，状态机记录它走到哪一步。
  未开始的工作可以从批次中拆出另派；一旦开始，工作钉在原台，
  只有原台 ``release`` 确认未保留或已停止后才会回到可领取池。
* 检验结果按 ``item@rev`` 保存为共享事实，工单只持有引用。
  多个工单可以引用同一份结果；取消其中一个不删除事实。
* 超过最迟开始时间仍未开工的工作记录违约原因，该记录不被后来的结果改写。
"""

from dataclasses import dataclass, field

URGENCY_RANK = {"stopped": 0, "due": 1, "regular": 2}


class QueueReject(Exception):
    """输入格式或前置条件不满足。

    正常的业务拒绝（重复回报、批号失效等）走回执 status=rejected，
    本异常只用于消息本身不合法，例如引用了不存在的工作。
    """


def parse_ref(ref):
    """把 ``item@rev`` 拆成 (item, rev)。"""
    if not isinstance(ref, str) or ref.count("@") != 1:
        raise QueueReject(f"引用必须形如 item@rev：{ref!r}")
    item, rev_text = ref.split("@")
    if not item:
        raise QueueReject(f"引用缺少项目名：{ref!r}")
    try:
        rev = int(rev_text)
    except ValueError as exc:
        raise QueueReject(f"修订号必须是整数：{ref!r}") from exc
    if rev < 1:
        raise QueueReject(f"修订号从 1 开始：{ref!r}")
    return item, rev


def ref_of(item, rev):
    return f"{item}@{rev}"


@dataclass
class ItemRev:
    item: str
    rev: int
    skill: str
    fixture: str
    urgency: str
    deadline: int
    depends: list  # ["other@2", ...]
    # 状态机：waiting -> assigned -> started -> passed / failed
    state: str = "waiting"
    bench: str | None = None
    batch: str | None = None
    started_at: int | None = None
    finished_at: int | None = None
    passed: bool | None = None
    # 未按期开工的留痕（永久保留，结果不能改写）
    breach_reason: str | None = None
    breach_at: int | None = None
    # 曾被拆批的历史，便于追查“旧批号为何失效”
    split_from: list = field(default_factory=list)
    released: bool = False


@dataclass
class Batch:
    batch: str
    bench: str
    created_at: int
    works: list = field(default_factory=list)          # 曾经/当前在批内的 item@rev
    active_works: list = field(default_factory=list)   # 仍归属本批的未开始项
    revoked: list = field(default_factory=list)         # 被拆出的 item@rev


@dataclass
class Bench:
    bench: str
    fixture: str
    skills: list
    duration: int
    current: str | None = None          # 正在执行的 item@rev
    started_at: int | None = None
    stopped: list = field(default_factory=list)        # 主动声明停止的工作
    absent: list = field(default_factory=list)         # 声明未实际保留的操作


@dataclass
class Job:
    id: str
    requires: list                      # [["a@1","b@1"], ["c@1"]] 组间与、组内或
    state: str = "open"                 # open / released / cancelled
    history: list = field(default_factory=list)        # 每次修订/amend 的快照
    released_by: dict | None = None
    cancelled_at: int | None = None


@dataclass
class Receipt:
    seq: int
    at: int
    kind: str
    status: str
    detail: dict


class Engine:
    """单线程、可重放的队列状态机。"""

    def __init__(self):
        self.benches: dict[str, Bench] = {}
        self.items: dict[str, ItemRev] = {}
        self.jobs: dict[str, Job] = {}
        self.batches: dict[str, Batch] = {}
        self.ledger: list[Receipt] = []
        self._seq = 0
        self.now = 0
        self.restarts: list[int] = []
        # 已处理的 finish，用于重启后识别重复回报
        self.finish_seen: dict[str, dict] = {}
        # 原始事件日志，restart 时据此重放重建
        self.events: list[dict] = []
        self._replaying = False

    # ------------------------------------------------------------------ 基础

    def _emit(self, at, kind, status, **detail):
        if self._replaying:
            return None
        self._seq += 1
        receipt = Receipt(self._seq, at, kind, status, detail)
        self.ledger.append(receipt)
        return receipt

    def _item(self, ref):
        work = self.items.get(ref)
        if work is None:
            raise QueueReject(f"未知工作 {ref}")
        return work

    def _check_refs(self, refs):
        for ref in refs:
            self._item(ref)

    def _capable_benches(self, work):
        return [
            b for b in self.benches.values()
            if b.fixture == work.fixture and work.skill in b.skills
        ]

    def _dependencies_open(self, work):
        """返回尚未满足的依赖引用；失败的依赖单独标注。"""
        open_refs, failed = [], []
        for dep in work.depends:
            other = self.items.get(dep)
            if other is None or other.state != "passed":
                open_refs.append(dep)
                if other is not None and other.state == "failed":
                    failed.append(dep)
        return open_refs, failed

    def _sweep_breaches(self, at):
        """到期仍未开工的工作留下永久违约记录。"""
        warnings = []
        for work in self.items.values():
            if (
                work.breach_reason is None
                and work.state in ("waiting", "assigned")
                and at > work.deadline
            ):
                work.breach_reason = "deadline_passed_without_start"
                work.breach_at = at
                warnings.append(
                    {"work": ref_of(work.item, work.rev),
                     "reason": work.breach_reason, "deadline": work.deadline}
                )
        return warnings

    # ------------------------------------------------------------- 合批安全

    def _plan_of(self, bench_name, extra=()):
        """某台手上未开始工作的队列（按全局优先级），extra 为候选追加项。"""
        queued = [
            w for w in self.items.values()
            if w.bench == bench_name and w.state in ("waiting", "assigned")
        ]
        queued.extend(extra)
        return sorted(queued, key=_priority_key)

    def _projected_starts(self, bench_name, extra=()):
        """投影台位队列中每件的最早开工时刻。"""
        plan = self._plan_of(bench_name, extra)
        bench = self.benches[bench_name]
        clock = self.now
        if bench.current is not None:
            # 台位一次只做一项，手头工作释放前队列只能排在它之后
            clock = max(clock, bench.started_at + bench.duration)
        result = {}
        for work in plan:
            result[ref_of(work.item, work.rev)] = clock
            clock += bench.duration
        return result

    def _candidate_fits(self, bench_name, work):
        """候选工作能否在本台截止前开工，且不挤掉更紧迫的等待工作。"""
        starts = self._projected_starts(bench_name, extra=[work])
        if starts[ref_of(work.item, work.rev)] > work.deadline:
            return False, "own_deadline"
        # 全局检查：更紧迫的未开始工作只要在任一有能力台位上能赶上截止即可
        cand_key = _priority_key(work)
        for other in self.items.values():
            if other.state not in ("waiting", "assigned") or other.breach_reason:
                continue
            if _priority_key(other) >= cand_key:
                continue  # 只保护更紧迫的工作
            capable = self._capable_benches(other)
            if not capable:
                continue
            earliest = self.now
            for b in capable:
                extra = [work] if b.bench == bench_name else ()
                starts = self._projected_starts(b.bench, extra=extra)
                earliest = min(earliest, starts.get(
                    ref_of(other.item, other.rev), self.now))
            if earliest > other.deadline:
                return False, "squeezes_more_urgent"
        return True, "ok"

    def _waiting_reason(self, work):
        """区分缺能力 / 等依赖 / 等空闲台位 / 已违约。"""
        if work.breach_reason:
            return "breached", {"reason": work.breach_reason, "at": work.breach_at}
        if work.state in ("passed", "failed"):
            return work.state, {}
        open_refs, failed = self._dependencies_open(work)
        if open_refs:
            reason = "dependency_failed" if failed else "dependency"
            return reason, {"open": open_refs, "failed": failed}
        capable = self._capable_benches(work)
        if not capable:
            return "no_capability", {"fixture": work.fixture, "skill": work.skill}
        free = [b.bench for b in capable if b.current is None]
        if free:
            return "eligible", {"benches": free}
        return "waiting_bench", {"benches": [b.bench for b in capable]}

    # ----------------------------------------------------------------- 入口

    def handle(self, at, kind, record):
        if type(at) is not int or at < self.now:
            raise QueueReject("消息秒点必须是非递减的非负整数")
        self.now = at
        warnings = self._sweep_breaches(at)
        if kind == "restart":
            if self._replaying:
                # 重放时状态无需处理，但事件日志要保留以便再次重放
                self.events.append({"at": at, "kind": "restart", "record": {}})
                self.restarts.append(at)
                return None
            receipt = self.restart(at)
            if warnings:
                receipt.detail["warnings"] = warnings
        else:
            handler = getattr(self, f"_on_{kind}", None)
            if handler is None:
                raise QueueReject(f"未登记的消息名称 {kind}")
            receipt = handler(record)
        if receipt is not None and not self._replaying and kind != "restart":
            self.events.append({"at": at, "kind": kind, "record": record})
            if warnings:
                receipt.detail.setdefault("warnings", []).extend(warnings)
        return receipt

    # ------------------------------------------------------------- 目录登记

    def _on_bench(self, rec):
        name = _require(rec, "bench", str)
        fixture = _require(rec, "fixture", str)
        skills = _require(rec, "skills", list)
        duration = _require(rec, "duration", int)
        if duration <= 0 or not skills or not all(isinstance(s, str) for s in skills):
            raise QueueReject("台位能力描述不合法")
        if name in self.benches:
            return self._emit(self.now, "bench", "rejected", reason="duplicate", bench=name)
        self.benches[name] = Bench(name, fixture, list(skills), duration)
        return self._emit(self.now, "bench", "accepted", bench=name,
                          fixture=fixture, skills=list(skills), duration=duration)

    def _on_item(self, rec):
        item = _require(rec, "item", str)
        rev = _require(rec, "rev", int)
        skill = _require(rec, "skill", str)
        fixture = _require(rec, "fixture", str)
        urgency = _require(rec, "urgency", str)
        deadline = _require(rec, "deadline", int)
        depends = rec.get("depends", [])
        if urgency not in URGENCY_RANK:
            raise QueueReject(f"未知紧急级别 {urgency}")
        if rev < 1 or deadline < 0 or not isinstance(depends, list):
            raise QueueReject("工作描述不合法")
        key = ref_of(item, rev)
        if key in self.items:
            return self._emit(self.now, "item", "rejected", reason="duplicate", work=key)
        for dep in depends:
            parse_ref(dep)
        # 依赖必须指向已经登记的修订（目录资料先于工单提交）
        unknown = [d for d in depends if d not in self.items]
        if unknown:
            raise QueueReject(f"{key} 依赖了尚未登记的工作 {unknown}")
        self.items[key] = ItemRev(item, rev, skill, fixture, urgency, deadline,
                                  list(depends))
        return self._emit(self.now, "item", "accepted", work=key,
                          urgency=urgency, deadline=deadline,
                          fixture=fixture, skill=skill, depends=list(depends))

    def _on_job(self, rec):
        jid = _require(rec, "id", str)
        requires = _require(rec, "requires", list)
        _validate_requires(requires)
        for group in requires:
            self._check_refs(group)
        if jid in self.jobs:
            return self._emit(self.now, "job", "rejected", reason="duplicate", id=jid)
        snapshot = {"at": self.now, "requires": [list(g) for g in requires]}
        self.jobs[jid] = Job(jid, [list(g) for g in requires], history=[snapshot])
        return self._emit(self.now, "job", "accepted", id=jid,
                          requires=snapshot["requires"])

    def _on_amend(self, rec):
        jid = _require(rec, "id", str)
        requires = _require(rec, "requires", list)
        _validate_requires(requires)
        for group in requires:
            self._check_refs(group)
        job = self.jobs.get(jid)
        if job is None:
            raise QueueReject(f"未知工单 {jid}")
        if job.state == "cancelled":
            return self._emit(self.now, "amend", "rejected",
                              reason="job_cancelled", id=jid)
        old = [list(g) for g in job.requires]
        was_released = job.state == "released"
        if was_released:
            # 等待方需要新修订：旧判定不再放开它，工单重新回到等待
            job.state = "open"
            job.released_by = None
        job.requires = [list(g) for g in requires]
        job.history.append({"at": self.now, "requires": [list(g) for g in requires]})
        # 修订后重新评估放行；评估只读当前 requires，旧修订结果天然失效
        release = self._evaluate_job(job)
        return self._emit(self.now, "amend", "accepted", id=jid,
                          previous=old, current=job.requires,
                          reopened=was_released and release is None,
                          released=release)

    def _on_cancel(self, rec):
        jid = _require(rec, "id", str)
        job = self.jobs.get(jid)
        if job is None:
            raise QueueReject(f"未知工单 {jid}")
        if job.state == "cancelled":
            return self._emit(self.now, "cancel", "rejected",
                              reason="already_cancelled", id=jid)
        job.state = "cancelled"
        job.cancelled_at = self.now
        # 共享事实保留：只关闭工单需求，不触碰任何 item 结果
        return self._emit(self.now, "cancel", "accepted", id=jid,
                          cancelled_at=self.now)

    # ------------------------------------------------------------- 领取合批

    def _eligible_pool(self, bench):
        """完全未派台、本台可做、依赖已满足、未违约的工作。"""
        out = []
        for work in self.items.values():
            if work.state not in ("waiting", "assigned"):
                continue
            if work.bench is not None:
                continue
            if work.breach_reason:
                continue
            if work.fixture != bench.fixture or work.skill not in bench.skills:
                continue
            open_refs, _ = self._dependencies_open(work)
            if open_refs:
                continue
            out.append(work)
        return sorted(out, key=_priority_key)

    def _on_pull(self, rec):
        bench_name = _require(rec, "bench", str)
        batch_no = _require(rec, "batch", str)
        maximum = _require(rec, "max", int)
        if maximum < 1:
            raise QueueReject("max 至少为 1")
        bench = self.benches.get(bench_name)
        if bench is None:
            raise QueueReject(f"未知台位 {bench_name}")
        if batch_no in self.batches:
            return self._emit(self.now, "pull", "rejected",
                              reason="duplicate_batch", batch=batch_no)
        batch = Batch(batch_no, bench_name, self.now)
        self.batches[batch_no] = batch
        assigned, skipped = [], []

        def take(work, source_batch=None):
            work.bench = bench_name
            work.batch = batch_no
            work.state = "assigned"
            batch.works.append(ref_of(work.item, work.rev))
            batch.active_works.append(ref_of(work.item, work.rev))
            assigned.append(ref_of(work.item, work.rev))
            if source_batch is not None:
                source_batch.active_works.remove(ref_of(work.item, work.rev))
                source_batch.revoked.append(ref_of(work.item, work.rev))
                work.split_from.append(
                    {"batch": source_batch.batch, "at": self.now,
                     "reason": "rerouted_unstarted"})

        # 第一阶段：领取无人认领的工作
        for work in self._eligible_pool(bench):
            if len(assigned) >= maximum:
                break
            fits, why = self._candidate_fits(bench_name, work)
            if fits:
                take(work)
            else:
                skipped.append({"work": ref_of(work.item, work.rev), "reason": why})

        # 第二阶段：名额未满时，可拆其他台未开始的工作。
        # 只有常规件允许为更紧迫的领取让台；临期/急检批次不拆。
        rerouted = []
        if len(assigned) < maximum:
            movable = self._movable_elsewhere(bench)
            for work in movable:
                if len(assigned) >= maximum:
                    break
                fits, why = self._candidate_fits(bench_name, work)
                if not fits:
                    skipped.append({"work": ref_of(work.item, work.rev), "reason": why})
                    continue
                source = self.batches[work.batch]
                take(work, source)
                rerouted.append(ref_of(work.item, work.rev))
        return self._emit(self.now, "pull", "accepted", bench=bench_name,
                          batch=batch_no, assigned=assigned, rerouted=rerouted,
                          skipped=skipped)

    def _movable_elsewhere(self, bench):
        """别的台已领取但未开始、本台也能做的常规工作。

        拆批只能“常规件给急检/临期让台”：stopped 与 due 的批次有明确的
        时间承诺，不允许被另一家拆走；只有 regular 未开始项可移动。
        """
        out = []
        for work in self.items.values():
            if work.state != "assigned" or work.bench in (None, bench.bench):
                continue
            if work.urgency != "regular" or work.breach_reason:
                continue
            if work.fixture != bench.fixture or work.skill not in bench.skills:
                continue
            open_refs, _ = self._dependencies_open(work)
            if open_refs:
                continue
            out.append(work)
        return sorted(out, key=lambda w: (w.deadline, w.item, w.rev))

    # ------------------------------------------------------------- 开始/回报

    def _on_start(self, rec):
        bench_name = _require(rec, "bench", str)
        batch_no = _require(rec, "batch", str)
        work_ref = _require(rec, "work", str)
        parse_ref(work_ref)
        bench = self.benches.get(bench_name)
        if bench is None:
            raise QueueReject(f"未知台位 {bench_name}")
        batch = self.batches.get(batch_no)
        work = self.items.get(work_ref)
        if work is None:
            raise QueueReject(f"未知工作 {work_ref}")
        if batch is None:
            return self._reject_start(work_ref, "unknown_batch", batch_no)
        if work_ref not in batch.active_works:
            reason = "batch_split" if work_ref in batch.revoked else "not_in_batch"
            return self._reject_start(work_ref, reason, batch_no)
        if work.bench != bench_name or batch.bench != bench_name:
            return self._reject_start(work_ref, "belongs_other_bench", batch_no,
                                      extra={"owner": work.bench})
        if work.state in ("passed", "failed"):
            return self._reject_start(work_ref, f"already_{work.state}", batch_no)
        if work.state == "started":
            return self._reject_start(work_ref, "already_started", batch_no)
        if work.breach_reason:
            return self._reject_start(work_ref, "breach_locked", batch_no,
                                      extra={"detail": work.breach_reason})
        open_refs, failed = self._dependencies_open(work)
        if open_refs:
            return self._reject_start(
                work_ref, "dependency_failed" if failed else "dependency_open",
                batch_no, extra={"open": open_refs})
        if bench.current is not None:
            return self._reject_start(work_ref, "bench_busy", batch_no,
                                      extra={"current": bench.current})
        work.state = "started"
        work.started_at = self.now
        bench.current = work_ref
        bench.started_at = self.now
        return self._emit(self.now, "start", "accepted", bench=bench_name,
                          batch=batch_no, work=work_ref, started_at=self.now)

    def _reject_start(self, work_ref, reason, batch_no, extra=None):
        payload = {"work": work_ref, "batch": batch_no, "reason": reason}
        if extra:
            payload.update(extra)
        return self._emit(self.now, "start", "rejected", **payload)

    def _on_finish(self, rec):
        bench_name = _require(rec, "bench", str)
        batch_no = _require(rec, "batch", str)
        work_ref = _require(rec, "work", str)
        passed = _require(rec, "passed", bool)
        parse_ref(work_ref)
        bench = self.benches.get(bench_name)
        if bench is None:
            raise QueueReject(f"未知台位 {bench_name}")
        work = self.items.get(work_ref)
        if work is None:
            raise QueueReject(f"未知工作 {work_ref}")

        previous = self.finish_seen.get(work_ref)
        if previous is not None:
            # 队列重启后的重复回报：同修订同判定是幂等事实，否则只入历史
            return self._emit(
                self.now, "finish", "duplicate",
                work=work_ref, bench=bench_name,
                same_bench=previous["bench"] == bench_name,
                same_passed=previous["passed"] == passed,
                kept=previous)
        if work.bench != bench_name:
            return self._emit(self.now, "finish", "rejected",
                              work=work_ref, reason="belongs_other_bench",
                              owner=work.bench)
        if work.state != "started":
            return self._emit(self.now, "finish", "rejected",
                              work=work_ref, reason=f"not_started_{work.state}")
        work.state = "passed" if passed else "failed"
        work.passed = passed
        work.finished_at = self.now
        bench.current = None
        self.finish_seen[work_ref] = {
            "bench": bench_name, "batch": batch_no,
            "passed": passed, "at": self.now}
        detail = {"work": work_ref, "bench": bench_name, "passed": passed,
                  "finished_at": self.now, "breach_on_start": work.breach_reason}
        # 事实落库后推进所有等待它的工单
        released = []
        if passed:
            for job in self.jobs.values():
                if job.state == "open":
                    info = self._evaluate_job(job)
                    if info:
                        released.append(info)
        detail["released"] = released
        return self._emit(self.now, "finish", "accepted", **detail)

    def _on_release(self, rec):
        bench_name = _require(rec, "bench", str)
        work_ref = _require(rec, "work", str)
        note = rec.get("note", "")
        parse_ref(work_ref)
        bench = self.benches.get(bench_name)
        if bench is None:
            raise QueueReject(f"未知台位 {bench_name}")
        work = self.items.get(work_ref)
        if work is None:
            raise QueueReject(f"未知工作 {work_ref}")
        if work.bench != bench_name:
            return self._emit(self.now, "release", "rejected",
                              work=work_ref, reason="belongs_other_bench",
                              owner=work.bench)
        if work.state in ("passed", "failed"):
            return self._emit(self.now, "release", "rejected",
                              work=work_ref, reason="already_finished")
        old_batch = work.batch
        if work.state == "started":
            # 已开始的工作钉在原台：只有原台以 note="stopped" 确认停止，
            # 调度才能把它交给另一家
            if note != "stopped":
                return self._emit(self.now, "release", "rejected",
                                  work=work_ref,
                                  reason="started_work_needs_stop_confirmation")
            bench.current = None
            bench.stopped.append(work_ref)
        else:
            # 未开始项：确认本台未实际保留该操作
            bench.absent.append(work_ref)
        if old_batch and work_ref in self.batches[old_batch].active_works:
            self.batches[old_batch].active_works.remove(work_ref)
            self.batches[old_batch].revoked.append(work_ref)
        work.state = "waiting"
        work.bench = None
        work.batch = None
        work.released = True
        work.split_from.append({"batch": old_batch, "at": self.now,
                                "reason": "release"})
        return self._emit(self.now, "release", "accepted", work=work_ref,
                          bench=bench_name, returned_to_pool=True)

    # 台位对已开始工作的停止声明通过 release(note="stopped") 表达，
    # 并记入 bench.stopped 供 inspect 审计。

    # ------------------------------------------------------------- 工单推进

    def _group_satisfied(self, group):
        """组内任一引用持有当前有效合格结果即满足。"""
        for ref in group:
            work = self.items.get(ref)
            if work is not None and work.state == "passed":
                return ref
        return None

    def _evaluate_job(self, job):
        """组间与、组内或；返回放行描述或 None。违约事实仍可放行但带标记。"""
        if job.state != "open":
            return None
        chosen = []
        for group in job.requires:
            ref = self._group_satisfied(group)
            if ref is None:
                return None
            chosen.append(ref)
        job.state = "released"
        job.released_by = {"at": self.now, "results": chosen,
                           "with_breach": [
                               r for r in chosen
                               if self.items[r].breach_reason is not None]}
        return {"id": job.id, "results": chosen,
                "with_breach": job.released_by["with_breach"]}

    def _waiting_groups(self, job):
        """工单仍在等什么：每组给出候选状态，便于值班台解释等待原因。"""
        groups = []
        for group in job.requires:
            candidates = []
            for ref in group:
                work = self.items.get(ref)
                if work is None:
                    candidates.append({"ref": ref, "state": "unknown"})
                    continue
                info = {"ref": ref, "state": work.state}
                if work.breach_reason:
                    info["breach"] = work.breach_reason
                if work.state in ("waiting", "assigned", "started"):
                    reason, extra = self._waiting_reason(work)
                    info["wait"] = reason
                    info.update(extra)
                candidates.append(info)
            groups.append(candidates)
        return groups

    # ----------------------------------------------------------------- 重启

    @classmethod
    def replay(cls, events):
        """用事件日志重建一台新引擎（重放期间不产生新回执）。"""
        fresh = cls()
        fresh._replaying = True
        for event in events:
            fresh.handle(event["at"], event["kind"], event["record"])
        fresh._replaying = False
        return fresh

    def restart(self, at):
        """模拟队列重启：重放全部历史事件，验证状态可重建。"""
        rebuilt = Engine.replay(self.events)
        self.benches = rebuilt.benches
        self.items = rebuilt.items
        self.jobs = rebuilt.jobs
        self.batches = rebuilt.batches
        self.finish_seen = rebuilt.finish_seen
        self.restarts.append(at)
        if not self._replaying:
            self.events.append({"at": at, "kind": "restart", "record": {}})
        return self._emit(at, "restart", "accepted", restarted_at=at,
                          items=len(self.items), jobs=len(self.jobs),
                          benches=len(self.benches),
                          batches=len(self.batches))

    # ----------------------------------------------------------------- 查询

    def _on_inspect(self, rec):
        if "job" in rec:
            return self._inspect_job(rec["job"])
        if "item" in rec:
            return self._inspect_item(rec["item"])
        if "bench" in rec:
            return self._inspect_bench(rec["bench"])
        return self._inspect_all()

    def _inspect_job(self, jid):
        job = self.jobs.get(jid)
        if job is None:
            raise QueueReject(f"未知工单 {jid}")
        payload = {
            "job": jid, "state": job.state,
            "requires": job.requires, "history": job.history,
            "released_by": job.released_by,
        }
        if job.state == "open":
            payload["waiting"] = self._waiting_groups(job)
        # 同一检验被几个工单使用
        usage = {}
        for ref in {r for g in job.requires for r in g}:
            usage[ref] = self._usage_of(ref)
        payload["shared_usage"] = usage
        return self._emit(self.now, "inspect", "ok", **payload)

    def _usage_of(self, ref):
        """一份检验被哪些工单引用（含已取消与旧修订历史）。"""
        users = []
        for j in self.jobs.values():
            seen = {}
            for snapshot in j.history:
                for idx, group in enumerate(snapshot["requires"]):
                    if ref in group:
                        seen[idx] = True
            for idx in sorted(seen):
                current = (j.state != "cancelled"
                           and any(ref in g for g in j.requires))
                users.append({"job": j.id, "state": j.state, "group": idx,
                              "current": current})
        return users

    def _inspect_item(self, ref):
        if "@" not in ref:
            # 只给 item 名时汇总该项目所有修订
            revs = [w for w in self.items.values() if w.item == ref]
            if not revs:
                raise QueueReject(f"未知项目 {ref}")
            return self._emit(self.now, "inspect", "ok", item=ref,
                              revisions=[self._item_payload(w) for w in
                                         sorted(revs, key=lambda w: w.rev)])
        work = self._item(ref)
        return self._emit(self.now, "inspect", "ok", **self._item_payload(work))

    def _item_payload(self, work):
        reason, extra = self._waiting_reason(work)
        payload = {
            "work": ref_of(work.item, work.rev), "state": work.state,
            "owner": work.bench, "batch": work.batch,
            "started_at": work.started_at, "finished_at": work.finished_at,
            "passed": work.passed, "depends": work.depends,
            "split_from": work.split_from, "wait_reason": reason,
        }
        payload.update(extra)
        if work.breach_reason:
            payload["breach"] = {"reason": work.breach_reason,
                                 "at": work.breach_at,
                                 "deadline": work.deadline}
        payload["used_by"] = self._usage_of(ref_of(work.item, work.rev))
        return payload

    def _inspect_bench(self, bench_name):
        bench = self.benches.get(bench_name)
        if bench is None:
            raise QueueReject(f"未知台位 {bench_name}")
        holding = [self._item_payload(w) for w in self.items.values()
                   if w.bench == bench_name]
        return self._emit(self.now, "inspect", "ok", bench=bench_name,
                          current=bench.current,
                          busy_until=(bench.started_at + bench.duration
                                      if bench.current else None),
                          fixture=bench.fixture, skills=bench.skills,
                          holding=holding,
                          stopped=list(bench.stopped), absent=list(bench.absent))

    def _inspect_all(self):
        unstarted = []
        for work in self.items.values():
            if work.state in ("waiting", "assigned"):
                reason, extra = self._waiting_reason(work)
                entry = {"work": ref_of(work.item, work.rev),
                         "state": work.state, "owner": work.bench,
                         "batch": work.batch, "wait_reason": reason}
                entry.update(extra)
                unstarted.append(entry)
        jobs = [{"id": j.id, "state": j.state} for j in self.jobs.values()]
        return self._emit(self.now, "inspect", "ok",
                          unstarted=unstarted, jobs=jobs,
                          benches=list(self.benches))


def _priority_key(work):
    return (URGENCY_RANK[work.urgency], work.deadline, work.item, work.rev)


def _require(rec, name, typ):
    value = rec.get(name)
    if not isinstance(value, typ) or (typ is str and not value):
        raise QueueReject(f"字段 {name} 缺失或类型不对")
    return value


def _validate_requires(requires):
    if not isinstance(requires, list) or not requires:
        raise QueueReject("requires 必须是非空备选列表")
    for group in requires:
        if not isinstance(group, list) or not group:
            raise QueueReject("每个备选列表必须非空")
        for ref in group:
            parse_ref(ref)
