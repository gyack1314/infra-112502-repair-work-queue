# 维修作业队列的可拆批交接与关键路径推进

台位按工装和技能接工作。合成检验关系来自工单端，结果来自维修台。时间、工单和部件号仅用于本地排演。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **bench**　维修台能力，fixture 给出可用夹具，skills 是项目，duration 表示每项仿真耗时。台位同一时刻只执行一项。
- **job**　提交工单 id。requires 的每个元素是一个备选列表，同一列表任一满足即可。所有列表同时满足才放行。每个引用写 item@rev。
- **item**　可被多个工单引用的工作，rev 从 1 开始，技能 skill，夹具 fixture，优先级 urgency 和最迟开始 deadline。depends 引用其他项的 item@rev。 urgency 为 stopped、due 或 regular，级别相同以 deadline 早者先，仍并列可任选。
- **pull**　台位申请一批工作。max 表示平台可领取连续工作数量上限，batch 是申请号。
- **start**　台位开始一个 item@rev，出示此前领取的 batch。
- **finish**　台位回报自己已开工的项，passed 为判定结果。
- **release**　台位确认指定项未实际开工或已停止。
- **amend**　工单端替换此前一份工单的 requires，修订后的引用由该工单明确给出。
- **cancel**　关闭一份工单的剩余需求。
- **restart**　模拟队列重启。
- **inspect**　按 job、item 或 bench 看进度。

## 示例

- urgent-after-first-start.jsonl：急检插队、拆批、旧批号失效、重启回报
- shared-work-revision.jsonl：共享检验事实、取消保留、修订后旧判定失效
- full-shift.jsonl：三台协作的完整班次，含合批投影、违约留痕、候选/配对、
  修订重开与重启重复回报

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入。

## 作业队列（jobqueue 包）

`jobqueue/` 是调度台与维修伙伴之间的队列引擎，`jobqueue/runner.py` 把演练记录
逐条送入引擎并为每条消息输出一张 JSON 回执：

    python3 -I jobqueue examples/urgent-after-first-start.jsonl
    python3 -I jobqueue examples --pretty

`-I` 隔离模式下也可运行，包内 `__main__.py` 会自行补好模块搜索路径，
不读取当前目录下的任何模块。

### 状态机与归属

每个 `item@rev` 是一项具体工作，状态沿
`waiting → assigned → started → passed/failed` 单向推进，另有违约锁定。

- **合批**：`pull` 按 urgency（stopped > due > regular）、deadline、项目号排序，
  把同夹具、技能匹配、依赖已满足且未违约的工作纳入批次。入批前投影台位队列中
  每件的最早开工时刻：候选自己赶不上截止（`own_deadline`），或会让任一更紧迫的
  未开始工作在所有有能力台位上都赶不上截止（`squeezes_more_urgent`）时跳过，
  跳过原因写在回执 `skipped` 中。
- **拆批**：未开始（`assigned`）的工作不属于任何台位，急检插队时可被其他台位
  拆走；原批次的 `active_works` 移除该项并记入 `revoked`。处理端随后拿旧批号
  `start` 被拒，原因 `batch_split`。已开始（`started`）的工作钉在原台，
  其他台位的 `pull` 看不到它，只有原台 `release`（`note: "stopped"` 确认停止，
  或未开始项声明未实际保留）后才回到领取池。
- **台位忙**：一台同一时刻只执行一项，未结束时第二张 `start` 回执原因
  `bench_busy`，并给出 `current`。

### 共享事实与修订

- 检验结果按 `item@rev` 保存为一份事实，工单的 `requires` 只存引用；
  外层列表之间是“与”，内层备选列表之间是“或”：两件候选备件任一件合格即可
  （一个备选列表），必须配对的一对写成两个单元素列表。
- 一份结果可同时放行多个工单；`cancel` 只关闭工单需求，不删除任何事实。
  `inspect` 的 `used_by` 同时给出当前生效引用与历史引用（含已取消工单）。
- `amend` 替换工单要求并保留每次快照。旧修订的合格结果对新要求不再有效；
  已放行工单若新要求尚未满足会重新置为等待（回执 `reopened: true`），
  旧快照仍可通过 `inspect` 追查。

### 等待原因与违约留痕

`inspect` 对每项未开始工作给出四类可区分的等待原因：

| wait_reason | 含义 |
| --- | --- |
| `no_capability` | 没有台位具备所需夹具/技能（可转其他伙伴的依据） |
| `dependency` / `dependency_failed` | 等依赖项出结果，或依赖已判不合格 |
| `waiting_bench` | 有能力的台位都在忙 |
| `eligible` | 现在即可领取，附可领台位 |

时间越过某工作的 `deadline` 而它仍未开工时，引擎留下永久违约记录
（`deadline_passed_without_start`，随当时回执的 `warnings` 下发），
此后 `start` 被 `breach_locked` 拒绝。事后补报合格不会清除该记录；
若结果仍在事实层面放行了工单，放行回执与 `released_by.with_breach` 会带上标记。

### 重启与重复回报

引擎采用事件溯源：全部输入消息保存在事件日志中，`restart` 调用
`Engine.replay` 重放日志重建完整状态（批次归属、台位占用、违约标记一致）。
重启后同一 `item@rev` 的重复 `finish`：同台同判定回执 `duplicate`
（`same_passed/same_bench: true`），是可复用的幂等事实；判定不一致只入历史，
不覆盖已落库的事实。

### 回执状态

- `accepted / ok`：消息生效；
- `rejected`：消息合法但前置条件不满足（批号失效、台位忙、归属他人等），
  一律带 `reason`，队列状态不变；
- `duplicate`：重启后的重复回报；
- `invalid`（runner 层）：消息本身不合法（未知引用、字段缺失等），
  对应引擎抛出的 `QueueReject`。

