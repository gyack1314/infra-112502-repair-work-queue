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

- urgent-after-first-start.jsonl
- shared-work-revision.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入。
