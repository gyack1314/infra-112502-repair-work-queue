"""调度台与维修伙伴之间的可拆批检验作业队列。

只依赖标准库，按事件溯源方式工作：每条输入消息产生一张回执，
``restart`` 通过重放全部历史消息重建内存状态。
"""

from .engine import Engine, QueueReject, parse_ref

__all__ = ["Engine", "QueueReject", "parse_ref"]
