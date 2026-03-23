"""
JSONL 输出格式定义模块

设计思路：
  不同大模型平台对训练数据格式的要求各不相同，例如：
    - OpenAI / LLaMA-Factory 标准格式：messages + user/assistant 角色
    - 内部平台格式：conversations + human/assistant 角色 + 自增 id

  通过 FormatSchema 统一描述一种格式的规则，所有预置格式注册到 REGISTRY。
  调用方只需传入格式名（如 "openai" / "internal"），无需关心具体字段细节。

扩展方法：
  如需支持新平台，只需在此文件底部追加一个 FormatSchema 并调用 register() 注册。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FormatSchema:
    """描述一种 JSONL 输出格式的规则。

    Attributes:
        name:               格式的唯一标识符，用于 config.yaml 和 --format 参数。
        conversations_key:  对话数组的字段名，如 "messages" 或 "conversations"。
        role_map:           角色映射表，key 为内部角色（system/user/assistant），
                            value 为该平台期望的角色名称。
        include_id:         是否在每条记录中添加自增 id 字段。
    """

    name: str
    conversations_key: str
    role_map: dict[str, str]
    include_id: bool = False

    def map_role(self, internal_role: str) -> str:
        """将内部角色名转换为平台角色名，找不到时原样返回。"""
        return self.role_map.get(internal_role, internal_role)


# ── 格式注册表 ───────────────────────────────────────────────
_REGISTRY: dict[str, FormatSchema] = {}


def register(schema: FormatSchema) -> FormatSchema:
    """将 FormatSchema 注册到全局注册表，并返回自身（便于链式使用）。"""
    _REGISTRY[schema.name] = schema
    return schema


def get_schema(name: str) -> FormatSchema:
    """按名称获取 FormatSchema。

    Raises:
        ValueError: 名称未注册时抛出，并列出所有可用格式。
    """
    if name not in _REGISTRY:
        available = list(_REGISTRY.keys())
        raise ValueError(
            f"未知的输出格式：'{name}'，可用格式：{available}"
        )
    return _REGISTRY[name]


def list_formats() -> list[str]:
    """返回所有已注册的格式名称列表。"""
    return list(_REGISTRY.keys())


# ════════════════════════════════════════════════════════════
# 内置预置格式
# ════════════════════════════════════════════════════════════

# ── 格式 1：OpenAI / LLaMA-Factory 标准格式 ──────────────
#   {
#     "messages": [
#       {"role": "system",    "content": "..."},
#       {"role": "user",      "content": "..."},
#       {"role": "assistant", "content": "..."}
#     ]
#   }
OPENAI = register(FormatSchema(
    name="openai",
    conversations_key="messages",
    role_map={
        "system":    "system",
        "user":      "user",
        "assistant": "assistant",
    },
    include_id=False,
))

# ── 格式 2：内部平台格式 ──────────────────────────────────
#   {
#     "id": 1,
#     "conversations": [
#       {"role": "system",    "content": "..."},
#       {"role": "human",     "content": "..."},
#       {"role": "assistant", "content": "..."}
#     ]
#   }
INTERNAL = register(FormatSchema(
    name="internal",
    conversations_key="conversations",
    role_map={
        "system":    "system",
        "user":      "human",      # 内部平台将用户角色称为 human
        "assistant": "assistant",
    },
    include_id=True,
))

# ── 在此追加新格式 ────────────────────────────────────────
# 示例：如需支持 Alpaca 格式，取消注释并按需修改：
#
# ALPACA = register(FormatSchema(
#     name="alpaca",
#     conversations_key="messages",
#     role_map={"system": "system", "user": "human", "assistant": "gpt"},
#     include_id=False,
# ))
