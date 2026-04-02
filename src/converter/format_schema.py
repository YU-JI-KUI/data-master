"""
输出格式定义模块

支持两种记录结构：
  conversations : 嵌套对话列表（openai / internal 格式）
  flat          : 平铺字段（ark 格式）

支持两种文件格式：
  jsonl       : 每行一个 JSON 对象（适合流式读取，体积大时更友好）
  json_array  : 整体一个 JSON 数组（ark 等平台要求的格式）

扩展方法：
  在文件底部追加 register(FormatSchema(...)) 即可注册新格式，无需改其他代码。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FormatSchema:
    """描述一种输出格式的完整规则。

    Attributes:
        name:              格式唯一标识，用于 config.yaml 和 --format 参数。

        ── conversations 风格字段（record_style="conversations" 时使用） ──
        conversations_key: 对话数组的字段名，如 "messages" / "conversations"。
        role_map:          角色映射，key 为内部角色（system/user/assistant），
                           value 为目标平台期望的角色名。
        include_id:        是否在每条记录前加自增 id 字段。
        content_key:       对话条目中承载文本的字段名，通常为 "content"。
                           内部平台因历史 bug 使用 "context"，故该字段可配。

        ── flat 风格字段（record_style="flat" 时使用） ──
        flat_field_map:    将内部角色名映射到平铺字段名，
                           如 {"system": "system", "user": "human", "assistant": "assistant"}。
        extra_fields:      每条记录追加的静态字段，如 {"instructions": ""}。

        ── 文件级控制 ──
        record_style:      "conversations"（嵌套）或 "flat"（平铺），默认 conversations。
        output_type:       "jsonl"（逐行）或 "json_array"（整体数组），默认 jsonl。
        file_extension:    输出文件的后缀，默认 ".jsonl"。
    """

    name: str

    # ── conversations 风格 ──
    conversations_key: str = "messages"
    role_map: dict[str, str] = field(default_factory=lambda: {
        "system": "system", "user": "user", "assistant": "assistant"
    })
    include_id: bool = False
    # 对话条目中承载文本的字段名，通常为 "content"；
    # 内部平台存在历史遗留 bug，使用 "context" 代替 "content"
    content_key: str = "content"

    # ── flat 风格 ──
    flat_field_map: dict[str, str] = field(default_factory=dict)
    extra_fields: dict[str, Any] = field(default_factory=dict)

    # ── 文件级 ──
    record_style: str = "conversations"   # "conversations" | "flat"
    output_type: str = "jsonl"            # "jsonl" | "json_array"
    file_extension: str = ".jsonl"

    def map_role(self, internal_role: str) -> str:
        """conversations 风格：将内部角色名转换为平台角色名。"""
        return self.role_map.get(internal_role, internal_role)

    def flat_key(self, internal_role: str) -> str:
        """flat 风格：将内部角色名转换为平铺字段名。"""
        return self.flat_field_map.get(internal_role, internal_role)


# ── 格式注册表 ───────────────────────────────────────────────
_REGISTRY: dict[str, FormatSchema] = {}


def register(schema: FormatSchema) -> FormatSchema:
    """注册 FormatSchema 到全局注册表，返回自身。"""
    _REGISTRY[schema.name] = schema
    return schema


def get_schema(name: str) -> FormatSchema:
    """按名称获取已注册的 FormatSchema。

    Raises:
        ValueError: 名称未注册时，列出所有可用格式。
    """
    if name not in _REGISTRY:
        raise ValueError(
            f"未知的输出格式：'{name}'，可用格式：{list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name]


def list_formats() -> list[str]:
    """返回所有已注册的格式名称列表。"""
    return list(_REGISTRY.keys())


# ════════════════════════════════════════════════════════════
# 内置预置格式
# ════════════════════════════════════════════════════════════

# ── 格式 1：OpenAI / LLaMA-Factory 标准格式 ──────────────
# 每行一个 JSON 对象：
#   {"messages": [{"role": "system", ...}, {"role": "user", ...}, {"role": "assistant", ...}]}
OPENAI = register(FormatSchema(
    name="openai",
    conversations_key="messages",
    role_map={"system": "system", "user": "user", "assistant": "assistant"},
    include_id=False,
    record_style="conversations",
    output_type="jsonl",
    file_extension=".jsonl",
))

# ── 格式 2：内部平台格式 ──────────────────────────────────
# 每行一个 JSON 对象，含自增 id：
#   {"id": 1, "conversations": [{"role": "system", "context": "..."}, {"role": "human", "context": "..."}, ...]}
# 注意：内部平台存在历史遗留 bug，对话条目的文本字段使用 "context" 而非标准的 "content"，
#       平台方暂不修复，因此此处配置 content_key="context" 以适配。
INTERNAL = register(FormatSchema(
    name="internal",
    conversations_key="conversations",
    role_map={"system": "system", "user": "human", "assistant": "assistant"},
    include_id=True,
    content_key="context",
    record_style="conversations",
    output_type="jsonl",
    file_extension=".jsonl",
))

# ── 格式 3：Ark 平台格式 ──────────────────────────────────
# 整体为 JSON 数组，每条记录是平铺字段：
#   [
#     {
#       "instruction": "<system_prompt>",
#       "input":       "<用户问句>",
#       "output":      "<标签>"
#     },
#     ...
#   ]
# 字段名映射：内部角色 → Ark 平台字段名
#   system    → instruction
#   user      → input
#   assistant → output
ARK = register(FormatSchema(
    name="ark",
    flat_field_map={"system": "instruction", "user": "input", "assistant": "output"},
    record_style="flat",
    output_type="json_array",
    file_extension=".json",
))

# ── 在此追加新格式 ────────────────────────────────────────
# register(FormatSchema(
#     name="新平台名",
#     ...
# ))
