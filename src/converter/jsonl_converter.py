"""
JSONL 转换模块

职责：将 DataFrame 转换为指定平台格式的 JSONL 文件。

格式由 FormatSchema 控制，支持多种平台格式的切换：
  - openai   : messages + user/assistant 角色（默认）
  - internal : conversations + human/assistant 角色 + 自增 id

格式可通过以下方式指定（优先级从高到低）：
  1. 直接传入 FormatSchema 对象
  2. config.yaml 中的 output_format.preset
  3. 内置默认值（openai）
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings
from src.converter.format_schema import FormatSchema, get_schema

logger = logging.getLogger(__name__)


class JsonlConverter:
    """DataFrame → JSONL 转换器。

    Args:
        settings: 全局配置对象，默认使用单例。
        schema:   输出格式，默认从 settings.output_format 读取。
    """

    def __init__(
        self,
        settings: Settings | None = None,
        schema: FormatSchema | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        # schema 优先级：显式传入 > settings 中的格式 > 回退 openai
        self.schema = schema or self.settings.output_format

    def row_to_record(self, input_text: str, output_text: str, idx: int) -> dict:
        """将单条 input/output 按当前 FormatSchema 转换为字典。

        Args:
            input_text:  用户输入文本。
            output_text: 期望输出标签。
            idx:         当前记录的自增序号（从 1 开始），仅在 include_id=True 时使用。

        Returns:
            符合目标平台格式要求的 dict。
        """
        schema = self.schema

        # ── 构建对话列表 ──
        conversations = [
            {
                "role": schema.map_role("system"),
                "content": self.settings.system_prompt,
            },
            {
                "role": schema.map_role("user"),
                "content": input_text,
            },
            {
                "role": schema.map_role("assistant"),
                "content": output_text,
            },
        ]

        # ── 组装最终记录 ──
        record: dict = {}
        if schema.include_id:
            record["id"] = idx                       # 自增 id 放在最前面
        record[schema.conversations_key] = conversations

        return record

    def convert(
        self,
        df: pd.DataFrame,
        output_path: str | Path | None = None,
        schema: FormatSchema | None = None,
    ) -> Path:
        """将 DataFrame 转换并写入 JSONL 文件。

        Args:
            df:          包含 input/output 列的 DataFrame。
            output_path: 输出路径，默认写入 settings.processed_jsonl_path。
            schema:      临时覆盖格式（不影响实例默认格式）。

        Returns:
            实际写入的文件路径。
        """
        active_schema = schema or self.schema
        path = Path(output_path) if output_path else self.settings.processed_jsonl_path
        path.parent.mkdir(parents=True, exist_ok=True)

        in_col  = self.settings.input_col
        out_col = self.settings.output_col

        logger.info(
            f"开始转换 {len(df)} 条数据 → {path}  [格式: {active_schema.name}]"
        )

        with path.open("w", encoding="utf-8") as f:
            for idx, (_, row) in enumerate(df.iterrows(), start=1):
                # 每次单独构建，临时 schema 不修改 self.schema
                old_schema, self.schema = self.schema, active_schema
                record = self.row_to_record(
                    input_text=str(row[in_col]),
                    output_text=str(row[out_col]),
                    idx=idx,
                )
                self.schema = old_schema
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info(f"转换完成：{len(df)} 条记录已写入 {path}")
        return path

    def convert_split(
        self,
        df: pd.DataFrame,
        output_path: str | Path,
        schema: FormatSchema | None = None,
    ) -> Path:
        """将已划分的子集（train/val/test）写入指定路径，支持临时格式覆盖。"""
        return self.convert(df, output_path, schema)


def convert_to_jsonl(
    df: pd.DataFrame,
    output_path: str | Path | None = None,
    settings: Settings | None = None,
    schema: FormatSchema | None = None,
) -> Path:
    """模块级便捷函数。

    Example:
        >>> from src.converter.format_schema import get_schema
        >>> path = convert_to_jsonl(df, schema=get_schema("internal"))
    """
    return JsonlConverter(settings, schema).convert(df, output_path)
