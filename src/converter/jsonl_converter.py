"""
数据转换模块

将 DataFrame 转换为目标平台所需的训练数据文件，格式由 FormatSchema 控制。

支持两种记录结构：
  conversations : 嵌套对话列表（openai / internal 格式）
  flat          : 平铺字段（ark 格式）

支持两种文件格式：
  jsonl       : 每行一个 JSON 对象
  json_array  : 整体一个 JSON 数组
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
    """DataFrame → 训练数据文件转换器。

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
        self.schema = schema or self.settings.output_format

    # ──────────────────────────────────────────────────────────
    # 单条记录构建
    # ──────────────────────────────────────────────────────────

    def row_to_record(self, input_text: str, output_text: str, idx: int) -> dict:
        """将单条 input/output 按当前 FormatSchema 转换为字典。

        Args:
            input_text:  用户输入文本（Excel input 列）。
            output_text: 期望输出标签（Excel output 列）。
            idx:         自增序号（从 1 开始），仅 include_id=True 时写入。

        Returns:
            符合目标平台格式的 dict。
        """
        if self.schema.record_style == "flat":
            return self._build_flat_record(input_text, output_text)
        else:
            return self._build_conversations_record(input_text, output_text, idx)

    def _build_conversations_record(
        self, input_text: str, output_text: str, idx: int
    ) -> dict:
        """构建嵌套 conversations 风格的记录（openai / internal）。"""
        schema = self.schema
        conversations = [
            {"role": schema.map_role("system"),    "content": self.settings.system_prompt},
            {"role": schema.map_role("user"),       "content": input_text},
            {"role": schema.map_role("assistant"),  "content": output_text},
        ]
        record: dict = {}
        if schema.include_id:
            record["id"] = idx
        record[schema.conversations_key] = conversations
        return record

    def _build_flat_record(self, input_text: str, output_text: str) -> dict:
        """构建平铺字段风格的记录（ark）。

        字段顺序：system → human（或自定义） → assistant → extra_fields
        """
        schema = self.schema
        record: dict = {
            schema.flat_key("system"):    self.settings.system_prompt,
            schema.flat_key("user"):      input_text,
            schema.flat_key("assistant"): output_text,
        }
        # 追加静态扩展字段（如 instructions: ""）
        record.update(schema.extra_fields)
        return record

    # ──────────────────────────────────────────────────────────
    # 文件写入
    # ──────────────────────────────────────────────────────────

    def convert(
        self,
        df: pd.DataFrame,
        output_path: str | Path | None = None,
        schema: FormatSchema | None = None,
    ) -> Path:
        """将 DataFrame 转换并写入文件。

        Args:
            df:          包含 input/output 列的 DataFrame。
            output_path: 输出路径；为 None 时使用 settings.processed_data_path(active_schema)。
            schema:      临时覆盖格式（不影响实例默认格式）。

        Returns:
            实际写入的文件路径。
        """
        active_schema = schema or self.schema

        if output_path is None:
            output_path = self.settings.get_processed_path(active_schema)
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        in_col  = self.settings.input_col
        out_col = self.settings.output_col

        logger.info(
            f"开始转换 {len(df)} 条数据 → {path}  [格式: {active_schema.name}]"
        )

        # ── 临时切换 schema 来构建记录 ──
        original_schema, self.schema = self.schema, active_schema

        records = [
            self.row_to_record(str(row[in_col]), str(row[out_col]), idx)
            for idx, (_, row) in enumerate(df.iterrows(), start=1)
        ]

        self.schema = original_schema

        # ── 按 output_type 写文件 ──
        with path.open("w", encoding="utf-8") as f:
            if active_schema.output_type == "json_array":
                # 整体 JSON 数组，缩进 2 格方便阅读
                json.dump(records, f, ensure_ascii=False, indent=2)
                f.write("\n")
            else:
                # 默认 JSONL：每条记录占一行
                for record in records:
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
        >>> path = convert_to_jsonl(df, schema=get_schema("ark"))
    """
    return JsonlConverter(settings, schema).convert(df, output_path)
