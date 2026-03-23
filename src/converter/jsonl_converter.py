"""
JSONL 转换模块

职责：将 DataFrame 转换为 Qwen3 微调格式的 JSONL 文件。

输出格式（每行一个 JSON 对象）：
{
  "messages": [
    {"role": "system",    "content": "<system_prompt>"},
    {"role": "user",      "content": "<input>"},
    {"role": "assistant", "content": "<output>"}
  ]
}

设计：
- 单条转换（row_to_messages）与批量写入（convert）分离
- 支持写入到任意路径，也可直接写入默认的 processed/data.jsonl
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


class JsonlConverter:
    """DataFrame → JSONL 转换器。

    Args:
        settings: 全局配置对象，默认使用单例。
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def row_to_messages(self, input_text: str, output_text: str) -> dict:
        """将单条 input/output 转换为 messages 格式字典。

        Args:
            input_text: 用户输入文本。
            output_text: 期望输出标签。

        Returns:
            符合 Qwen3 微调格式的 dict。
        """
        return {
            "messages": [
                {
                    "role": "system",
                    "content": self.settings.system_prompt,
                },
                {
                    "role": "user",
                    "content": input_text,
                },
                {
                    "role": "assistant",
                    "content": output_text,
                },
            ]
        }

    def convert(
        self,
        df: pd.DataFrame,
        output_path: str | Path | None = None,
    ) -> Path:
        """将 DataFrame 转换并写入 JSONL 文件。

        Args:
            df: 包含 input/output 列的 DataFrame。
            output_path: 输出路径，默认写入 settings.processed_jsonl_path。

        Returns:
            实际写入的文件路径。
        """
        path = Path(output_path) if output_path else self.settings.processed_jsonl_path
        path.parent.mkdir(parents=True, exist_ok=True)

        in_col = self.settings.input_col
        out_col = self.settings.output_col

        logger.info(f"开始转换 {len(df)} 条数据 → {path}")

        with path.open("w", encoding="utf-8") as f:
            for _, row in df.iterrows():
                record = self.row_to_messages(
                    input_text=str(row[in_col]),
                    output_text=str(row[out_col]),
                )
                # ensure_ascii=False 保证中文正常写出
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info(f"转换完成：{len(df)} 条记录已写入 {path}")
        return path

    def convert_split(
        self,
        df: pd.DataFrame,
        output_path: str | Path,
    ) -> Path:
        """将已划分的子集（train/val/test）写入指定路径。

        与 convert() 相同逻辑，提供语义更清晰的别名。
        """
        return self.convert(df, output_path)


def convert_to_jsonl(
    df: pd.DataFrame,
    output_path: str | Path | None = None,
    settings: Settings | None = None,
) -> Path:
    """模块级便捷函数。

    Example:
        >>> path = convert_to_jsonl(df, "data/processed/data.jsonl")
    """
    return JsonlConverter(settings).convert(df, output_path)
