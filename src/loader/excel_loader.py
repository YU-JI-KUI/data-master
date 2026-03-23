"""
Excel 数据加载模块

职责：将 .xlsx 文件读取为标准化的 pandas DataFrame。
- 仅保留 input / output 两列
- 将列值统一转为 str 并 strip 空白
- 提供便捷函数 load_excel() 供外部直接调用
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


class ExcelLoader:
    """Excel 文件加载器。

    Args:
        settings: 全局配置对象，默认使用单例。
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def load(self, file_path: str | Path) -> pd.DataFrame:
        """读取 Excel 文件，返回仅含 input/output 列的 DataFrame。

        Args:
            file_path: Excel 文件路径（相对或绝对路径均可）。

        Returns:
            包含 'input' 和 'output' 两列的 DataFrame。

        Raises:
            FileNotFoundError: 文件不存在时抛出。
            ValueError: 缺少必要列时抛出。
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"找不到 Excel 文件：{path.resolve()}")

        logger.info(f"正在加载 Excel 文件：{path}")
        df = pd.read_excel(path, engine="openpyxl")

        # ── 检查必要列是否存在 ──
        required_cols = [self.settings.input_col, self.settings.output_col]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Excel 缺少必要列：{missing}，当前列：{list(df.columns)}"
            )

        # ── 只保留需要的两列，避免多余列干扰后续处理 ──
        df = df[required_cols].copy()

        # ── 统一转 str 并去除首尾空白，防止类型不一致 ──
        df[self.settings.input_col] = (
            df[self.settings.input_col].astype(str).str.strip()
        )
        df[self.settings.output_col] = (
            df[self.settings.output_col].astype(str).str.strip()
        )

        logger.info(f"加载完成，共 {len(df)} 条记录")
        return df

    def load_from_raw_dir(self, filename: str) -> pd.DataFrame:
        """从配置的 raw 目录加载指定文件名的 Excel。

        Args:
            filename: 文件名（如 'train_data.xlsx'）。
        """
        return self.load(self.settings.data_raw_dir / filename)


def load_excel(file_path: str | Path, settings: Settings | None = None) -> pd.DataFrame:
    """模块级便捷函数，等同于 ExcelLoader().load(file_path)。

    Example:
        >>> df = load_excel("data/raw/sample.xlsx")
    """
    return ExcelLoader(settings).load(file_path)
