"""
Excel 数据加载模块

职责：将 .xlsx 文件读取为标准化的 pandas DataFrame。
- 自动读取所有 sheet，合并为一份数据（每个 sheet 处理逻辑相同）
- 仅保留 input / output 两列，其余列自动忽略
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
        """读取 Excel 文件（所有 sheet），返回仅含 input/output 列的 DataFrame。

        支持多 sheet：自动遍历所有 sheet，只要包含 input/output 列就纳入处理；
        不含必要列的 sheet 会被跳过并打印警告。结果按原始顺序纵向合并，
        重置行索引。

        Args:
            file_path: Excel 文件路径（相对或绝对路径均可）。

        Returns:
            包含 'input' 和 'output' 两列的 DataFrame。

        Raises:
            FileNotFoundError: 文件不存在时抛出。
            ValueError: 所有 sheet 都缺少必要列时抛出。
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"找不到 Excel 文件：{path.resolve()}")

        logger.info(f"正在加载 Excel 文件：{path}")

        # ── 读取所有 sheet，sheet_name=None 返回 {sheet_name: DataFrame} ──
        sheets: dict[str, pd.DataFrame] = pd.read_excel(
            path, sheet_name=None, engine="openpyxl"
        )
        logger.info(f"文件共 {len(sheets)} 个 sheet：{list(sheets.keys())}")

        required_cols = [self.settings.input_col, self.settings.output_col]
        valid_frames: list[pd.DataFrame] = []

        for sheet_name, raw_df in sheets.items():
            # ── 只保留必要列；其余列自动忽略 ──
            missing = [c for c in required_cols if c not in raw_df.columns]
            if missing:
                logger.warning(
                    f"Sheet '{sheet_name}' 缺少列 {missing}，已跳过"
                )
                continue

            df = raw_df[required_cols].copy()

            # ── 统一转 str 并去除首尾空白 ──
            df[self.settings.input_col] = (
                df[self.settings.input_col].astype(str).str.strip()
            )
            df[self.settings.output_col] = (
                df[self.settings.output_col].astype(str).str.strip()
            )

            logger.info(f"Sheet '{sheet_name}'：{len(df)} 条记录")
            valid_frames.append(df)

        if not valid_frames:
            raise ValueError(
                f"Excel 所有 sheet 均缺少必要列 {required_cols}，"
                f"请检查列名配置（config.yaml columns 节）"
            )

        # ── 合并所有有效 sheet，重置行索引 ──
        result = pd.concat(valid_frames, ignore_index=True)
        logger.info(f"加载完成，合并后共 {len(result)} 条记录")
        return result

    def load_from_raw_dir(self, filename: str) -> pd.DataFrame:
        """从配置的 raw 目录加载指定文件名的 Excel（自动处理所有 sheet）。

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
