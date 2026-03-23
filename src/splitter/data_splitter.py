"""
数据划分模块

职责：将清洗后的 DataFrame 按比例划分为 train / val / test 三份。

特性：
- 分层抽样（stratified sampling）：保证每个子集中标签分布与原始数据一致
- 固定随机种子，保证结果可复现
- 返回 SplitResult，包含三个子集及统计信息

依赖：sklearn.model_selection.train_test_split（支持 stratify 参数）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from sklearn.model_selection import train_test_split

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass
class SplitResult:
    """数据划分结果。

    Attributes:
        train: 训练集 DataFrame。
        val:   验证集 DataFrame。
        test:  测试集 DataFrame。
    """

    train: pd.DataFrame
    val: pd.DataFrame
    test: pd.DataFrame

    def summary(self) -> str:
        """返回各子集的大小和标签分布摘要。"""
        lines = ["── 数据划分摘要 ──────────────────────────"]
        for name, df in [("train", self.train), ("val", self.val), ("test", self.test)]:
            label_dist = df["output"].value_counts().to_dict()
            lines.append(f"  {name:6s}: {len(df):5d} 条  {label_dist}")
        lines.append("──────────────────────────────────────────")
        return "\n".join(lines)


class DataSplitter:
    """分层数据划分器。

    Args:
        settings: 全局配置对象，默认使用单例。
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def split(self, df: pd.DataFrame) -> SplitResult:
        """执行分层抽样划分。

        流程：
            1. 先从全量数据中切分出 test 集（比例 = test_ratio）
            2. 再从剩余数据中切分出 val 集（比例 = val_ratio / (train_ratio + val_ratio)）
            3. 剩余即为 train 集

        Args:
            df: 清洗后的 DataFrame，需包含 output 列用于分层。

        Returns:
            SplitResult，包含 train/val/test 三个 DataFrame。
        """
        cfg = self.settings
        out_col = cfg.output_col
        seed = cfg.random_seed

        # ── 检查数据量是否足够 ──
        # 分层抽样要求每个标签在每个子集中至少有 1 条
        label_counts = df[out_col].value_counts()
        min_count = label_counts.min()
        if min_count < 3:
            logger.warning(
                f"标签 '{label_counts.idxmin()}' 仅有 {min_count} 条，"
                f"分层抽样可能失败，将回退到随机划分"
            )

        try:
            result = self._stratified_split(df, seed)
        except ValueError as e:
            logger.warning(f"分层抽样失败（{e}），回退到随机划分")
            result = self._random_split(df, seed)

        logger.info(
            f"数据划分完成：train={len(result.train)}, "
            f"val={len(result.val)}, test={len(result.test)}"
        )
        return result

    def _stratified_split(self, df: pd.DataFrame, seed: int) -> SplitResult:
        """分层抽样划分（优先路径）。"""
        cfg = self.settings
        out_col = cfg.output_col

        # Step 1：从全量切出 test 集
        train_val, test = train_test_split(
            df,
            test_size=cfg.test_ratio,
            random_state=seed,
            stratify=df[out_col],
        )

        # Step 2：从剩余中切出 val 集
        # val 在剩余数据中的比例 = val_ratio / (1 - test_ratio)
        val_ratio_adjusted = cfg.val_ratio / (cfg.train_ratio + cfg.val_ratio)
        train, val = train_test_split(
            train_val,
            test_size=val_ratio_adjusted,
            random_state=seed,
            stratify=train_val[out_col],
        )

        return SplitResult(
            train=train.reset_index(drop=True),
            val=val.reset_index(drop=True),
            test=test.reset_index(drop=True),
        )

    def _random_split(self, df: pd.DataFrame, seed: int) -> SplitResult:
        """无分层的随机划分（回退路径）。"""
        cfg = self.settings

        train_val, test = train_test_split(
            df, test_size=cfg.test_ratio, random_state=seed
        )
        val_ratio_adjusted = cfg.val_ratio / (cfg.train_ratio + cfg.val_ratio)
        train, val = train_test_split(
            train_val, test_size=val_ratio_adjusted, random_state=seed
        )

        return SplitResult(
            train=train.reset_index(drop=True),
            val=val.reset_index(drop=True),
            test=test.reset_index(drop=True),
        )


def split_data(
    df: pd.DataFrame, settings: Settings | None = None
) -> SplitResult:
    """模块级便捷函数。

    Example:
        >>> result = split_data(df)
        >>> print(result.summary())
    """
    return DataSplitter(settings).split(df)
