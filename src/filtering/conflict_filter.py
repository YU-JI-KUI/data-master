"""
语义冲突筛选模块

职责：
- 接收 FAISS 检索结果（scores + indices），结合原始 DataFrame
- 按阈值判定"高风险"样本（新拒识语义 ≥ threshold → 疑似寿险意图）
- 支持 TopK > 1 时输出多条相似参考（逐行展开）
- 输出标准 DataFrame，可直接写入 Excel

风险等级划分（可扩展）：
  HIGH   : similarity ≥ threshold（默认 0.9）
  MEDIUM : 0.8 ≤ similarity < 0.9（预留，当前不过滤）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ConflictResult:
    """筛选结果容器。

    Attributes:
        high_risk_df:  高风险样本 DataFrame（similarity ≥ threshold）。
        total_checked: 检查的新拒识总条数。
        threshold:     使用的判定阈值。
    """

    high_risk_df: pd.DataFrame
    total_checked: int
    threshold: float
    topk: int = 1

    @property
    def high_risk_count(self) -> int:
        """高风险样本去重后的唯一文本数（一条新拒识可能匹配多条寿险）。"""
        if self.high_risk_df.empty:
            return 0
        return self.high_risk_df["input"].nunique()

    def summary(self) -> str:
        """返回可读的检测摘要字符串。"""
        lines = [
            "── 语义冲突检测结果 ──────────────────────────────",
            f"  检查的新拒识条数 : {self.total_checked}",
            f"  判定阈值         : {self.threshold}",
            f"  TopK             : {self.topk}",
            f"  高风险样本数     : {self.high_risk_count} 条（唯一 input）",
            f"  高风险记录行数   : {len(self.high_risk_df)} 行（含 TopK 展开）",
            "──────────────────────────────────────────────────",
        ]
        return "\n".join(lines)


class ConflictFilter:
    """基于 cosine 相似度阈值的语义冲突筛选器。

    Args:
        threshold: 判定为高风险的相似度下界，默认 0.9。
        topk:      每条新拒识检索的最近邻数，对应 FaissIndex.search 的 topk。
    """

    def __init__(self, threshold: float = 0.9, topk: int = 1) -> None:
        if not 0.0 < threshold <= 1.0:
            raise ValueError(f"threshold 必须在 (0, 1] 范围内，当前：{threshold}")
        self.threshold = threshold
        self.topk = topk

    def filter(
        self,
        new_reject_df: pd.DataFrame,
        life_df: pd.DataFrame,
        scores: np.ndarray,
        indices: np.ndarray,
        input_col: str = "input",
    ) -> ConflictResult:
        """筛选出与寿险意图高度相似的新拒识样本。

        Args:
            new_reject_df: 新拒识数据，至少含 input_col 列。
            life_df:       寿险意图数据，至少含 input_col 列。
            scores:        FAISS 检索分数，shape=(n_reject, topk)。
            indices:       FAISS 检索索引，shape=(n_reject, topk)。
            input_col:     文本列列名，默认 "input"。

        Returns:
            ConflictResult，其中 high_risk_df 含以下列：
              - input        : 新拒识文本
              - similarity   : cosine 相似度（保留 4 位小数）
              - similar_text : 最相似寿险文本
        """
        rows: list[dict] = []
        n_reject = len(new_reject_df)
        life_texts = life_df[input_col].tolist()
        reject_texts = new_reject_df[input_col].tolist()

        for i in range(n_reject):
            for k in range(self.topk):
                score = float(scores[i, k])
                idx = int(indices[i, k])

                # FAISS 用 -1 填充不足 topk 的位置（数据量 < topk 时）
                if idx < 0:
                    continue

                if score >= self.threshold:
                    rows.append({
                        "input":        reject_texts[i],
                        "similarity":   round(score, 4),
                        "similar_text": life_texts[idx],
                    })

        high_risk_df = pd.DataFrame(
            rows,
            columns=["input", "similarity", "similar_text"],
        )

        # 按相似度降序排列，便于人工审核时优先看最高风险
        if not high_risk_df.empty:
            high_risk_df = high_risk_df.sort_values(
                "similarity", ascending=False
            ).reset_index(drop=True)

        result = ConflictResult(
            high_risk_df=high_risk_df,
            total_checked=n_reject,
            threshold=self.threshold,
            topk=self.topk,
        )

        logger.info(
            f"筛选完成：{n_reject} 条新拒识中，"
            f"{result.high_risk_count} 条被标记为高风险"
            f"（similarity ≥ {self.threshold}）"
        )
        return result
