"""
数据分析模块

职责：
1. 统计标签分布（数量 + 占比）
2. 统计 input 文本长度（平均、最大、最小、分位数）
3. 生成可读报告（打印到控制台 / 写入 txt 文件）

设计：
- AnalysisReport 是纯数据对象，便于序列化或后续扩展（如写入 JSON）
- DataAnalyzer 负责分析逻辑，report_to_text() 负责格式化输出
- 支持对全量数据分析，也支持对 train/val/test 子集分别分析
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass
class LabelStats:
    """单个标签的统计信息。"""

    label: str
    count: int
    ratio: float  # 占总数的百分比（0~100）


@dataclass
class LengthStats:
    """文本长度统计信息（字符数）。"""

    mean: float
    max: int
    min: int
    p50: float   # 中位数
    p90: float   # 90 分位数
    p99: float   # 99 分位数


@dataclass
class AnalysisReport:
    """完整的分析报告对象。

    Attributes:
        total: 总数据条数。
        label_stats: 各标签统计列表。
        input_length_stats: input 字段长度统计。
        dataset_name: 数据集名称（用于报告标题）。
    """

    total: int
    label_stats: list[LabelStats]
    input_length_stats: LengthStats
    dataset_name: str = "全量数据"

    def to_text(self) -> str:
        """将报告格式化为纯文本字符串。"""
        sep = "=" * 45
        lines = [
            sep,
            f"  数据分析报告 — {self.dataset_name}",
            sep,
            f"  总条数：{self.total}",
            "",
            "  ── 标签分布 ──────────────────────────",
        ]

        for stat in self.label_stats:
            bar = "█" * int(stat.ratio / 5)  # 每 5% 一个 ██
            lines.append(
                f"  {stat.label:8s}  {stat.count:5d} 条  "
                f"({stat.ratio:5.1f}%)  {bar}"
            )

        lines += [
            "",
            "  ── input 文本长度（字符数）─────────",
            f"  平均值   : {self.input_length_stats.mean:.1f}",
            f"  最大值   : {self.input_length_stats.max}",
            f"  最小值   : {self.input_length_stats.min}",
            f"  中位数   : {self.input_length_stats.p50:.1f}",
            f"  P90      : {self.input_length_stats.p90:.1f}",
            f"  P99      : {self.input_length_stats.p99:.1f}",
            sep,
        ]
        return "\n".join(lines)


class DataAnalyzer:
    """数据分析器。

    Args:
        settings: 全局配置对象，默认使用单例。
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def analyze(
        self, df: pd.DataFrame, dataset_name: str = "全量数据"
    ) -> AnalysisReport:
        """对 DataFrame 执行统计分析，返回 AnalysisReport。

        Args:
            df: 包含 input/output 列的 DataFrame。
            dataset_name: 报告标题中显示的名称。
        """
        out_col = self.settings.output_col
        in_col = self.settings.input_col
        total = len(df)

        # ── 标签分布 ──
        label_stats = self._compute_label_stats(df[out_col], total)

        # ── 文本长度统计 ──
        lengths = df[in_col].astype(str).str.len()
        length_stats = LengthStats(
            mean=round(lengths.mean(), 2),
            max=int(lengths.max()),
            min=int(lengths.min()),
            p50=float(lengths.quantile(0.50)),
            p90=float(lengths.quantile(0.90)),
            p99=float(lengths.quantile(0.99)),
        )

        return AnalysisReport(
            total=total,
            label_stats=label_stats,
            input_length_stats=length_stats,
            dataset_name=dataset_name,
        )

    def analyze_splits(
        self,
        train: pd.DataFrame,
        val: pd.DataFrame,
        test: pd.DataFrame,
    ) -> list[AnalysisReport]:
        """对 train/val/test 分别进行分析，返回三份报告。"""
        return [
            self.analyze(train, "train"),
            self.analyze(val, "val"),
            self.analyze(test, "test"),
        ]

    def print_report(self, report: AnalysisReport) -> None:
        """将报告打印到控制台。"""
        print(report.to_text())

    def save_report(
        self,
        reports: list[AnalysisReport],
        output_path: str | Path | None = None,
    ) -> Path:
        """将一或多份报告写入 txt 文件。

        Args:
            reports: 报告列表（可传单个报告用列表包裹）。
            output_path: 输出路径，默认用 settings.report_path。

        Returns:
            实际写入路径。
        """
        path = Path(output_path) if output_path else self.settings.report_path
        path.parent.mkdir(parents=True, exist_ok=True)

        content = "\n\n".join(r.to_text() for r in reports)
        path.write_text(content, encoding="utf-8")

        logger.info(f"分析报告已写入：{path}")
        return path

    # ──────────────────────────────────────────────
    # 私有辅助方法
    # ──────────────────────────────────────────────

    def _compute_label_stats(
        self, labels: pd.Series, total: int
    ) -> list[LabelStats]:
        """计算每个标签的数量和占比。"""
        counts = labels.value_counts()
        return [
            LabelStats(
                label=label,
                count=int(cnt),
                ratio=round(cnt / total * 100, 2) if total > 0 else 0.0,
            )
            for label, cnt in counts.items()
        ]


def analyze(
    df: pd.DataFrame,
    dataset_name: str = "全量数据",
    settings: Settings | None = None,
) -> AnalysisReport:
    """模块级便捷函数。

    Example:
        >>> report = analyze(df)
        >>> print(report.to_text())
    """
    return DataAnalyzer(settings).analyze(df, dataset_name)
