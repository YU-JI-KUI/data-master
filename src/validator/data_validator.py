"""
数据校验模块

职责：
1. 空值检查 —— input/output 列不允许为空或空字符串
2. 标签合法性检查 —— output 只能是配置中的 valid_labels
3. 去重 —— 基于 input 列去重，保留第一条

设计原则：
- 校验结果通过 ValidationResult 返回，不直接抛异常，让调用方决定如何处理
- 每个检查步骤独立封装为私有方法，方便单元测试和扩展
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """校验结果封装对象。

    Attributes:
        is_valid: 整体校验是否通过（无错误时为 True）。
        cleaned_df: 经过清洗后的 DataFrame（去除无效行、去重）。
        null_count: 发现的空值行数。
        invalid_label_count: 标签非法的行数。
        duplicate_count: 去重前后的行数差值。
        errors: 错误描述列表，is_valid=False 时非空。
        warnings: 警告描述列表（不影响 is_valid）。
    """

    is_valid: bool
    cleaned_df: pd.DataFrame
    null_count: int = 0
    invalid_label_count: int = 0
    duplicate_count: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def summary(self) -> str:
        """返回人类可读的校验摘要。"""
        lines = [
            "── 校验摘要 ──────────────────────────",
            f"  整体状态     : {'✅ 通过' if self.is_valid else '❌ 失败'}",
            f"  清洗后行数   : {len(self.cleaned_df)}",
            f"  空值行数     : {self.null_count}（已移除）",
            f"  非法标签行数 : {self.invalid_label_count}（已移除）",
            f"  重复行数     : {self.duplicate_count}（已去重，保留第一条）",
        ]
        if self.errors:
            lines.append("  错误：")
            lines.extend(f"    - {e}" for e in self.errors)
        if self.warnings:
            lines.append("  警告：")
            lines.extend(f"    - {w}" for w in self.warnings)
        lines.append("──────────────────────────────────────")
        return "\n".join(lines)


class DataValidator:
    """数据校验器。

    Args:
        settings: 全局配置对象，默认使用单例。
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """对 DataFrame 执行完整校验流程，返回 ValidationResult。

        流程：空值检查 → 标签检查 → 去重

        Args:
            df: 原始 DataFrame，需包含 input/output 列。
        """
        errors: list[str] = []
        warnings: list[str] = []
        original_len = len(df)

        # ── Step 1：空值检查 ──
        df, null_count = self._check_nulls(df)
        if null_count > 0:
            warnings.append(f"发现 {null_count} 行空值，已移除")
            logger.warning(f"移除空值行：{null_count} 行")

        # ── Step 2：标签合法性检查 ──
        df, invalid_count = self._check_labels(df)
        if invalid_count > 0:
            warnings.append(
                f"发现 {invalid_count} 行非法标签，已移除"
                f"（合法标签：{self.settings.valid_labels}）"
            )
            logger.warning(f"移除非法标签行：{invalid_count} 行")

        # ── Step 3：去重（基于 input 列） ──
        df, dup_count = self._deduplicate(df)
        if dup_count > 0:
            warnings.append(f"发现 {dup_count} 条重复 input，已去重（保留第一条）")
            logger.warning(f"去重：移除 {dup_count} 条重复")

        # ── 判断整体是否通过：清洗后必须有数据 ──
        if len(df) == 0:
            errors.append("校验后数据为空，请检查原始数据")

        is_valid = len(errors) == 0
        logger.info(
            f"校验完成：原始 {original_len} 条 → 清洗后 {len(df)} 条，"
            f"is_valid={is_valid}"
        )

        return ValidationResult(
            is_valid=is_valid,
            cleaned_df=df.reset_index(drop=True),
            null_count=null_count,
            invalid_label_count=invalid_count,
            duplicate_count=dup_count,
            errors=errors,
            warnings=warnings,
        )

    # ──────────────────────────────────────────────
    # 私有方法：各检查步骤独立封装
    # ──────────────────────────────────────────────

    def _check_nulls(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """检查并移除 input/output 任一列为空的行。

        Returns:
            (清洗后 df, 移除的空值行数)
        """
        in_col = self.settings.input_col
        out_col = self.settings.output_col

        # 将空字符串也视为 NaN（因为 load 时 astype(str) 可能将 NaN 转为 'nan'）
        df = df.replace({"nan": pd.NA, "": pd.NA, "None": pd.NA})

        null_mask = df[in_col].isna() | df[out_col].isna()
        null_count = int(null_mask.sum())
        return df[~null_mask].copy(), null_count

    def _check_labels(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """检查 output 列标签是否合法，移除非法行。

        Returns:
            (清洗后 df, 非法标签行数)
        """
        out_col = self.settings.output_col
        valid_mask = df[out_col].isin(self.settings.valid_labels)
        invalid_count = int((~valid_mask).sum())

        if invalid_count > 0:
            # 记录非法标签值，便于排查
            bad_labels = df.loc[~valid_mask, out_col].unique().tolist()
            logger.debug(f"非法标签值：{bad_labels}")

        return df[valid_mask].copy(), invalid_count

    def _deduplicate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """基于 input 列去重，保留第一条。

        Returns:
            (去重后 df, 移除的重复行数)
        """
        in_col = self.settings.input_col
        before = len(df)
        df = df.drop_duplicates(subset=[in_col], keep="first")
        dup_count = before - len(df)
        return df.copy(), dup_count


def validate(df: pd.DataFrame, settings: Settings | None = None) -> ValidationResult:
    """模块级便捷函数，等同于 DataValidator().validate(df)。

    Example:
        >>> result = validate(df)
        >>> print(result.summary())
    """
    return DataValidator(settings).validate(df)
