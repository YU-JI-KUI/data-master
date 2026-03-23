"""
run_pipeline.py —— 一键执行完整数据处理流水线

流程：Excel → 校验 → 转换（全量 JSONL）→ 划分（train/val/test）→ 分析报告

用法：
    uv run python scripts/run_pipeline.py --input data/raw/sample.xlsx
    uv run python scripts/run_pipeline.py --input data/raw/sample.xlsx --no-report
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# ── 将项目根加入 sys.path，使 `src` 包可直接 import ──
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.analyzer import DataAnalyzer
from src.config import get_settings
from src.converter import JsonlConverter
from src.loader import ExcelLoader
from src.splitter import DataSplitter
from src.validator import DataValidator

# ── 日志格式：时间 + 级别 + 模块 + 消息 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="data-master 完整数据处理流水线",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="输入 Excel 文件路径（如 data/raw/sample.xlsx）",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="跳过分析报告步骤",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=None,
        help="训练集比例（默认 0.8）",
    )
    parser.add_argument(
        "--val-ratio",
        type=float,
        default=None,
        help="验证集比例（默认 0.1）",
    )
    parser.add_argument(
        "--test-ratio",
        type=float,
        default=None,
        help="测试集比例（默认 0.1）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_settings()

    # ── 运行时覆盖比例（如命令行传入）──
    if args.train_ratio is not None:
        cfg.train_ratio = args.train_ratio
    if args.val_ratio is not None:
        cfg.val_ratio = args.val_ratio
    if args.test_ratio is not None:
        cfg.test_ratio = args.test_ratio

    print("\n🚀 data-master 流水线启动")
    print(f"   输入文件：{args.input.resolve()}\n")

    # ──────────────────────────────────────────
    # Step 1：加载 Excel
    # ──────────────────────────────────────────
    print("📂 [1/4] 加载 Excel 数据...")
    loader = ExcelLoader(cfg)
    df_raw = loader.load(args.input)
    print(f"   原始数据：{len(df_raw)} 条\n")

    # ──────────────────────────────────────────
    # Step 2：校验 & 清洗
    # ──────────────────────────────────────────
    print("✅ [2/4] 数据校验与清洗...")
    validator = DataValidator(cfg)
    result = validator.validate(df_raw)
    print(result.summary())

    if not result.is_valid:
        print("\n❌ 校验失败，流水线中止。请检查原始数据。")
        sys.exit(1)

    df_clean = result.cleaned_df
    print(f"   清洗后：{len(df_clean)} 条\n")

    # ──────────────────────────────────────────
    # Step 3：转换全量 JSONL
    # ──────────────────────────────────────────
    print("🔄 [3/4] 划分数据集 & 转换 JSONL...")
    splitter = DataSplitter(cfg)
    split_result = splitter.split(df_clean)
    print(split_result.summary())

    converter = JsonlConverter(cfg)
    # 同时写入全量文件和三个子集文件
    converter.convert(df_clean)
    converter.convert_split(split_result.train, cfg.train_jsonl_path)
    converter.convert_split(split_result.val,   cfg.val_jsonl_path)
    converter.convert_split(split_result.test,  cfg.test_jsonl_path)

    print(f"\n   输出目录：{cfg.data_output_dir}")
    print(f"   ├── train.jsonl  ({len(split_result.train)} 条)")
    print(f"   ├── val.jsonl    ({len(split_result.val)} 条)")
    print(f"   └── test.jsonl   ({len(split_result.test)} 条)\n")

    # ──────────────────────────────────────────
    # Step 4：数据分析报告
    # ──────────────────────────────────────────
    if not args.no_report:
        print("📊 [4/4] 生成分析报告...")
        analyzer = DataAnalyzer(cfg)

        # 全量报告
        full_report = analyzer.analyze(df_clean, "全量数据")
        analyzer.print_report(full_report)

        # 各子集报告
        split_reports = analyzer.analyze_splits(
            split_result.train, split_result.val, split_result.test
        )
        report_path = analyzer.save_report([full_report] + split_reports)
        print(f"\n   报告已写入：{report_path}")
    else:
        print("⏭️  [4/4] 已跳过分析报告")

    print("\n✨ 流水线执行完成！")


if __name__ == "__main__":
    main()
