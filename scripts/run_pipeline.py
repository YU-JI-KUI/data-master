"""
run_pipeline.py —— 一键执行完整数据处理流水线

流程：Excel → 校验 → 转换（全量 JSONL）→ 划分（train/val/test）→ 分析报告

新增数据直接注入（config.yaml split.new_data_sheet 配置）：
    若 Excel 中存在指定名称的 sheet（默认 "new"），该 sheet 数据不参与 8:1:1 划分，
    而是全量直接追加到 train 和 val，确保每条新数据都出现在训练集和验证集中。
    适用于存量数据量大（如 6 万条）、新增数据少（如 100 条）的场景。

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
from src.converter.format_schema import get_schema, list_formats
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
    parser.add_argument(
        "--format",
        type=str,
        default=None,
        metavar="FORMAT",
        help=f"输出格式，临时覆盖 config.yaml 设置（可选值：{list_formats()}）",
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

    # ── 格式覆盖（--format 参数 > config.yaml）──
    schema = get_schema(args.format) if args.format else cfg.output_format

    print("\n🚀 data-master 流水线启动")
    print(f"   输入文件：{args.input.resolve()}")
    print(f"   运行时间：{cfg.run_timestamp}")
    print(f"   输出格式：{schema.name}\n")

    # ──────────────────────────────────────────
    # Step 1：加载 Excel
    # ──────────────────────────────────────────
    print("📂 [1/4] 加载 Excel 数据...")
    loader = ExcelLoader(cfg)

    new_sheet = cfg.new_data_sheet  # 空字符串表示禁用
    if new_sheet:
        df_raw, df_new = loader.load_separated(args.input, new_sheet)
        if len(df_new) > 0:
            print(f"   常规数据：{len(df_raw)} 条")
            print(f"   新增数据（sheet='{new_sheet}'）：{len(df_new)} 条 → 直接注入 train+val\n")
        else:
            print(f"   未找到 sheet '{new_sheet}'，所有数据按常规流程处理")
            print(f"   原始数据：{len(df_raw)} 条\n")
    else:
        df_raw = loader.load(args.input)
        df_new = __import__("pandas").DataFrame()
        print(f"   原始数据：{len(df_raw)} 条\n")

    # ──────────────────────────────────────────
    # Step 2：校验 & 清洗
    # ──────────────────────────────────────────
    print("✅ [2/4] 数据校验与清洗...")
    validator = DataValidator(cfg)

    # 清洗常规数据
    result = validator.validate(df_raw)
    print(result.summary())
    if not result.is_valid:
        print("\n❌ 校验失败，流水线中止。请检查原始数据。")
        sys.exit(1)
    df_clean = result.cleaned_df
    print(f"   清洗后：{len(df_clean)} 条")

    # 清洗新增数据（若存在）
    df_new_clean = __import__("pandas").DataFrame()
    if len(df_new) > 0:
        new_result = validator.validate(df_new)
        df_new_clean = new_result.cleaned_df
        print(f"   新增数据清洗后：{len(df_new_clean)} 条\n")
    else:
        print()

    # ──────────────────────────────────────────
    # Step 3：划分数据集 & 转换
    # ──────────────────────────────────────────
    print("🔄 [3/4] 划分数据集 & 转换 JSONL...")
    splitter = DataSplitter(cfg)
    split_result = splitter.split(df_clean)
    print(split_result.summary())

    # 将新增数据全量追加到 train 和 val（不进 test）
    import pandas as pd  # noqa: PLC0415
    if len(df_new_clean) > 0:
        train_df = pd.concat([split_result.train, df_new_clean], ignore_index=True)
        val_df   = pd.concat([split_result.val,   df_new_clean], ignore_index=True)
        print(
            f"\n   新增数据已注入：train +{len(df_new_clean)} 条 → {len(train_df)} 条，"
            f"val +{len(df_new_clean)} 条 → {len(val_df)} 条"
        )
    else:
        train_df = split_result.train
        val_df   = split_result.val

    # 全量数据 = 常规数据 + 新增数据（用于全量文件和分析）
    df_all = pd.concat([df_clean, df_new_clean], ignore_index=True) if len(df_new_clean) > 0 else df_clean

    converter = JsonlConverter(cfg, schema)
    converter.convert(df_all)
    converter.convert_split(train_df,         cfg.get_train_path(schema))
    converter.convert_split(val_df,           cfg.get_val_path(schema))
    converter.convert_split(split_result.test, cfg.get_test_path(schema))

    ext = schema.file_extension
    ts  = cfg.run_timestamp
    print(f"\n   输出目录：{cfg.data_output_dir}")
    print(f"   ├── train_{ts}{ext}  ({len(train_df)} 条)")
    print(f"   ├── val_{ts}{ext}    ({len(val_df)} 条)")
    print(f"   └── test_{ts}{ext}   ({len(split_result.test)} 条)\n")

    # ──────────────────────────────────────────
    # Step 4：数据分析报告
    # ──────────────────────────────────────────
    if not args.no_report:
        print("📊 [4/4] 生成分析报告...")
        analyzer = DataAnalyzer(cfg)

        # 全量报告（含新增数据）
        full_report = analyzer.analyze(df_all, "全量数据")
        analyzer.print_report(full_report)

        # 各子集报告（train/val 已含新增数据）
        split_reports = analyzer.analyze_splits(
            train_df, val_df, split_result.test
        )
        report_path = analyzer.save_report([full_report] + split_reports)
        print(f"\n   报告已写入：{report_path}")
    else:
        print("⏭️  [4/4] 已跳过分析报告")

    print("\n✨ 流水线执行完成！")


if __name__ == "__main__":
    main()
