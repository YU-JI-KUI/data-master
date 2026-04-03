"""
run_split.py —— 仅执行数据划分（Excel → train/val/test）

适用场景：需要重新划分数据集（比如调整比例）

支持新增数据直接注入（config.yaml split.new_data_sheet 配置）：
    若 Excel 中存在指定名称的 sheet（默认 "new"），该 sheet 数据不参与 8:1:1 划分，
    而是全量直接追加到 train 和 val。

用法：
    uv run python scripts/run_split.py --input data/raw/sample.xlsx
    uv run python scripts/run_split.py --input data/raw/sample.xlsx --train 0.7 --val 0.15 --test 0.15
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import get_settings
from src.converter import JsonlConverter
from src.converter.format_schema import get_schema, list_formats
from src.loader import ExcelLoader
from src.splitter import DataSplitter
from src.validator import DataValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_split")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="data-master：数据集划分（生成 train/val/test JSONL）",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="输入 Excel 文件路径",
    )
    parser.add_argument(
        "--train",
        type=float,
        default=None,
        help="训练集比例（默认 0.8）",
    )
    parser.add_argument(
        "--val",
        type=float,
        default=None,
        help="验证集比例（默认 0.1）",
    )
    parser.add_argument(
        "--test",
        type=float,
        default=None,
        help="测试集比例（默认 0.1）",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="随机种子（默认 42）",
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

    # 运行时覆盖配置
    if args.train is not None:
        cfg.train_ratio = args.train
    if args.val is not None:
        cfg.val_ratio = args.val
    if args.test is not None:
        cfg.test_ratio = args.test
    if args.seed is not None:
        cfg.random_seed = args.seed

    schema = get_schema(args.format) if args.format else cfg.output_format

    print("\n✂️  run_split：数据集划分")
    print(f"   运行时间：{cfg.run_timestamp}")
    print(f"   输出格式：{schema.name}")
    print(
        f"   比例 train={cfg.train_ratio} / val={cfg.val_ratio} / test={cfg.test_ratio}"
    )
    print(f"   随机种子：{cfg.random_seed}\n")

    import pandas as pd  # noqa: PLC0415

    # Step 1：加载（支持新增数据 sheet 分离）
    loader = ExcelLoader(cfg)
    new_sheet = cfg.new_data_sheet

    if new_sheet:
        df, df_new = loader.load_separated(args.input, new_sheet)
        if len(df_new) > 0:
            print(f"   常规数据：{len(df)} 条")
            print(f"   新增数据（sheet='{new_sheet}'）：{len(df_new)} 条 → 直接注入 train+val")
        else:
            print(f"   未找到 sheet '{new_sheet}'，所有数据按常规流程处理")
            print(f"   原始数据：{len(df)} 条")
            df_new = pd.DataFrame()
    else:
        df = loader.load(args.input)
        df_new = pd.DataFrame()
        print(f"   原始数据：{len(df)} 条")

    # Step 2：校验
    validator = DataValidator(cfg)

    result = validator.validate(df)
    print(result.summary())
    if not result.is_valid:
        print("❌ 校验失败，终止")
        sys.exit(1)
    df = result.cleaned_df

    df_new_clean = pd.DataFrame()
    if len(df_new) > 0:
        new_result = validator.validate(df_new)
        df_new_clean = new_result.cleaned_df
        print(f"   新增数据清洗后：{len(df_new_clean)} 条\n")

    # Step 3：划分常规数据
    splitter = DataSplitter(cfg)
    split_result = splitter.split(df)
    print(split_result.summary())

    # 新增数据全量追加到 train 和 val（不进 test）
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

    # Step 4：写出三份文件
    converter = JsonlConverter(cfg, schema)
    converter.convert_split(train_df,          cfg.get_train_path(schema))
    converter.convert_split(val_df,            cfg.get_val_path(schema))
    converter.convert_split(split_result.test, cfg.get_test_path(schema))

    ext = schema.file_extension
    ts  = cfg.run_timestamp
    print(f"\n✅ 划分完成，输出目录：{cfg.data_output_dir}")
    print(f"   train_{ts}{ext} : {len(train_df)} 条")
    print(f"   val_{ts}{ext}   : {len(val_df)} 条")
    print(f"   test_{ts}{ext}  : {len(split_result.test)} 条")


if __name__ == "__main__":
    main()
