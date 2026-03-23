"""
run_convert.py —— 仅执行数据转换（Excel → JSONL）

适用场景：数据已经清洗好，只需要重新生成 JSONL 格式

用法：
    uv run python scripts/run_convert.py --input data/raw/sample.xlsx
    uv run python scripts/run_convert.py --input data/raw/sample.xlsx --output data/processed/my_data.jsonl
    uv run python scripts/run_convert.py --input data/raw/sample.xlsx --skip-validation
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
from src.loader import ExcelLoader
from src.validator import DataValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_convert")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="data-master：Excel → JSONL 格式转换",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="输入 Excel 文件路径",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="输出 JSONL 路径（默认：data/processed/data.jsonl）",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="跳过校验步骤（不推荐，仅用于已清洗数据）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_settings()

    print("\n🔄 run_convert：数据转换")
    print(f"   运行时间：{cfg.run_timestamp}")
    print(f"   输入：{args.input.resolve()}\n")

    # Step 1：加载
    loader = ExcelLoader(cfg)
    df = loader.load(args.input)
    print(f"   加载完成：{len(df)} 条")

    # Step 2：可选校验
    if not args.skip_validation:
        validator = DataValidator(cfg)
        result = validator.validate(df)
        print(result.summary())
        if not result.is_valid:
            print("❌ 校验失败，终止")
            sys.exit(1)
        df = result.cleaned_df
    else:
        print("   ⚠️  已跳过校验步骤")

    # Step 3：转换
    converter = JsonlConverter(cfg)
    output_path = converter.convert(df, args.output)
    print(f"\n✅ 转换完成：{len(df)} 条 → {output_path}")


if __name__ == "__main__":
    main()
