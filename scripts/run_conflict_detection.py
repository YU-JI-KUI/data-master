"""
run_conflict_detection.py —— 语义冲突检测入口脚本

功能：
  读取 Excel 数据，检测【新增拒识数据（拒识_new）】中
  与【寿险意图数据】语义高度相似的样本（cosine similarity ≥ threshold）。

  检测结果输出为 Excel：data/output/high_risk_samples.xlsx
  字段：input | similarity | similar_text

典型用法：
  # 使用 config.yaml 默认配置
  uv run python scripts/run_conflict_detection.py --input data/raw/sample.xlsx

  # 临时覆盖阈值和 TopK
  uv run python scripts/run_conflict_detection.py \\
    --input data/raw/sample.xlsx \\
    --threshold 0.85 \\
    --topk 3

  # 强制重新计算寿险 embedding（忽略缓存）
  uv run python scripts/run_conflict_detection.py \\
    --input data/raw/sample.xlsx \\
    --refresh-cache

前置条件：
  1. Excel 中 output 列须含 "寿险意图" 和 "拒识_new" 两种标签
  2. config.yaml 中 conflict_detection.embedding.model_path 指向本地模型目录
  3. 已执行 uv sync 安装依赖（sentence-transformers, faiss-cpu）
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

import yaml

from src.loader import ExcelLoader
from src.pipelines.conflict_detection_pipeline import (
    ConflictDetectionConfig,
    ConflictDetectionPipeline,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_conflict_detection")


def load_conflict_config(
    config_path: Path,
    threshold_override: float | None = None,
    topk_override: int | None = None,
    model_path_override: str | None = None,
) -> ConflictDetectionConfig:
    """从 config.yaml 读取 conflict_detection 节并构建配置对象。

    命令行参数优先级 > config.yaml。
    """
    raw: dict = {}
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    cd_cfg: dict = raw.get("conflict_detection", {})
    config = ConflictDetectionConfig.from_yaml_dict(cd_cfg)

    # ── 命令行参数覆盖 ──
    if threshold_override is not None:
        config.threshold = threshold_override
    if topk_override is not None:
        config.topk = topk_override
    if model_path_override is not None:
        config.model_path = model_path_override

    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="data-master：新拒识数据语义冲突检测",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="输入 Excel 文件路径（须含 input / output 列，output 含 '拒识_new' 标签）",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        metavar="FLOAT",
        help="相似度判定阈值，覆盖 config.yaml（默认 0.9）",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=None,
        metavar="INT",
        help="FAISS 检索最近邻数，覆盖 config.yaml（默认 1）",
    )
    parser.add_argument(
        "--model-path",
        type=str,
        default=None,
        metavar="PATH",
        help="本地 embedding 模型路径，覆盖 config.yaml",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="强制重新计算寿险 embedding（忽略已有缓存文件）",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=_PROJECT_ROOT / "config.yaml",
        metavar="PATH",
        help="YAML 配置文件路径（默认：项目根目录 config.yaml）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # ── 加载配置 ──
    config = load_conflict_config(
        config_path=args.config,
        threshold_override=args.threshold,
        topk_override=args.topk,
        model_path_override=args.model_path,
    )

    print("\n🔎 语义冲突检测启动")
    print(f"   输入文件    ：{args.input.resolve()}")
    print(f"   Embedding   ：{config.model_path}")
    print(f"   相似度阈值  ：{config.threshold}")
    print(f"   TopK        ：{config.topk}")
    print(f"   寿险缓存    ：{config.life_cache_path}")
    print(f"   输出路径    ：{config.output_path}")

    # ── 强制刷新缓存 ──
    if args.refresh_cache:
        cache_path = Path(config.life_cache_path)
        if not cache_path.is_absolute():
            cache_path = _PROJECT_ROOT / cache_path
        if cache_path.exists():
            cache_path.unlink()
            print(f"\n🗑️  已删除旧缓存：{cache_path}")
        else:
            print("\n💡 缓存文件不存在，无需删除。")

    # ── 加载 Excel（支持多 sheet，自动忽略多余列）──
    print("\n📂 加载 Excel 数据...")
    # 直接用 pandas 读取，不走现有 ExcelLoader（标签不在 valid_labels 里会报错）
    import pandas as pd  # noqa: PLC0415
    sheets: dict = pd.read_excel(args.input, sheet_name=None, engine="openpyxl")
    frames = []
    in_col  = config.input_col
    out_col = config.output_col
    for sheet_name, raw_df in sheets.items():
        required = [in_col, out_col]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.warning(f"Sheet '{sheet_name}' 缺少列 {missing}，已跳过")
            continue
        df_sheet = raw_df[[in_col, out_col]].copy()
        df_sheet[in_col]  = df_sheet[in_col].astype(str).str.strip()
        df_sheet[out_col] = df_sheet[out_col].astype(str).str.strip()
        frames.append(df_sheet)

    if not frames:
        print("❌ 没有找到包含 input/output 列的 sheet，请检查 Excel 文件。")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    print(f"   总计加载：{len(df)} 条记录")

    # 打印标签分布，便于确认数据
    label_counts = df[out_col].value_counts()
    for label, count in label_counts.items():
        print(f"     {label}：{count} 条")

    # ── 运行检测流水线 ──
    pipeline = ConflictDetectionPipeline(config, project_root=_PROJECT_ROOT)
    try:
        result = pipeline.run(df)
    except ValueError as e:
        print(f"\n❌ 数据问题：{e}")
        sys.exit(1)
    except RuntimeError as e:
        print(f"\n❌ 配置问题：{e}")
        sys.exit(1)

    # ── 最终摘要 ──
    if result.high_risk_df.empty:
        print("\n✅ 未发现高风险样本，新拒识数据质量良好。")
    else:
        print(f"\n⚠️  发现 {result.high_risk_count} 条高风险样本，请人工审核。")
        print(f"   结果文件：{config.output_path}")

    print("\n✨ 检测完成！")


if __name__ == "__main__":
    main()
