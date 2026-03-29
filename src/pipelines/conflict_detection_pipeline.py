"""
语义冲突检测流水线

将 Embedding → FAISS 索引 → 相似度检索 → 阈值筛选 → 输出 Excel 串联成
一个统一入口，调用方只需提供 DataFrame 和配置。

完整流程：
  Step 1  数据拆分    — 按标签区分寿险意图 / 新拒识
  Step 2  Embedding   — 寿险数据（加载缓存 or 重新计算）
                        新拒识数据（始终重新计算）
  Step 3  构建索引    — FAISS IndexFlatIP
  Step 4  检索        — 每条新拒识 → TopK 最近邻
  Step 5  筛选        — similarity ≥ threshold → 高风险
  Step 6  保存        — 写入 Excel

性能优化：
  - 寿险 embedding 缓存（.npy）：首次计算后保存，下次直接加载
  - 新拒识 embedding 每次重新计算（增量逻辑：只算新增，不复用）
  - 全程批处理（batch_size 可配）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.embedding.embedding_model import EmbeddingModel
from src.filtering.conflict_filter import ConflictFilter, ConflictResult
from src.similarity.faiss_index import FaissIndex

logger = logging.getLogger(__name__)


@dataclass
class ConflictDetectionConfig:
    """流水线配置，对应 config.yaml 中 conflict_detection 节。

    Attributes:
        model_path:       本地 embedding 模型路径（必填）。
        batch_size:       每批编码文本数，默认 64。
        life_cache_path:  寿险 embedding 缓存路径（.npy），默认 data/cache/life_embeddings.npy。
        topk:             FAISS 检索最近邻数，默认 1。
        threshold:        高风险判定阈值，默认 0.9。
        life_label:       寿险意图标签值，默认 "寿险意图"。
        new_reject_label: 新拒识标签值，默认 "拒识_new"。
        output_path:      输出 Excel 路径，默认 data/output/high_risk_samples.xlsx。
        input_col:        文本列名，默认 "input"。
        output_col:       标签列名，默认 "output"。
    """

    model_path: str
    batch_size: int = 64
    life_cache_path: str = "data/cache/life_embeddings.npy"
    topk: int = 1
    threshold: float = 0.9
    life_label: str = "寿险意图"
    new_reject_label: str = "拒识_new"
    output_path: str = "data/output/high_risk_samples.xlsx"
    input_col: str = "input"
    output_col: str = "output"

    @classmethod
    def from_yaml_dict(cls, cfg: dict[str, Any]) -> "ConflictDetectionConfig":
        """从 config.yaml 解析出的字典构建配置对象。

        Expected YAML structure::

            conflict_detection:
              embedding:
                model_path: "D:/models/bge-base-zh"
                batch_size: 64
                cache_path: "data/cache/life_embeddings.npy"
              faiss:
                topk: 1
              threshold: 0.9
              labels:
                life: "寿险意图"
                new_reject: "拒识_new"
              output:
                path: "data/output/high_risk_samples.xlsx"
        """
        emb = cfg.get("embedding", {})
        faiss_cfg = cfg.get("faiss", {})
        labels = cfg.get("labels", {})
        output = cfg.get("output", {})

        return cls(
            model_path=emb.get("model_path", ""),
            batch_size=int(emb.get("batch_size", 64)),
            life_cache_path=emb.get("cache_path", "data/cache/life_embeddings.npy"),
            topk=int(faiss_cfg.get("topk", 1)),
            threshold=float(cfg.get("threshold", 0.9)),
            life_label=labels.get("life", "寿险意图"),
            new_reject_label=labels.get("new_reject", "拒识_new"),
            output_path=output.get("path", "data/output/high_risk_samples.xlsx"),
        )


class ConflictDetectionPipeline:
    """语义冲突检测流水线。

    Args:
        config:       流水线配置对象。
        project_root: 项目根目录（用于解析相对路径），默认当前目录。

    Example::

        pipeline = ConflictDetectionPipeline(config)
        result = pipeline.run(df)
        print(result.summary())
    """

    def __init__(
        self,
        config: ConflictDetectionConfig,
        project_root: str | Path | None = None,
    ) -> None:
        self.cfg = config
        self.project_root = Path(project_root) if project_root else Path.cwd()

    def _resolve(self, path: str) -> Path:
        """将相对路径解析为基于 project_root 的绝对路径。"""
        p = Path(path)
        return p if p.is_absolute() else self.project_root / p

    # ──────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────

    def run(self, df: pd.DataFrame) -> ConflictResult:
        """执行完整的语义冲突检测流水线。

        Args:
            df: 包含 input / output 列的 DataFrame，output 列须包含
                life_label（寿险意图）和 new_reject_label（拒识_new）。

        Returns:
            ConflictResult，含高风险 DataFrame 和统计摘要。

        Raises:
            ValueError: 缺少必要标签数据时。
            RuntimeError: model_path 未配置时。
        """
        cfg = self.cfg

        # ── Step 1：数据拆分 ──────────────────────────────────
        print("\n📋 [1/5] 数据拆分...")
        life_df, new_reject_df = self._split_data(df)
        print(f"   寿险意图：{len(life_df)} 条")
        print(f"   新拒识  ：{len(new_reject_df)} 条")

        if len(new_reject_df) == 0:
            raise ValueError(
                f"DataFrame 中没有找到标签为 '{cfg.new_reject_label}' 的数据，"
                "请检查 Excel 中的 output 列值。"
            )

        # ── Step 2：Embedding ────────────────────────────────
        print("\n🔢 [2/5] 向量化...")
        if not cfg.model_path:
            raise RuntimeError(
                "config.yaml 中 conflict_detection.embedding.model_path 未配置，"
                "请填写本地模型路径。"
            )
        model = EmbeddingModel(cfg.model_path, batch_size=cfg.batch_size)
        life_embeddings = self._get_life_embeddings(model, life_df)
        print(f"   寿险向量：{life_embeddings.shape}")

        print("   计算新拒识向量...")
        reject_embeddings = model.encode(
            new_reject_df[cfg.input_col].tolist(), show_progress=True
        )
        print(f"   新拒识向量：{reject_embeddings.shape}")

        # ── Step 3：构建 FAISS 索引 ───────────────────────────
        print("\n🗂️  [3/5] 构建 FAISS 索引...")
        index = FaissIndex()
        index.build(life_embeddings)
        print(f"   索引大小：{index.size} 条，维度：{index.dim}")

        # ── Step 4：相似度检索 ────────────────────────────────
        print(f"\n🔍 [4/5] 检索（TopK={cfg.topk}）...")
        scores, indices = index.search(reject_embeddings, topk=cfg.topk)
        print(f"   检索完成，分数范围：{scores.min():.4f} ~ {scores.max():.4f}")

        # ── Step 5：冲突筛选 ──────────────────────────────────
        print(f"\n⚠️  [5/5] 冲突筛选（threshold={cfg.threshold}）...")
        cf = ConflictFilter(threshold=cfg.threshold, topk=cfg.topk)
        result = cf.filter(
            new_reject_df, life_df, scores, indices,
            input_col=cfg.input_col,
        )
        print(result.summary())

        # ── 保存结果 Excel ────────────────────────────────────
        out_path = self._save_result(result.high_risk_df)
        print(f"\n💾 结果已保存至：{out_path}")

        return result

    # ──────────────────────────────────────────────────────────
    # 私有辅助方法
    # ──────────────────────────────────────────────────────────

    def _split_data(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """按 output 标签拆分为寿险意图 / 新拒识两个子集。"""
        output_col = self.cfg.output_col
        life_df = df[df[output_col] == self.cfg.life_label].reset_index(drop=True)
        new_reject_df = df[df[output_col] == self.cfg.new_reject_label].reset_index(drop=True)
        return life_df, new_reject_df

    def _get_life_embeddings(
        self,
        model: EmbeddingModel,
        life_df: pd.DataFrame,
    ) -> np.ndarray:
        """加载寿险 embedding 缓存；缓存不存在时重新计算并保存。

        缓存路径由 config.life_cache_path 决定（支持相对/绝对路径）。
        只要缓存文件存在就复用，无论条数是否匹配（依赖用户手动管理缓存）。
        """
        cache_path = self._resolve(self.cfg.life_cache_path)
        cached = EmbeddingModel.load_cache(cache_path)

        if cached is not None:
            print(f"   已从缓存加载寿险向量：{cache_path}")
            return cached

        print("   计算寿险向量（首次运行，结果将缓存）...")
        embeddings = model.encode(
            life_df[self.cfg.input_col].tolist(), show_progress=True
        )
        EmbeddingModel.save_cache(embeddings, cache_path)
        return embeddings

    def _save_result(self, high_risk_df: pd.DataFrame) -> Path:
        """将高风险样本 DataFrame 保存为 Excel 文件。"""
        out_path = self._resolve(self.cfg.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if high_risk_df.empty:
            logger.info("高风险样本为空，写入空 Excel。")

        high_risk_df.to_excel(out_path, index=False, engine="openpyxl")
        logger.info(f"高风险样本已写入：{out_path}  ({len(high_risk_df)} 行)")
        return out_path
