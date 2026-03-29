"""
FAISS 向量索引模块

职责：
- 使用 IndexFlatIP（内积）构建精确索引
- 输入向量已 L2 归一化，内积 = cosine similarity
- 支持 TopK 检索，返回相似度分数和原始索引

为什么用 IndexFlatIP 而不是 IndexFlatL2？
  归一化向量满足：cosine_sim(a, b) = dot(a, b) = inner_product(a, b)
  使用内积索引检索结果等价于 cosine 相似度排名，且分数直接可读（0~1 范围）。
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)


class FaissIndex:
    """基于 FAISS IndexFlatIP 的精确近邻检索器。

    适用场景：
        向量已 L2 归一化时，inner product = cosine similarity，
        使用 IndexFlatIP 可直接得到 cosine 相似度分数（无需额外换算）。

    Note:
        IndexFlatIP 是暴力精确检索，不做近似。对于 25,000 条寿险数据
        规模，速度完全够用，且结果准确，无近似误差。
    """

    def __init__(self) -> None:
        self._index = None
        self._dim: int | None = None
        self._size: int = 0

    # ──────────────────────────────────────────────────────────
    # 构建索引
    # ──────────────────────────────────────────────────────────

    def build(self, embeddings: np.ndarray) -> None:
        """用给定向量矩阵构建 FAISS 内积索引。

        Args:
            embeddings: shape=(n, d) 的 float32 归一化向量矩阵。

        Raises:
            ImportError: 未安装 faiss-cpu 时。
            ValueError:  输入不是二维矩阵时。
        """
        try:
            import faiss  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "请先安装 faiss-cpu：uv add faiss-cpu"
            ) from e

        if embeddings.ndim != 2:
            raise ValueError(
                f"embeddings 必须是二维矩阵，当前 shape={embeddings.shape}"
            )

        embeddings = embeddings.astype(np.float32)
        self._dim = embeddings.shape[1]
        self._size = embeddings.shape[0]

        self._index = faiss.IndexFlatIP(self._dim)
        self._index.add(embeddings)

        logger.info(
            f"FAISS 索引构建完成：{self._size} 条向量，维度 {self._dim}"
        )

    # ──────────────────────────────────────────────────────────
    # 检索
    # ──────────────────────────────────────────────────────────

    def search(
        self,
        query_embeddings: np.ndarray,
        topk: int = 1,
    ) -> tuple[np.ndarray, np.ndarray]:
        """在已构建的索引中检索最相似的 TopK 条目。

        Args:
            query_embeddings: shape=(m, d) 的查询向量矩阵（已归一化）。
            topk:             返回的最近邻数量，默认 1。

        Returns:
            (scores, indices)：
                scores  — shape=(m, topk)，cosine 相似度（0~1）
                indices — shape=(m, topk)，对应的原始数据行索引

        Raises:
            RuntimeError: 索引尚未构建（未调用 build()）时。
        """
        if self._index is None:
            raise RuntimeError("请先调用 build() 构建索引后再检索。")

        query_embeddings = query_embeddings.astype(np.float32)
        scores, indices = self._index.search(query_embeddings, topk)

        logger.info(
            f"检索完成：{len(query_embeddings)} 条查询 × TopK={topk}"
        )
        return scores, indices

    # ──────────────────────────────────────────────────────────
    # 状态查询
    # ──────────────────────────────────────────────────────────

    @property
    def size(self) -> int:
        """索引中的向量数量。"""
        return self._size

    @property
    def dim(self) -> int | None:
        """向量维度（构建前为 None）。"""
        return self._dim
