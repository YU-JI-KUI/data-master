"""
Embedding 模型封装

职责：
- 从本地路径加载 SentenceTransformer 模型（不依赖外网）
- 批量编码文本，输出 L2 归一化向量（normalize_embeddings=True）
- 支持 batch_size 控制，避免内存溢出
- 支持将向量缓存到 .npy 文件，下次运行直接复用

设计原则：
  纯计算模块，不依赖 Settings；路径、批大小由调用方传入。
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


class EmbeddingModel:
    """本地 SentenceTransformer 模型封装。

    Args:
        model_path: 本地模型目录路径（如 D:/models/bge-base-zh）。
        batch_size: 每批编码的文本数，默认 64。建议根据内存调整。
    """

    def __init__(self, model_path: str | Path, batch_size: int = 64) -> None:
        self.model_path = str(model_path)
        self.batch_size = batch_size
        self._model = None  # 懒加载：第一次 encode 时才真正加载

    def _load(self) -> None:
        """懒加载模型，避免 import 时就触发大文件读取。"""
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "请先安装 sentence-transformers：uv add sentence-transformers"
            ) from e

        logger.info(f"正在加载 Embedding 模型：{self.model_path}")
        self._model = SentenceTransformer(self.model_path)
        logger.info("模型加载完成")

    def encode(
        self,
        texts: list[str],
        show_progress: bool = True,
    ) -> np.ndarray:
        """将文本列表编码为 L2 归一化向量矩阵。

        Args:
            texts:         待编码的文本列表。
            show_progress: 是否显示 tqdm 进度条，默认 True。

        Returns:
            shape=(len(texts), embedding_dim) 的 float32 numpy 数组，已归一化。
        """
        self._load()
        logger.info(f"开始编码 {len(texts)} 条文本（batch_size={self.batch_size}）")
        embeddings = self._model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,   # cosine sim ≈ inner product
            show_progress_bar=show_progress,
            convert_to_numpy=True,
        )
        logger.info(f"编码完成，向量维度：{embeddings.shape}")
        return embeddings.astype(np.float32)

    # ──────────────────────────────────────────────────────────
    # 缓存相关
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def save_cache(embeddings: np.ndarray, cache_path: str | Path) -> None:
        """将向量矩阵保存到 .npy 文件。

        Args:
            embeddings: 待缓存的向量矩阵。
            cache_path: 缓存文件路径（建议后缀 .npy）。
        """
        path = Path(cache_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        np.save(path, embeddings)
        logger.info(f"向量已缓存至：{path}  shape={embeddings.shape}")

    @staticmethod
    def load_cache(cache_path: str | Path) -> np.ndarray | None:
        """从 .npy 文件加载缓存向量，文件不存在时返回 None。

        Args:
            cache_path: 缓存文件路径。

        Returns:
            向量矩阵，或 None（缓存不存在时）。
        """
        path = Path(cache_path)
        if not path.exists():
            logger.info(f"缓存文件不存在，将重新计算：{path}")
            return None
        embeddings = np.load(path).astype(np.float32)
        logger.info(f"已从缓存加载向量：{path}  shape={embeddings.shape}")
        return embeddings
