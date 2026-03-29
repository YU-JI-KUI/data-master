"""Pipelines 模块：串联各子模块的完整数据处理流水线。"""

from src.pipelines.conflict_detection_pipeline import ConflictDetectionPipeline

__all__ = ["ConflictDetectionPipeline"]
