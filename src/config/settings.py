"""
配置管理模块

所有路径、标签、模型相关配置集中在此，避免硬编码。
通过 get_settings() 获取单例配置对象。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


# ──────────────────────────────────────────────
# 项目根目录：自动推断（本文件位于 src/config/）
# ──────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Settings:
    """全局配置，所有路径均基于 project_root 计算，支持环境变量覆盖。"""

    # ── 项目根 ──
    project_root: Path = _PROJECT_ROOT

    # ── 数据目录 ──
    data_raw_dir: Path = field(default_factory=lambda: _PROJECT_ROOT / "data" / "raw")
    data_processed_dir: Path = field(
        default_factory=lambda: _PROJECT_ROOT / "data" / "processed"
    )
    data_output_dir: Path = field(
        default_factory=lambda: _PROJECT_ROOT / "data" / "output"
    )

    # ── 数据列名 ──
    input_col: str = "input"
    output_col: str = "output"

    # ── 合法标签（意图分类） ──
    valid_labels: list[str] = field(default_factory=lambda: ["寿险意图", "拒识"])

    # ── 系统提示词 ──
    system_prompt: str = "你是一个意图分类模型，只能输出：寿险意图 或 拒识"

    # ── 数据集划分比例 ──
    train_ratio: float = 0.8
    val_ratio: float = 0.1
    test_ratio: float = 0.1

    # ── 随机种子（保证复现性） ──
    random_seed: int = 42

    def __post_init__(self) -> None:
        """初始化后：从环境变量读取覆盖值，并确保目录存在。"""
        # 支持通过环境变量覆盖关键路径，方便 CI/CD 场景
        if env_raw := os.getenv("DATA_RAW_DIR"):
            self.data_raw_dir = Path(env_raw)
        if env_proc := os.getenv("DATA_PROCESSED_DIR"):
            self.data_processed_dir = Path(env_proc)
        if env_out := os.getenv("DATA_OUTPUT_DIR"):
            self.data_output_dir = Path(env_out)

        # 校验比例之和
        total = self.train_ratio + self.val_ratio + self.test_ratio
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"train/val/test 比例之和必须为 1.0，当前为 {total:.4f}"
            )

        # 自动创建目录
        for d in (self.data_raw_dir, self.data_processed_dir, self.data_output_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ── 便捷属性：常用输出文件路径 ──
    @property
    def processed_jsonl_path(self) -> Path:
        """转换后的全量 JSONL 文件。"""
        return self.data_processed_dir / "data.jsonl"

    @property
    def train_jsonl_path(self) -> Path:
        return self.data_output_dir / "train.jsonl"

    @property
    def val_jsonl_path(self) -> Path:
        return self.data_output_dir / "val.jsonl"

    @property
    def test_jsonl_path(self) -> Path:
        return self.data_output_dir / "test.jsonl"

    @property
    def report_path(self) -> Path:
        return self.data_output_dir / "analysis_report.txt"


# ── 单例缓存 ──
_settings_instance: Settings | None = None


def get_settings() -> Settings:
    """返回全局唯一的 Settings 实例（懒加载单例）。"""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
