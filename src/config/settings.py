"""
配置管理模块

优先级（由高到低）：
  1. 环境变量（DATA_RAW_DIR / DATA_PROCESSED_DIR / DATA_OUTPUT_DIR）
  2. 项目根目录下的 config.yaml
  3. 代码内置默认值

每次运行时，输出目录会自动追加时间戳子目录（格式：yyyy-MM-dd HHmmss），
保证多次运行互不覆盖，方便对比不同版本的处理结果。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

# NOTE: FormatSchema 在 __post_init__ 里懒加载，避免循环依赖
# ── 项目根目录（本文件位于 src/config/）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ── 默认 config.yaml 路径
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    """读取 YAML 文件，文件不存在时返回空 dict。"""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@dataclass
class Settings:
    """全局配置。

    通常不直接实例化，通过 get_settings() 获取单例。

    Args:
        config_path: YAML 配置文件路径，默认为项目根目录的 config.yaml。
    """

    config_path: Path = _DEFAULT_CONFIG_PATH

    # ── 以下字段在 __post_init__ 中从 YAML + 环境变量填充 ──

    # 目录路径
    project_root: Path = field(init=False)
    data_raw_dir: Path = field(init=False)
    data_processed_dir: Path = field(init=False)
    data_output_dir: Path = field(init=False)

    # 列名
    input_col: str = field(init=False)
    output_col: str = field(init=False)

    # 标签 & 提示词
    valid_labels: list[str] = field(init=False)
    system_prompt: str = field(init=False)

    # 划分比例
    train_ratio: float = field(init=False)
    val_ratio: float = field(init=False)
    test_ratio: float = field(init=False)
    random_seed: int = field(init=False)

    # 运行时时间戳（每个 Settings 实例固定，整次运行保持一致）
    run_timestamp: str = field(init=False)

    # 输出格式（FormatSchema，懒加载避免循环依赖）
    output_format: Any = field(init=False)

    def __post_init__(self) -> None:
        cfg = _load_yaml(self.config_path)

        self.project_root = _PROJECT_ROOT

        # ── 时间戳：格式 yyyy-MM-dd HHmmss ──
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d %H%M%S")

        # ── 路径：YAML → 环境变量覆盖 ──
        paths = cfg.get("paths", {})
        self.data_raw_dir = Path(
            os.getenv("DATA_RAW_DIR", _PROJECT_ROOT / paths.get("raw", "data/raw"))
        )
        self.data_processed_dir = Path(
            os.getenv(
                "DATA_PROCESSED_DIR",
                _PROJECT_ROOT / paths.get("processed", "data/processed"),
            )
        )
        # output 根目录（不含时间戳），供外部感知基础路径
        self.data_output_dir = Path(
            os.getenv(
                "DATA_OUTPUT_DIR",
                _PROJECT_ROOT / paths.get("output", "data/output"),
            )
        )

        # ── 列名 ──
        columns = cfg.get("columns", {})
        self.input_col = columns.get("input", "input")
        self.output_col = columns.get("output", "output")

        # ── 标签 & 提示词 ──
        self.valid_labels = cfg.get("valid_labels", ["寿险意图", "拒识"])
        self.system_prompt = cfg.get(
            "system_prompt", "你是一个意图分类模型，只能输出：寿险意图 或 拒识"
        )

        # ── 划分比例 ──
        split = cfg.get("split", {})
        self.train_ratio = float(split.get("train", 0.8))
        self.val_ratio = float(split.get("val", 0.1))
        self.test_ratio = float(split.get("test", 0.1))
        self.random_seed = int(split.get("random_seed", 42))

        # ── 校验比例之和 ──
        total = self.train_ratio + self.val_ratio + self.test_ratio
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"config.yaml split 比例之和必须为 1.0，当前为 {total:.4f}"
            )

        # ── 输出格式：从 config.yaml 加载 preset 名称，再解析为 FormatSchema ──
        # 延迟导入避免循环依赖（settings ← converter ← settings）
        from src.converter.format_schema import get_schema, list_formats  # noqa: PLC0415
        fmt_cfg = cfg.get("output_format", {})
        if isinstance(fmt_cfg, str):
            # 兼容写法：output_format: internal
            preset_name = fmt_cfg
        else:
            preset_name = fmt_cfg.get("preset", "openai")
        try:
            self.output_format = get_schema(preset_name)
        except ValueError:
            available = list_formats()
            raise ValueError(
                f"config.yaml output_format.preset 未知：'{preset_name}'，"
                f"可用格式：{available}"
            )

        # ── 确保基础目录存在 ──
        for d in (self.data_raw_dir, self.data_processed_dir, self.data_output_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ── 带时间戳的运行输出目录 ──────────────────────────────
    @property
    def run_output_dir(self) -> Path:
        """当前运行的输出子目录，如 data/output/2026-03-23 173526/"""
        d = self.data_output_dir / self.run_timestamp
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ── 全量 JSONL（写入 processed/，不含时间戳，代表最新版本） ──
    @property
    def processed_jsonl_path(self) -> Path:
        return self.data_processed_dir / "data.jsonl"

    # ── 划分后的三份文件（带时间戳子目录） ──────────────────
    @property
    def train_jsonl_path(self) -> Path:
        return self.run_output_dir / "train.jsonl"

    @property
    def val_jsonl_path(self) -> Path:
        return self.run_output_dir / "val.jsonl"

    @property
    def test_jsonl_path(self) -> Path:
        return self.run_output_dir / "test.jsonl"

    @property
    def report_path(self) -> Path:
        return self.run_output_dir / "analysis_report.txt"


# ── 单例 ────────────────────────────────────────────────────
_settings_instance: Settings | None = None


def get_settings(config_path: Path | None = None) -> Settings:
    """返回全局唯一的 Settings 实例（懒加载单例）。

    Args:
        config_path: 首次调用时可指定自定义 YAML 路径；后续调用忽略此参数。
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings(
            config_path=config_path or _DEFAULT_CONFIG_PATH
        )
    return _settings_instance


def reset_settings() -> None:
    """清除单例缓存（主要用于测试场景）。"""
    global _settings_instance
    _settings_instance = None
