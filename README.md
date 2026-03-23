# data-master

> 大模型训练数据处理与管理工具，专为 **Qwen3-8B 意图分类微调**设计。
>
> 标签：`寿险意图` / `拒识`

---

## 项目结构

```
data-master/
├── pyproject.toml          # uv 项目配置 & 依赖
├── data/
│   ├── raw/                # 原始 Excel 数据放这里
│   ├── processed/          # 全量 data.jsonl（转换后）
│   └── output/             # train/val/test.jsonl + 分析报告
├── src/
│   ├── config/             # 全局配置（路径、标签、比例等）
│   ├── loader/             # Excel 读取
│   ├── validator/          # 空值/标签/去重校验
│   ├── converter/          # DataFrame → JSONL
│   ├── splitter/           # 分层抽样划分
│   └── analyzer/           # 统计分析 & 报告
└── scripts/
    ├── run_pipeline.py     # 一键完整流水线
    ├── run_convert.py      # 仅转换
    └── run_split.py        # 仅划分
```

---

## 快速开始

### 1. 安装依赖（使用 uv）

```bash
# 安装 uv（如果没有）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 在项目目录下同步依赖
cd data-master
uv sync
```

### 2. 准备数据

将 Excel 文件放入 `data/raw/`，格式要求：

| input          | output |
|----------------|--------|
| 我想买一份寿险  | 寿险意图 |
| 今天天气怎么样  | 拒识     |
| 请帮我理赔      | 寿险意图 |

> 列名必须是 `input` 和 `output`，标签只能是 `寿险意图` 或 `拒识`。

### 3. 运行

#### 一键完整流水线（推荐）

```bash
uv run python scripts/run_pipeline.py --input data/raw/sample.xlsx
```

执行步骤：加载 → 校验/清洗 → 转换 JSONL → 分层划分 → 分析报告

#### 仅转换（Excel → JSONL）

```bash
uv run python scripts/run_convert.py --input data/raw/sample.xlsx
# 自定义输出路径
uv run python scripts/run_convert.py --input data/raw/sample.xlsx --output data/processed/my_data.jsonl
```

#### 仅划分（重新切分比例）

```bash
uv run python scripts/run_split.py --input data/raw/sample.xlsx
# 自定义比例（三者之和须为 1.0）
uv run python scripts/run_split.py --input data/raw/sample.xlsx --train 0.7 --val 0.15 --test 0.15
```

---

## 输出格式（JSONL）

每行一条训练样本，符合 Qwen3/LLaMA-Factory 微调格式：

```json
{
  "messages": [
    {"role": "system",    "content": "你是一个意图分类模型，只能输出：寿险意图 或 拒识"},
    {"role": "user",      "content": "我想了解一下万能险"},
    {"role": "assistant", "content": "寿险意图"}
  ]
}
```

---

## 配置说明

所有配置集中在 `src/config/settings.py`，无需修改业务代码：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `valid_labels` | `["寿险意图", "拒识"]` | 合法标签列表 |
| `system_prompt` | `"你是一个意图分类模型..."` | 系统提示词 |
| `train_ratio` | `0.8` | 训练集比例 |
| `val_ratio` | `0.1` | 验证集比例 |
| `test_ratio` | `0.1` | 测试集比例 |
| `random_seed` | `42` | 随机种子 |

也支持通过**环境变量**覆盖路径：

```bash
DATA_RAW_DIR=/your/path uv run python scripts/run_pipeline.py --input ...
```

---

## 模块设计原则

- **配置集中**：所有路径和参数通过 `Settings` 管理，不硬编码
- **校验与业务分离**：`ValidationResult` 返回结果对象，调用方决定是否中止
- **可复现**：所有随机操作均受 `random_seed` 控制
- **分层抽样**：`DataSplitter` 保证 train/val/test 中各标签比例一致
- **回退机制**：数据量不足时自动降级为随机划分，并打印警告

---

## 扩展示例

### 在代码中调用各模块

```python
from src.config import get_settings
from src.loader import load_excel
from src.validator import validate
from src.converter import convert_to_jsonl
from src.splitter import split_data
from src.analyzer import analyze

cfg = get_settings()
df = load_excel("data/raw/sample.xlsx")
result = validate(df)
split = split_data(result.cleaned_df)
convert_to_jsonl(split.train, cfg.train_jsonl_path)
report = analyze(result.cleaned_df)
print(report.to_text())
```
