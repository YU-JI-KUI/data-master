# data-master

> 大模型训练数据处理与管理工具，专为 **Qwen3-8B 意图分类微调**设计。
>
> 将原始标注数据（Excel）自动处理成微调所需的 JSONL 格式，并完成清洗、划分、分析全流程。
> 同时提供**语义冲突检测**能力，自动识别新增数据中的标注错误和边界样本。
>
> 支持标签：`寿险意图` / `拒识`

---

## 目录

- [环境要求](#环境要求)
- [安装步骤](#安装步骤)
- [项目结构](#项目结构)
- [第一步：准备数据](#第一步准备数据)
- [第二步：修改配置](#第二步修改配置config-yaml-说明)
- [第三步：运行](#第三步运行)
- [输出文件说明](#输出文件说明)
- [输出格式详解](#输出格式详解)
- [切换平台格式](#切换平台格式)
- [语义冲突检测](#语义冲突检测)
- [常见问题](#常见问题)
- [进阶：在代码中调用各模块](#进阶在代码中调用各模块)

---

## 环境要求

| 项目 | 要求 |
|------|------|
| Python | 3.12 或以上 |
| 包管理器 | [uv](https://github.com/astral-sh/uv)（推荐）或 pip |
| 操作系统 | macOS / Linux / Windows（WSL） |

> **什么是 uv？** uv 是一个极快的 Python 包管理器，用来替代 pip + venv。本项目用它管理依赖和虚拟环境。

---

## 安装步骤

### 1. 安装 uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# 安装后重启终端，或执行：
source ~/.bashrc   # Linux
source ~/.zshrc    # macOS (zsh)
```

安装成功后执行 `uv --version`，能看到版本号即为成功。

### 2. 克隆项目

```bash
git clone https://github.com/YU-JI-KUI/data-master.git
cd data-master
```

### 3. 安装依赖

```bash
uv sync
```

执行后 uv 会自动创建虚拟环境（`.venv/` 目录）并安装所有依赖：

**基础数据处理**
- `pandas` — 数据处理
- `openpyxl` — 读写 Excel 文件
- `scikit-learn` — 分层抽样划分
- `pyyaml` — 读取 YAML 配置文件

**语义冲突检测（`run_conflict_detection.py` 使用）**
- `sentence-transformers` — 本地 Embedding 模型加载与推理
- `faiss-cpu` — 向量相似度检索
- `numpy` — 向量矩阵运算
- `tqdm` — 进度条显示

> **注意：** 不需要手动激活虚拟环境，运行脚本时统一用 `uv run python ...`，uv 会自动使用项目的虚拟环境。

---

## 项目结构

```
data-master/
│
├── config.yaml                  ← ⭐ 唯一需要你修改的配置文件
├── pyproject.toml               ← 项目元信息、依赖声明、脚本入口
│
├── data/
│   ├── raw/                     ← 📥 把你的原始 Excel 文件放在这里
│   ├── cache/                   ← 寿险 Embedding 缓存（自动生成，可删除重建）
│   │   └── life_embeddings.npy
│   ├── processed/               ← 全量转换结果（文件名含时间戳）
│   │   ├── data_2026-03-23_19-13-42.jsonl
│   │   └── cleaned_2026-03-23_19-13-42.xlsx  ← run_clean 输出
│   └── output/                  ← 划分后的训练数据（文件名含时间戳）
│       ├── train_2026-03-23_19-13-42.jsonl
│       ├── val_2026-03-23_19-13-42.jsonl
│       ├── test_2026-03-23_19-13-42.jsonl
│       ├── analysis_report_2026-03-23_19-13-42.txt
│       └── high_risk_samples.xlsx            ← run_conflict_detection 输出
│
├── src/                         ← 核心代码（通常不需要改动）
│   ├── config/                  ← 配置加载，读取 config.yaml
│   ├── loader/                  ← 从 Excel 读取数据（支持多 sheet，忽略多余列）
│   ├── validator/               ← 数据校验：空值/非法标签/去重（保留最后一条）
│   ├── converter/               ← 转换为目标格式，支持多平台格式切换
│   ├── splitter/                ← 分层抽样划分 train/val/test
│   ├── analyzer/                ← 统计分析，生成报告
│   ├── embedding/               ← 本地 SentenceTransformer 封装（支持 .npy 缓存）
│   ├── similarity/              ← FAISS IndexFlatIP 向量检索
│   ├── filtering/               ← 相似度阈值筛选，标记高风险冲突样本
│   └── pipelines/               ← 串联各模块的完整检测流水线
│
└── scripts/                     ← ⭐ 日常使用的入口脚本
    ├── run_pipeline.py           ← 一键完整流水线：加载→清洗→转换→划分→分析
    ├── run_convert.py            ← 仅做格式转换（Excel → JSONL/JSON）
    ├── run_split.py              ← 仅做数据集划分
    ├── run_clean.py              ← 仅做数据清洗，输出 Excel（含自定义列宽）
    └── run_conflict_detection.py ← 语义冲突检测（新拒识 vs 寿险意图）
```

---

## 第一步：准备数据

将原始标注数据保存为 `.xlsx` 格式，放入 `data/raw/` 目录。

> **多 sheet 支持：** Excel 中有多个 sheet 时，程序会自动读取全部 sheet 并合并处理，无需额外配置。每个 sheet 只需包含 `input` 和 `output` 两列，其余列会被自动忽略。不含这两列的 sheet 会跳过并打印警告。

**Excel 格式要求：**

| input | output |
|-------|--------|
| 我想了解一下万能险 | 寿险意图 |
| 请帮我办理理赔手续 | 寿险意图 |
| 今天天气怎么样 | 拒识 |
| 帮我写一首诗 | 拒识 |

**规则：**

- 第一行必须是标题行，列名必须完全一致：`input` 和 `output`
- `input` 列：用户的原始问句，不能为空
- `output` 列：只能填 `寿险意图` 或 `拒识`，其他值会在校验时被过滤掉
- 建议数据量：至少 30 条（每个标签至少 10 条），以保证分层划分能正常执行

---

## 第二步：修改配置（config.yaml 说明）

项目根目录下的 `config.yaml` 是**唯一需要你按需修改的文件**，代码本身不需要改动。

```yaml
# ── 合法标签列表 ──────────────────────────────────────────
# 校验时会检查 output 列的值是否在此列表内，不在的行会被过滤
valid_labels:
  - 寿险意图
  - 拒识

# ── 系统提示词 ───────────────────────────────────────────
# 写入每条 JSONL 的 system message，决定模型的角色定位
# 修改这里不需要改任何代码
system_prompt: "你是一个意图分类模型，只能输出：寿险意图 或 拒识"

# ── Excel 列名映射 & 前缀 ────────────────────────────────────
# 如果你的 Excel 列名不是 input/output，在这里修改即可
# input_prefix：写入训练文件时，统一拼接在每条 input 前面的固定文本
#   留空（""）表示不追加任何前缀
#   示例：input_prefix: "请判断以下文本的意图：\n"
columns:
  input: input
  output: output
  input_prefix: ""

# ── 数据集划分比例 ────────────────────────────────────────
# 三个值相加必须等于 1.0
# random_seed 固定后，每次运行划分结果完全一致（保证可复现）
split:
  train: 0.8    # 80% 用于训练
  val: 0.1      # 10% 用于验证
  test: 0.1     # 10% 用于测试
  random_seed: 42

# ── 数据目录（通常不需要修改）─────────────────────────────
paths:
  raw: data/raw
  processed: data/processed
  output: data/output

# ── 输出格式 ──────────────────────────────────────────────
# 切换不同平台的训练数据格式，目前支持：
#   internal : 内部平台格式（conversations + human 角色 + id 字段）← 当前默认
#   openai   : OpenAI/LLaMA-Factory 标准格式（messages + user 角色）
#   ark      : Ark 平台格式（平铺 JSON 数组，含 instructions 字段）
output_format:
  preset: internal
```

---

## 第三步：运行

所有命令都在项目根目录下执行。

### 方式一：一键完整流水线（推荐新手使用）

```bash
uv run python scripts/run_pipeline.py --input data/raw/你的文件名.xlsx
```

这一条命令会依次执行：

1. **加载** Excel 文件
2. **校验 & 清洗** — 自动过滤空值、非法标签、重复 input
3. **划分** — 按 8:1:1 分层抽样，保证每个子集中标签比例一致
4. **转换** — 生成全量 `data.jsonl` 和 train/val/test 三份 JSONL
5. **分析** — 输出标签分布、文本长度统计报告

**示例输出：**

```
🚀 data-master 流水线启动
   输入文件：/path/to/data/raw/sample.xlsx
   运行时间：2026-03-23 183228
   输出格式：internal

📂 [1/4] 加载 Excel 数据...
   原始数据：300 条

✅ [2/4] 数据校验与清洗...
── 校验摘要 ──────────────────────────
  整体状态     : ✅ 通过
  清洗后行数   : 295
  空值行数     : 2（已移除）
  非法标签行数 : 1（已移除）
  重复行数     : 2（已去重，保留第一条）
──────────────────────────────────────

🔄 [3/4] 划分数据集 & 转换 JSONL...
── 数据划分摘要 ──────────────────────────
  train :   236 条  {'寿险意图': 160, '拒识': 76}
  val   :    30 条  {'寿险意图': 20, '拒识': 10}
  test  :    29 条  {'寿险意图': 19, '拒识': 10}
──────────────────────────────────────────

   输出目录：data/output
   ├── train_2026-03-23_18-32-28.jsonl  (236 条)
   ├── val_2026-03-23_18-32-28.jsonl    (30 条)
   └── test_2026-03-23_18-32-28.jsonl   (29 条)

📊 [4/4] 生成分析报告...
✨ 流水线执行完成！
```

**全部可用参数：**

```bash
uv run python scripts/run_pipeline.py \
  --input data/raw/sample.xlsx \   # 必填：输入文件路径
  --format openai \                # 可选：临时切换输出格式（覆盖 config.yaml）
  --train-ratio 0.7 \              # 可选：训练集比例（覆盖 config.yaml）
  --val-ratio 0.15 \               # 可选：验证集比例
  --test-ratio 0.15 \              # 可选：测试集比例
  --no-report                      # 可选：跳过分析报告生成
```

---

### 方式二：仅转换（Excel → JSONL，不划分）

适用场景：只想生成全量 JSONL，不需要划分。

```bash
uv run python scripts/run_convert.py --input data/raw/sample.xlsx

# 指定输出路径
uv run python scripts/run_convert.py \
  --input data/raw/sample.xlsx \
  --output data/processed/my_data.jsonl

# 跳过校验（仅在数据已确认干净时使用）
uv run python scripts/run_convert.py \
  --input data/raw/sample.xlsx \
  --skip-validation
```

---

### 方式三：仅划分（重新切分已有数据）

适用场景：已有清洗好的数据，想用不同比例重新划分。

```bash
uv run python scripts/run_split.py --input data/raw/sample.xlsx

# 自定义比例（三者之和必须为 1.0）
uv run python scripts/run_split.py \
  --input data/raw/sample.xlsx \
  --train 0.7 --val 0.15 --test 0.15

# 更换随机种子（会得到不同的划分结果）
uv run python scripts/run_split.py \
  --input data/raw/sample.xlsx \
  --seed 123
```

---

## 输出文件说明

每次运行生成的文件名都会自动追加时间戳后缀，格式为 `yyyy-mm-dd_HH-mm-ss`。

```
data/
├── processed/
│   ├── data_2026-03-23_19-13-42.jsonl   ← 第一次运行的全量数据
│   └── data_2026-03-24_09-15-00.jsonl   ← 第二次运行的全量数据
└── output/
    ├── train_2026-03-23_19-13-42.jsonl
    ├── val_2026-03-23_19-13-42.jsonl
    ├── test_2026-03-23_19-13-42.jsonl
    ├── analysis_report_2026-03-23_19-13-42.txt
    ├── train_2026-03-24_09-15-00.jsonl  ← 第二次运行，文件名不同，不会覆盖
    └── ...
```

> 时间戳文件名保证了多次运行的结果互不覆盖，直接通过文件名就能区分每次运行的时间。
> 注：Windows 文件名不允许冒号和空格，故使用 `-` 替代 `:`，`_` 替代空格。

---

## 输出格式详解

本项目支持两种 JSONL 格式，由 `config.yaml` 的 `output_format.preset` 控制。

### internal 格式（内部平台，当前默认）

```json
{
  "id": 1,
  "conversations": [
    {"role": "system",    "context": "你是一个意图分类模型，只能输出：寿险意图 或 拒识"},
    {"role": "human",     "context": "我想了解一下万能险"},
    {"role": "assistant", "context": "寿险意图"}
  ]
}
```

特点：
- 有自增 `id` 字段（从 1 开始）
- 对话数组字段名为 `conversations`
- 用户角色名为 `human`
- 文本字段名为 `context`（内部平台历史遗留问题，平台方暂不修复，故此处适配）

### openai 格式（OpenAI / LLaMA-Factory 标准）

```json
{
  "messages": [
    {"role": "system",    "content": "你是一个意图分类模型，只能输出：寿险意图 或 拒识"},
    {"role": "user",      "content": "我想了解一下万能险"},
    {"role": "assistant", "content": "寿险意图"}
  ]
}
```

特点：
- 无 `id` 字段
- 对话数组字段名为 `messages`
- 用户角色名为 `user`
- 文本字段名为标准的 `content`
- 文件后缀为 `.jsonl`，每行一个 JSON 对象

### ark 格式（Ark 平台）

```json
[
  {
    "instruction": "你是一个意图分类模型，只能输出：寿险意图 或 拒识",
    "input":       "我想了解一下万能险",
    "output":      "寿险意图"
  },
  {
    "instruction": "你是一个意图分类模型，只能输出：寿险意图 或 拒识",
    "input":       "帮我写一首诗",
    "output":      "拒识"
  }
]
```

特点：
- 整个文件是一个 **JSON 数组**（不是 JSONL），格式为 `.json`
- 字段平铺（无嵌套对话数组），使用 `instruction` / `input` / `output`
- 适合直接上传到 Ark 等需要 JSON 数组格式的训练平台

---

## 切换平台格式

### 永久切换（修改 config.yaml）

打开 `config.yaml`，将 `preset` 改为目标格式名：

```yaml
output_format:
  preset: ark   # 可选：internal / openai / ark
```

之后所有运行都使用新格式。

### 临时切换（命令行参数）

不修改配置文件，只对本次运行生效：

```bash
# 使用 openai 格式输出 .jsonl
uv run python scripts/run_pipeline.py --input data/raw/sample.xlsx --format openai

# 使用 ark 格式输出 .json 数组
uv run python scripts/run_pipeline.py --input data/raw/sample.xlsx --format ark
```

### 添加新平台格式

如果将来需要对接新的训练平台，只需在 `src/converter/format_schema.py` 文件末尾追加几行，无需改动任何其他代码。

**conversations 嵌套风格（类似 openai/internal）：**

```python
register(FormatSchema(
    name="新平台名称",             # 用于 --format 参数和 config.yaml
    conversations_key="messages",  # 对话数组的字段名
    role_map={
        "system":    "system",
        "user":      "human",      # 按新平台要求填写角色名
        "assistant": "assistant",
    },
    include_id=True,               # 是否需要 id 字段
    record_style="conversations",
    output_type="jsonl",
    file_extension=".jsonl",
))
```

**flat 平铺风格（类似 ark）：**

```python
register(FormatSchema(
    name="新平台名称",
    # 将内部角色（system/user/assistant）映射到平台字段名
    flat_field_map={"system": "instruction", "user": "input", "assistant": "output"},
    record_style="flat",
    output_type="json_array",      # 整体 JSON 数组
    file_extension=".json",
))
```

注册后即可通过 `--format 新平台名称` 或 config.yaml 使用。

---

## 语义冲突检测

### 什么是语义冲突？

当向训练数据中加入**新增拒识样本**时，部分样本可能：

- 实际语义接近"寿险意图"（标注错误）
- 与已有寿险样本高度相似（边界模糊）

这类样本会污染训练数据，损害模型的决策边界。**语义冲突检测模块**通过 Embedding + FAISS 自动识别这类高风险样本，供人工审核。

---

### 数据格式要求

运行检测前，Excel 的 output 列须包含三种标签：

| output 值 | 含义 |
|-----------|------|
| `寿险意图` | 已有寿险样本（参考库） |
| `拒识` | 已有拒识样本（不参与检测，忽略）|
| `拒识_new` | **新增拒识样本**（检测对象） |

> 寿险意图和拒识_new 标签名可在 config.yaml 中修改。

---

### 第一步：配置 config.yaml

找到 `conflict_detection` 节，填写本地模型路径：

```yaml
conflict_detection:
  embedding:
    model_path: "D:/models/bge-base-zh"   # 改为你的本地模型路径
    batch_size: 64
    cache_path: "data/cache/life_embeddings.npy"
  faiss:
    topk: 1
  threshold: 0.9     # 相似度阈值：≥ 0.9 → 高风险
  labels:
    life: "寿险意图"
    new_reject: "拒识_new"
  output:
    path: "data/output/high_risk_samples.xlsx"
```

> **模型推荐：** `bge-base-zh`（百度开源，中文语义能力强）。从 HuggingFace 下载到本地后填写路径，不依赖外网。

---

### 第二步：运行检测

```bash
# 基本用法（使用 config.yaml 所有配置）
uv run python scripts/run_conflict_detection.py --input data/raw/sample.xlsx

# 临时调低阈值（扩大检测范围）
uv run python scripts/run_conflict_detection.py \
  --input data/raw/sample.xlsx \
  --threshold 0.85

# 检索 TopK=3（每条新拒识返回 3 条最相似寿险样本）
uv run python scripts/run_conflict_detection.py \
  --input data/raw/sample.xlsx \
  --topk 3

# 寿险数据有更新时，强制刷新缓存
uv run python scripts/run_conflict_detection.py \
  --input data/raw/sample.xlsx \
  --refresh-cache
```

---

### 输出结果

检测结果保存至 `data/output/high_risk_samples.xlsx`，包含三列：

| 字段 | 说明 |
|------|------|
| `input` | 新拒识文本 |
| `similarity` | 与寿险样本的 cosine 相似度（0~1，越高越危险）|
| `similar_text` | 最相似的寿险意图文本（供人工对比） |

结果按 similarity 降序排列，优先展示最高风险样本。

---

### 性能说明

| 操作 | 耗时估计 |
|------|---------|
| 首次计算寿险 embedding（25,000 条）| 约 3~8 分钟（CPU）|
| 后续运行（命中缓存）| 数秒 |
| 新拒识 embedding（1,000 条）| 约 10~30 秒（CPU）|
| FAISS 检索 | < 1 秒 |

> 寿险 embedding 会自动缓存至 `data/cache/life_embeddings.npy`，下次运行直接复用，无需重新计算。

---

## 常见问题

**Q：运行后提示"找不到 Excel 文件"？**

检查路径是否正确。建议使用相对于项目根目录的路径，例如：

```bash
# ✅ 正确（相对路径，在项目根目录下运行）
uv run python scripts/run_pipeline.py --input data/raw/sample.xlsx

# ✅ 也正确（绝对路径）
uv run python scripts/run_pipeline.py --input /Users/kris/data/sample.xlsx
```

**Q：校验后数据大量减少，非法标签行数很多？**

说明 Excel 中的 output 列有不符合规范的值。检查标签是否有多余空格、全角/半角问题，或者拼写错误（如「寿险相关」应该是「寿险意图」）。

**Q：重复数据是保留第一条还是最后一条？**

保留**最后一条**。原因是 Excel 中靠后的行代表更新的标注，当同一条 input 有多次标注时，最新的标注记录在最后，保留最后一条可确保最新标注生效。

**Q：提示"分层抽样失败，回退到随机划分"？**

说明某个标签的数据量太少，无法在每个子集中都分到样本。建议每个标签至少准备 30 条以上的数据。

**Q：想修改系统提示词（system prompt）？**

直接修改 `config.yaml` 中的 `system_prompt` 字段即可，不需要改代码：

```yaml
system_prompt: "你是一个新的提示词"
```

**Q：多次运行后 data/output/ 下有很多带时间戳的文件，想清理怎么办？**

手动删除不需要的文件即可，时间戳不同的文件互不影响。建议按时间戳识别保留哪次运行结果，其余文件直接删除。

---

## 进阶：在代码中调用各模块

如果需要在自己的脚本中使用各模块，可以这样调用：

```python
from src.config import get_settings
from src.loader import load_excel
from src.validator import validate
from src.converter import convert_to_jsonl, get_schema
from src.splitter import split_data
from src.analyzer import analyze

# 加载全局配置（会读取 config.yaml）
cfg = get_settings()

# 加载数据
df = load_excel("data/raw/sample.xlsx")

# 校验 & 清洗
result = validate(df)
if not result.is_valid:
    print(result.summary())
    exit(1)

# 划分
split = split_data(result.cleaned_df)
print(split.summary())

# 转换为 JSONL（使用 internal 格式）
schema = get_schema("internal")
convert_to_jsonl(split.train, cfg.train_jsonl_path, schema=schema)

# 生成分析报告
report = analyze(result.cleaned_df)
print(report.to_text())
```
