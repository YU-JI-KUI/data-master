#!/usr/bin/env bash
# ============================================================
# github_init.sh —— 一键创建 GitHub 仓库并推送
# 在本地终端运行（需要已安装并登录 gh CLI）
#
# 用法：
#   cd /path/to/data-master
#   bash scripts/github_init.sh
# ============================================================

set -e  # 任意命令失败即退出

REPO_NAME="data-master"
DESCRIPTION="Qwen3-8B 意图分类微调数据处理工具"

echo "🚀 正在创建 GitHub 仓库：$REPO_NAME"

# 1. 创建 GitHub 远程仓库（私有）
#    如需公开仓库，改为 --public
gh repo create "$REPO_NAME" \
  --private \
  --description "$DESCRIPTION" \
  --source=. \
  --remote=origin \
  --push

echo ""
echo "✅ 完成！仓库地址：$(gh repo view --json url -q .url)"
