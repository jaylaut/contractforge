# ContractForge —— 智能合同审查与审批协作平台

> 在 RAG 平台（`rag-from-llm-ai`）与 Agent 平台（`agentforge`）之上的第三个项目：从「知识问答」走向「业务闭环」，做一个**真实业务场景、企业级、全栈**的智能合同审查与审批协作系统。

## 项目定位

- **核心能力**：文档审查 + 结构化抽取 + 规则校验 + 审批工作流 + 版本追溯
- **本质区别**：前两个项目是「检索+生成」「自主执行」，本项目是「AI 结果如何可靠进入业务审批流、可追溯、可审计」
- **技术原则**：延续「底层手写」，不引入 LangChain/LlamaIndex，复用 agentforge 的 LLM 网关

## 目录结构

```
contractforge/
├── src/contractforge/
│   ├── config/         # 配置（pydantic-settings 单例）
│   ├── parsing/        # 文档解析（pdfplumber/docx → 分条款结构化）
│   ├── extraction/     # 要素抽取（LLM Function Calling → 结构化 JSON）
│   ├── review/         # 双通道审查（规则引擎 + LLM 语义）
│   ├── workflow/       # 审批工作流（状态机 + 权限）
│   ├── storage/        # 数据层（PostgreSQL + pgvector）
│   ├── llm/            # LLM 网关（复用 agentforge）
│   ├── tasks/          # 异步任务（arq 队列）
│   ├── api/            # FastAPI 接口
│   └── utils/          # 工具函数
├── frontend/           # React + TypeScript + Ant Design
├── tests/              # 测试
├── docs/               # 文档（含 dev-guide）
├── scripts/            # 脚本
└── data/               # 合同文件 + 日志（不入 git）
```

## 快速开始

```bash
# 1. 创建虚拟环境
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. 安装依赖
pip install -e .

# 3. 配置环境变量
cp .env.example .env             # 填入 DASHSCOPE_API_KEY 等

# 4. 启动基础设施（PostgreSQL + Redis）
docker compose up -d

# 5. 运行测试
pytest tests/ -v
```

## 详细规划

- 架构与选型：见 [`docs/项目架构.md`](docs/项目架构.md)
- 开发指引：见 `docs/dev-guide/`（各阶段 C0~C5）
- 完整规划文档（需求/踩坑/面试叙事）位于 WorkBuddy 工作区：
  `/Users/jayl/WorkBuddy/2026-10-17-23-00-29/output/07_智能合同审查平台项目规划.md`
