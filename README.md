# Antigravity External Workers（DeepSeek Worker + OpenCode Worker）

两个完全独立、即插即用的 **Google Antigravity** 外部编程子代理插件，让 Antigravity 主模型专注做**架构思考、规划方案与最终验收**，把繁重的源码检索、多文件编写、测试与修复交给外部 CLI 子代理执行，大幅节省主模型额度并支持最多 **6 路并行拆单**。

- **`plugins/dsh-worker`（DeepSeek Worker）**：调用 DeepSeek 官方 CLI（`@deepseek-ai/dsh` headless 模式），支持 **native / PTC（Programmatic Tool Calling）** 双模式自动切换与实时侧边栏监控面板 **`DSH Monitor`**。
- **`plugins/opencode-monitor`（OpenCode Worker）**：调用本地 **OpenCode CLI**，支持实时 SSE 流式监控面板 **`OpenCode Monitor`**。

两个插件彼此完全解耦：**自带规则 + 技能 + 监控面板**。平时不用哪个，直接在 Antigravity 插件管理里关掉对应开关，就会彻底关闭该插件的全部功能与上下文占用。

---

## 核心功能一览

- **主从分工省额度**：主模型只做规划与验收，繁重读写/改代码/跑测试全交外部 Worker。
- **2～6 路并发拆单**：可拆分的独立模块自动后台并发派发，按任务组 + 横向 Worker 标签页实时监控，支持单任务/整组一键停止与历史清理。
- **实时可视化监控面板**：思维链（支持实时流式与单步展开）、工具调用流、任务书、最终输出、步骤数、Token 用量（`<10 万` 显示精确数字，`≥10 万` 自动转为 `M` 单位，悬停看精确值）、**缓存命中率**、**纯生成输出速度（TPS，自动扣除工具执行耗时）**。
- **DSH PTC 模式支持**：简单单点任务自动用 `native` 模式；多文件改动/重构/复杂排查自动加 `--mode ptc`（加载 `~/.dsh/patches/ptc.yml`），模型以 TypeScript 组合批量工具调用，实测显著降低耗时和 Token。
- **瞬时网络波动自动容错**：遇到临时断连但 CLI 自动恢复完成任务时，标记为 `OK`，并在完成行上方集中汇总中途出现过的错误及时间戳供查阅。

---

## 零基础安装指南（假设电脑里目前只有 Antigravity）

以下步骤以 **Windows 10 / 11（PowerShell）** 为例，全程约 3～5 分钟。

### 第 1 步：安装基础运行环境（Python + Node.js）

打开 PowerShell，先检查是否已有 Python 和 Node.js：

```powershell
python --version
node -v
```

如果提示“无法识别”，用 Windows 自带的 `winget` 一键安装（装完后**重开一个 PowerShell 窗口**生效）：

```powershell
winget install -e --id Python.Python.3.12
winget install -e --id OpenJS.NodeJS.LTS
```

---

### 第 2 步：下载本仓库并安装插件

**方式一：用 Git 克隆（推荐）**

```powershell
git clone https://github.com/Stalker457/antigravity-external-workers.git
cd antigravity-external-workers
```

**方式二：没装 Git，直接下载 ZIP**
1. 点击本页面右上角绿色 **`Code`** → **`Download ZIP`**。
2. 解压到任意文件夹，在文件夹内右键 → **在终端中打开**。

然后运行一键安装脚本：

```powershell
# 两个插件全装（默认）
powershell -ExecutionPolicy Bypass -File .\install.ps1

# 或者只装某一个：
# powershell -ExecutionPolicy Bypass -File .\install.ps1 -Only dsh
# powershell -ExecutionPolicy Bypass -File .\install.ps1 -Only opencode
```

> 脚本会自动把插件复制到 `%USERPROFILE%\.gemini\config\plugins\`，把 PTC 补丁复制到 `%USERPROFILE%\.dsh\patches\ptc.yml`，并自动检查你本机还缺哪些 CLI 工具。

---

### 第 3 步：安装并配置你要用的外部 CLI（二选一，或两个都装）

#### 选项 A：配置 DeepSeek Worker（推荐）

1. **安装官方 DSH CLI（纯 Node 全局版）**

   ```powershell
   npm install -g @deepseek-ai/dsh
   ```

   > 推荐安装这个 npm 全局版，避免桌面端 UAC 管理员提权导致的子进程启动失败。

2. **创建 headless 配置文件**

   把仓库里的 `templates\cordis.patch.example.yml` 复制到 `%USERPROFILE%\.dsh\profiles\headless\cordis.patch.yml`：

   ```powershell
   New-Item -ItemType Directory -Force "$HOME\.dsh\profiles\headless" | Out-Null
   Copy-Item ".\templates\cordis.patch.example.yml" "$HOME\.dsh\profiles\headless\cordis.patch.yml"
   ```

   用记事本或编辑器打开 `$HOME\.dsh\profiles\headless\cordis.patch.yml`，把里面的 `baseURL`、`model`、`apiKeyEnv` 改成你自己的服务商（默认模板已写好 DeepSeek 官方接口 `https://api.deepseek.com/v1`）。

3. **设置你的 API Key（Windows 用户环境变量）**

   ```powershell
   setx DEEPSEEK_API_KEY "sk-替换成你自己的真实APIKey"
   ```

   > `DEEPSEEK_API_KEY` 需与 `cordis.patch.yml` 里的 `apiKeyEnv` 名称一致。**切勿把真实 Key 写进任何代码或仓库里。**

4. **验证 DSH 是否就绪**

   ```powershell
   python "$HOME\.gemini\config\plugins\dsh-worker\skills\dsh-worker\scripts\run_dsh_worker.py" --list-models
   ```

   能看到默认模型和可用模型列表即配置成功！

---

#### 选项 B：配置 OpenCode Worker（可选）

1. 安装 [OpenCode](https://opencode.ai/)（桌面版或 CLI 版均可）。
2. 在 OpenCode 中配置好你的模型提供商和 API Key。
3. 验证是否就绪：

   ```powershell
   python "$HOME\.gemini\config\plugins\opencode-monitor\skills\opencode-worker\scripts\run_worker.py" --list-models
   ```

> 如果你的 `opencode-cli.exe` 不在默认安装目录或 PATH 中，可设置环境变量 `OPENCODE_CLI_PATH` 指向它。

---

### 第 4 步：重启 Antigravity 并启用插件

1. 完全关闭并重新打开 **Antigravity**。
2. 打开 **Plugins Manager（插件管理）**：
   - 能看到 **`DeepSeek Worker`** 和 **`OpenCode Worker`** 两个独立插件。
   - 打开你要用的开关；平时不用的直接关掉即可。
3. 开启后，在右侧辅助面板区域即可打开 **`DSH Monitor`** / **`OpenCode Monitor`** 实时观看子代理工作过程。

---

## 日常怎么用（对话口令）

装好后，新开一个 Antigravity 对话，直接像下面这样说就行：

- **调用 DeepSeek Worker（默认）**：
  - `用 deepseek 帮我实现……`
  - `用 dsh 帮我排查这个仓库的报错并修复`
  - `用 dsh 并行拆单把这几个模块写完并跑通测试`
  - 指定模式：`用 dsh 的 ptc 模式重构……` / `用 dsh 的 native 模式改一下这个小脚本`
- **调用 OpenCode Worker**（需开启 OpenCode Worker 插件）：
  - `用 opencode 帮我重构……`
- **不想等确认直接执行**：
  - `用 dsh 直接执行，不用等我确认：……`

---

## 仓库目录结构

```text
antigravity-external-workers/
├── README.md
├── install.ps1                        # Windows 一键安装与环境体检脚本
├── LICENSE
├── plugins/
│   ├── dsh-worker/                    # DeepSeek Worker 插件（规则 + 技能 + DSH Monitor）
│   └── opencode-monitor/              # OpenCode Worker 插件（规则 + 技能 + OpenCode Monitor）
└── templates/
    ├── ptc.yml                        # DSH PTC 模式开关补丁
    └── cordis.patch.example.yml       # DSH headless 脱敏配置模板
```

## 常见问题

1. **提示找不到 `dsh` 或报权限错误（WinError 740）**
   - 请使用 `npm install -g @deepseek-ai/dsh` 安装纯 Node.js 全局版，不要直接把桌面端 `.exe` 设成“以管理员身份运行”。
2. **刚设置完 `setx DEEPSEEK_API_KEY`，还需要重启电脑吗？**
   - 不需要。`run_dsh_worker.py` 会自动从 Windows 用户注册表（`HKCU\Environment`）动态读取 `apiKeyEnv` 对应的环境变量。
3. **DSH Monitor / OpenCode Monitor 界面没更新怎么办？**
   - 在右侧监控面板右上角点一下 **“重载”** 即可，无需重启 Antigravity。
4. **如何彻底关闭其中一个 Worker？**
   - 在 Antigravity 的 Plugins Manager 里关掉对应插件开关，新开对话即完全停用（面板、技能、规则一并关闭）。
