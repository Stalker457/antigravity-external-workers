<#
.SYNOPSIS
  安装 Antigravity 外部编程子代理插件（DeepSeek Worker / OpenCode Worker）。

.DESCRIPTION
  将 plugins/ 下的自包含插件复制到 %USERPROFILE%\.gemini\config\plugins\，
  将 PTC 补丁模板复制到 %USERPROFILE%\.dsh\patches\ptc.yml，并自动检测本机前置依赖。

.PARAMETER Only
  选择安装范围：all（默认，两个都装）、dsh（仅装 DeepSeek Worker）、opencode（仅装 OpenCode Worker）。

.EXAMPLE
  .\install.ps1
  .\install.ps1 -Only dsh
  .\install.ps1 -Only opencode
#>
[CmdletBinding()]
param(
  [ValidateSet('all', 'dsh', 'opencode')]
  [string]$Only = 'all'
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PluginsTarget = Join-Path $HOME '.gemini\config\plugins'
$DshPatchDir = Join-Path $HOME '.dsh\patches'
$DshHeadlessDir = Join-Path $HOME '.dsh\profiles\headless'

Write-Host '==================================================' -ForegroundColor Cyan
Write-Host ' Antigravity External Workers 插件安装器' -ForegroundColor Cyan
Write-Host '==================================================' -ForegroundColor Cyan
Write-Host "安装范围: $Only"
Write-Host "插件目标目录: $PluginsTarget"
Write-Host ''

New-Item -ItemType Directory -Force -Path $PluginsTarget | Out-Null

function Install-PluginDir {
  param(
    [string]$Name,
    [string]$Label
  )
  $src = Join-Path $RepoRoot "plugins\$Name"
  $dst = Join-Path $PluginsTarget $Name
  if (-not (Test-Path $src)) {
    throw "未找到源插件目录: $src"
  }
  if (Test-Path $dst) {
    Remove-Item -Recurse -Force $dst
  }
  Copy-Item -Recurse -Force $src $dst
  Write-Host "[OK] 已安装 $Label -> $dst" -ForegroundColor Green
}

if ($Only -in @('all', 'dsh')) {
  Install-PluginDir -Name 'dsh-worker' -Label 'DeepSeek Worker (dsh-worker)'

  New-Item -ItemType Directory -Force -Path $DshPatchDir | Out-Null
  $ptcSrc = Join-Path $RepoRoot 'templates\ptc.yml'
  $ptcDst = Join-Path $DshPatchDir 'ptc.yml'
  Copy-Item -Force $ptcSrc $ptcDst
  Write-Host "[OK] 已安装 PTC 补丁 -> $ptcDst" -ForegroundColor Green
}

if ($Only -in @('all', 'opencode')) {
  Install-PluginDir -Name 'opencode-monitor' -Label 'OpenCode Worker (opencode-monitor)'
}

Write-Host ''
Write-Host '--- 本机前置环境体检 ---' -ForegroundColor Yellow

# 1. Python
$pyCmd = Get-Command python -ErrorAction SilentlyContinue
if ($pyCmd) {
  $pyVer = (& python --version 2>&1) -join ' '
  Write-Host "[OK] Python: $pyVer" -ForegroundColor Green
} else {
  Write-Host '[缺失] 未检测到 python（需要 Python 3.10+）。可运行：' -ForegroundColor Red
  Write-Host '       winget install -e --id Python.Python.3.12' -ForegroundColor Gray
}

# 2. Node.js
$nodeCmd = Get-Command node -ErrorAction SilentlyContinue
if ($nodeCmd) {
  $nodeVer = (& node -v 2>&1) -join ' '
  Write-Host "[OK] Node.js: $nodeVer" -ForegroundColor Green
} else {
  Write-Host '[缺失] 未检测到 node（监控面板和 DSH CLI 都需要 Node.js 20+）。可运行：' -ForegroundColor Red
  Write-Host '       winget install -e --id OpenJS.NodeJS.LTS' -ForegroundColor Gray
}

# 3. DSH CLI & headless patch
if ($Only -in @('all', 'dsh')) {
  $npmDshBin = Join-Path $env:APPDATA 'npm\node_modules\@deepseek-ai\dsh\lib\bin.js'
  $dshCmd = Get-Command dsh -ErrorAction SilentlyContinue
  if (Test-Path $npmDshBin) {
    Write-Host "[OK] DeepSeek Harness CLI (npm 全局版): $npmDshBin" -ForegroundColor Green
  } elseif ($dshCmd) {
    Write-Host "[提示] 检测到 dsh ($($dshCmd.Source))，建议安装纯 Node 全局版避免桌面版 UAC 权限冲突：" -ForegroundColor Yellow
    Write-Host '       npm install -g @deepseek-ai/dsh' -ForegroundColor Gray
  } else {
    Write-Host '[缺失] 未检测到 @deepseek-ai/dsh。请运行：' -ForegroundColor Red
    Write-Host '       npm install -g @deepseek-ai/dsh' -ForegroundColor Gray
  }

  $cordisPatch = Join-Path $DshHeadlessDir 'cordis.patch.yml'
  if (Test-Path $cordisPatch) {
    Write-Host "[OK] DSH headless 配置文件已存在: $cordisPatch" -ForegroundColor Green
  } else {
    Write-Host "[待配置] 尚未创建 $cordisPatch" -ForegroundColor Yellow
    Write-Host "         请参考 templates\cordis.patch.example.yml 创建并设置你的 API Key 环境变量。" -ForegroundColor Gray
  }
}

# 4. OpenCode CLI
if ($Only -in @('all', 'opencode')) {
  $ocDesktop = Join-Path $env:LOCALAPPDATA 'Programs\@opencodedesktop\resources\opencode-cli.exe'
  $ocCmd = Get-Command opencode-cli -ErrorAction SilentlyContinue
  if (-not $ocCmd) { $ocCmd = Get-Command opencode -ErrorAction SilentlyContinue }
  if ($ocCmd) {
    Write-Host "[OK] OpenCode CLI: $($ocCmd.Source)" -ForegroundColor Green
  } elseif (Test-Path $ocDesktop) {
    Write-Host "[OK] OpenCode Desktop CLI: $ocDesktop" -ForegroundColor Green
  } else {
    Write-Host '[提示] 未检测到 OpenCode CLI（如仅使用 DSH 可忽略，并在 Antigravity 中关闭 OpenCode Worker 插件）。' -ForegroundColor Yellow
  }
}

Write-Host ''
Write-Host '安装完成！下一步：' -ForegroundColor Cyan
Write-Host '1. 完全重启 Antigravity（或重新加载插件列表）。'
Write-Host '2. 打开 Antigravity -> Plugins Manager，确认已启用 DeepSeek Worker / OpenCode Worker。'
Write-Host '3. 平时不用的插件可直接关掉开关，零上下文占用。'
Write-Host '4. 详细配置与日常口令请查看 README.md。'
