# Linux Deployment

本文档说明如何把 `fare-monitor` 部署到 Linux 服务器，并通过 `cron + shell` 每天早上 `09:00` 自动生成报告并发送到邮箱。

## 目标目录

推荐目录结构：

- 应用代码：`/opt/fare-monitor/app`
- 运行数据：`/opt/fare-monitor/runtime`

说明：

- 代码和配置文件放在 `app`
- `data/`、`output/`、`run.log` 都通过 `--base-dir /opt/fare-monitor/runtime` 输出到运行目录

## 1. 安装系统依赖

以 Debian / Ubuntu 为例：

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip cron flock
```

## 2. 部署项目

```bash
sudo mkdir -p /opt/fare-monitor
sudo chown -R "$USER":"$USER" /opt/fare-monitor

cd /opt/fare-monitor
git clone https://github.com/woniuweb/SpringAirlines-Monitor.git app
cd app

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
python -m playwright install chromium
python -m playwright install-deps chromium
```

## 3. 配置文件

编辑：

```bash
/opt/fare-monitor/app/fare-monitor.toml
```

重点配置：

- `[search]`：出发地、目的地、扫描天数、价格阈值
- `[browser]`：Linux 建议 `headless = true`
- `[email]`：
  - `enabled = true`
  - `smtp_host`
  - `smtp_port`
  - `smtp_username`
  - `smtp_password_env`
  - `from_address`
  - `to_addresses`

示例：

```toml
[browser]
headless = true
browser_executable_path = ""
browser_channel = ""

[email]
enabled = true
smtp_host = "smtp.example.com"
smtp_port = 587
smtp_username = "your-account@example.com"
smtp_password_env = "FARE_MONITOR_SMTP_PASSWORD"
from_address = "your-account@example.com"
to_addresses = ["alice@example.com", "bob@example.com"]
subject_prefix = "[Fare Monitor]"
send_on_success = true
send_on_failure = true
attach_report_html = true
attach_qualified_csv = true
attach_run_log_on_failure = true
smtp_use_tls = true
smtp_use_ssl = false
```

## 4. 准备 SMTP 环境变量

复制模板：

```bash
cp /opt/fare-monitor/app/deploy/fare-monitor.env.example /opt/fare-monitor/app/deploy/fare-monitor.env
```

编辑：

```bash
/opt/fare-monitor/app/deploy/fare-monitor.env
```

写入你的 SMTP 应用专用密码：

```bash
FARE_MONITOR_SMTP_PASSWORD=replace-with-your-app-password
```

## 5. 手动执行一次

```bash
cd /opt/fare-monitor/app
chmod +x deploy/run_daily_report.sh
./deploy/run_daily_report.sh
```

主执行命令等价于：

```bash
python -m fare_monitor run-and-email \
  --base-dir /opt/fare-monitor/runtime \
  --config /opt/fare-monitor/app/fare-monitor.toml \
  --days 180
```

运行完成后，输出位于：

- `/opt/fare-monitor/runtime/data/fares.db`
- `/opt/fare-monitor/runtime/output/latest/report.html`
- `/opt/fare-monitor/runtime/output/latest/qualified_fares.csv`
- `/opt/fare-monitor/runtime/output/latest/run.log`

## 6. 配置 cron

编辑当前用户 crontab：

```bash
crontab -e
```

加入：

```cron
CRON_TZ=Asia/Shanghai
0 9 * * * /opt/fare-monitor/app/deploy/run_daily_report.sh >> /opt/fare-monitor/runtime/cron.log 2>&1
```

说明：

- 每天 `09:00` 执行一次
- 标准输出和错误输出都写入 `cron.log`
- 脚本内部用 `flock` 防止重复并发执行

## 7. 常用命令

只生成报告并发邮件：

```bash
python -m fare_monitor run-and-email \
  --base-dir /opt/fare-monitor/runtime \
  --config /opt/fare-monitor/app/fare-monitor.toml \
  --days 180
```

只发送已有 collection 的邮件：

```bash
python -m fare_monitor email-report \
  --base-dir /opt/fare-monitor/runtime \
  --config /opt/fare-monitor/app/fare-monitor.toml
```

邮件 dry-run：

```bash
python -m fare_monitor email-report \
  --base-dir /opt/fare-monitor/runtime \
  --config /opt/fare-monitor/app/fare-monitor.toml \
  --dry-run
```

## 8. 失败排查

如果邮件没有发出，优先检查：

- `fare-monitor.toml` 里的 `[email]` 是否启用
- `smtp_host` / `smtp_port` / `smtp_username` 是否正确
- `smtp_password_env` 对应的环境变量是否已在 `deploy/fare-monitor.env` 中配置
- `smtp_use_tls` / `smtp_use_ssl` 是否与邮箱服务商要求一致

如果 Spring 采集失败，优先检查：

- Linux 服务器是否已安装 Playwright Chromium
- `python -m playwright install chromium`
- `python -m playwright install-deps chromium`
- `run.log` 是否出现 block / route mismatch / browser launch failed
