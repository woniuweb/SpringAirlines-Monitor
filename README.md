# Fare Monitor

本项目是一个本地运行的机票监控工具，当前稳定版本主要用于监控中国指定出发地飞往日本的低价机票，并输出 `CSV`、`SQLite` 和静态 `HTML` 报表。

当前默认主链路为 `Spring Airlines 官网` 实时验证，重点是尽量保留“能落到官方可售页”的价格，减少 404、引流价和落地变贵的问题。

## 启动命令

安装：

```bash
pip install -e .
```

运行一次采集并生成报表：

```bash
python -m fare_monitor run --config fare-monitor.toml --days 7
```

常用命令：

```bash
python -m fare_monitor collect --config fare-monitor.toml --days 7
python -m fare_monitor report --config fare-monitor.toml
python -m fare_monitor probe-spring --config fare-monitor.toml
```

## 配置文件

主配置文件为 [fare-monitor.toml](./fare-monitor.toml)。

当前最常改的配置项：

- `search.origins`：出发地机场
- `search.destinations`：目的地机场
- `search.scan_days`：扫描未来多少天
- `search.qualified_threshold_cny`：主筛选价格阈值
- `report.top_n`：首页低价榜展示条数
- `report.title` / `scope_description` / `rules_description`：报表文案

当前默认范围：

- 出发地：`PEK`、`TSN`、`SJW`
- 目的地：`NRT`、`HND`、`KIX`、`ITM`、`NGO`、`FUK`、`CTS`、`OKA`

## 项目使用介绍

运行后默认生成：

- `data/fares.db`
- `output/latest/fares.csv`
- `output/latest/qualified_fares.csv`
- `output/latest/report.html`
- `output/latest/run.log`

报表内容包括：

- 未来低价榜
- 半年按周最低价走势
- 按目的地对比不同出发地价格走势
- 机场代码、航司、来源说明
- 来源执行摘要

补充说明见：

- [docs/current-live-coverage.md](./docs/current-live-coverage.md)
- [docs/linux-deployment.md](./docs/linux-deployment.md)

## 自动构建与 Release

项目已包含 GitHub Actions 工作流：

- 手动触发：在 GitHub `Actions` 页面运行 `Build Windows Release`
- 自动发布：推送形如 `v1.0.0` 的 tag 后，自动构建 Windows `exe` 并上传到 `Releases`

当前自动构建产物为：

- `fare-monitor.exe`
- `fare-monitor-windows.zip`
