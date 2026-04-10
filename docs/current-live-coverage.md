# 当前实现与 Live 覆盖说明

## 当前实现了什么

- 通过官方或可验证页面采集机票数据
- 将结果标准化后写入 `SQLite`
- 导出 `fares.csv` 和 `qualified_fares.csv`
- 生成离线 `HTML` 报表
- 提供 `probe-spring` 命令逐条验证 Spring 官方路线是否可售
- 提供阶段简略日志，输出到控制台和 `output/latest/run.log`
- Spring 主链路已改成单路由持久会话扫描，同一路线只开一次浏览器会话，再按周推进

## 当前来源状态

### 已启用主来源

- `Spring Airlines 官网`
  - 当前主 live 来源
  - 只把精确机场匹配且可验证的票价写入主结果
  - 优先读取顶部 7 天价格带，只有价格低于阈值的日期才会在同一路由会话内点击抓取最低航班

### 已有代码但默认未启用

- `SPRING JAPAN 官网`
  - 默认关闭，避免和 Spring 主站重复
- `Peach 官网`
  - 已收录来源入口，暂未完成默认 live 验证链路
- `Jetstar Japan 官网`
  - 已收录来源入口，暂未完成默认 live 验证链路

## 默认出发地

- `PEK`: 北京首都国际机场
- `TSN`: 天津滨海国际机场
- `SJW`: 石家庄正定国际机场
- `SHA`: 上海虹桥国际机场
- `PVG`: 上海浦东国际机场

## 默认到达地

- `NRT`: 东京成田国际机场
- `HND`: 东京羽田国际机场
- `KIX`: 大阪关西国际机场
- `ITM`: 大阪伊丹机场
- `NGO`: 名古屋中部国际机场
- `FUK`: 福冈机场
- `CTS`: 札幌新千岁机场
- `OKA`: 冲绳那霸机场

## 当前已验证的 Spring Live 路线

- `PEK -> NRT`
- `TSN -> NRT`
- `PVG -> NRT`
- `PVG -> HND`
- `PVG -> KIX`
- `PVG -> NGO`
- `PVG -> FUK`

这些路线来自实际浏览器渲染验证，只在命中精确机场并能落到官方可售页时才进入稳定集合。

## 当前主规则

- 主结果阈值：`1200` 元
- Spring 路由扫描：单路由单会话
- 页面顶部周视图跨度：`7` 天
- 只有顶部价格带低于 `1200` 的日期，才会在同一页里点击抓取最低航班
- 同一天如果有多班低于 `1200`，只保留最低一班
- 当前阶段不抓第二档价格，不在报表里新增舱位价格列

## 当前未纳入稳定集合的原因

- `SHA -> 日本`
  - 页面经常落到 `PVG` 实际票源，不能当成精确 `SHA` 路线
- `SJW -> 日本`
  - 当前验证窗口内没有拿到精确机场可售行
- `PVG -> CTS/OKA`
  - 历史上见过，但最近半年窗口复核未再次验证成功，已从默认稳定集合移除

## 常用命令

### 真实采集并生成报表

```bash
python -m fare_monitor run --config D:\test\fare-monitor\fare-monitor.toml --days 7
```

### 只采集不生成报表

```bash
python -m fare_monitor collect --config D:\test\fare-monitor\fare-monitor.toml --days 7
```

### 只生成报表

```bash
python -m fare_monitor report --config D:\test\fare-monitor\fare-monitor.toml
```

### 探测 Spring 候选路线

```bash
python -m fare_monitor probe-spring --config D:\test\fare-monitor\fare-monitor.toml
```

## 配置文件说明

配置文件默认是 [fare-monitor.toml](/D:/test/fare-monitor/fare-monitor.toml)，主要字段如下：

- `[search].origins`
  - 出发机场列表
- `[search].destinations`
  - 目的地机场列表
- `[search].scan_days`
  - 默认扫描未来天数
- `[search].qualified_threshold_cny`
  - 主结果低价阈值，单位人民币
- `[report].exclude_today`
  - 报表是否排除今天
- `[report].top_n`
  - 首页低价榜展示条数
- `[sources].*_enabled`
  - 各来源默认是否参与 live 链路
- `[performance].spring_live_workers`
  - Spring live 并发 worker 数
- `[performance].spring_window_days`
  - Spring 单页复用窗口天数
- `[performance].spring_date_click_threshold_cny`
  - 同页点击抓取阈值
- `[performance].probe_step_days`
  - `probe-spring` 默认探测步长
- `[logging].enabled`
  - 是否启用阶段日志

## 阶段日志示例

成功运行时会看到类似输出：

```text
[10:00:00] run start collection_id=... mode=live days=14 config=...
[10:00:01] source spring_airlines start routes=7 workers=1 mode=route-session
[10:01:20] source spring_airlines progress ok=5 empty=1 failed=0 partial=0 blocked=no routes=7 weeks_scanned=35 preview_days=245 clicked_dates=12 written_fares=12
[10:01:20] source spring_airlines end status=ok fares=12 routes=7
[10:01:21] report start collection_id=...
[10:01:21] report end path=D:\test\fare-monitor\output\latest\report.html
```

失败或跳过时会看到类似输出：

```text
[10:00:00] source peach skipped reason=Official Peach live verification is not implemented yet, so this source is skipped in stable mode.
[10:02:30] source spring_airlines progress ok=0 empty=0 failed=3 blocked=yes windows=7 preview_days=49 clicked_dates=2 written_fares=0
```

## 注意
实测并发不要开高于5线程，否则会被锁小黑屋