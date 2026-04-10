from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from jinja2 import Template

from fare_monitor.config import AppConfig
from fare_monitor.constants import (
    AIRLINE_DISPLAY,
    AIRPORT_DISPLAY,
    COLOR_PALETTE,
    SOURCE_DISPLAY,
    SOURCE_HOME_URL,
    SOURCE_NOTES,
)
from fare_monitor.storage import Storage

ORIGIN_CHART_COLORS = {
    "PEK": "#0b3954",
    "TSN": "#d1495b",
    "SJW": "#2a9d8f",
}


def format_airport(code: str) -> str:
    return f"{AIRPORT_DISPLAY.get(code, code)} ({code})"


def format_route(origin: str, destination: str) -> str:
    return f"{format_airport(origin)} -> {format_airport(destination)}"


def format_airport_inline(code: str) -> str:
    return f"{AIRPORT_DISPLAY.get(code, code)}（{code}）"


def join_display_codes(codes: tuple[str, ...]) -> str:
    return "、".join(format_airport_inline(code) for code in codes)


def summarize_destinations(codes: tuple[str, ...]) -> str:
    if len(codes) <= 3:
        return join_display_codes(codes)
    return "已配置目的地"


def filter_verified_future_qualified_rows(
    rows: list[dict[str, object]],
    today: date,
    threshold: float = 1200.0,
    exclude_today: bool = True,
) -> list[dict[str, object]]:
    def qualifies(row: dict[str, object]) -> bool:
        depart_date = str(row["depart_date"])
        if exclude_today:
            if depart_date <= today.isoformat():
                return False
        elif depart_date < today.isoformat():
            return False
        return float(row["price_total_cny"]) < threshold and str(row.get("verification_status", "")) == "verified"

    filtered = [row for row in rows if qualifies(row)]
    return sorted(
        filtered,
        key=lambda row: (
            float(row["price_total_cny"]),
            str(row["depart_date"]),
            str(row["depart_time"]),
            str(row.get("carrier_display_name", "")),
            str(row.get("source_display_name", "")),
        ),
    )


def week_start(date_value: str) -> str:
    parsed = date.fromisoformat(date_value)
    return (parsed - timedelta(days=parsed.weekday())).isoformat()


def build_weekly_minimums(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, dict[str, dict[str, object]]] = defaultdict(dict)
    for row in rows:
        route_key = f"{row['origin']}->{row['destination']}"
        bucket = week_start(str(row["depart_date"]))
        current = grouped[route_key].get(bucket)
        if current is None or float(row["price_total_cny"]) < float(current["price_total_cny"]):
            grouped[route_key][bucket] = row

    return {
        route_key: [values[key] for key in sorted(values)]
        for route_key, values in grouped.items()
    }


def build_destination_weekly_series(
    rows: list[dict[str, object]],
) -> dict[str, dict[str, list[dict[str, object]]]]:
    grouped: dict[str, dict[str, dict[str, dict[str, object]]]] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        destination = str(row["destination"])
        origin = str(row["origin"])
        bucket = week_start(str(row["depart_date"]))
        current = grouped[destination][origin].get(bucket)
        if current is None or float(row["price_total_cny"]) < float(current["price_total_cny"]):
            grouped[destination][origin][bucket] = row

    return {
        destination: {
            origin: [origin_values[key] for key in sorted(origin_values)]
            for origin, origin_values in origin_group.items()
        }
        for destination, origin_group in grouped.items()
    }


def origin_chart_color(origin: str, index: int) -> str:
    return ORIGIN_CHART_COLORS.get(origin, COLOR_PALETTE[index % len(COLOR_PALETTE)])


def select_chart_destinations(
    rows: list[dict[str, object]],
    destination_series: dict[str, dict[str, list[dict[str, object]]]],
    limit: int = 6,
) -> list[str]:
    ranked: list[tuple[float, int, str]] = []
    seen: set[str] = set()
    for row in rows:
        destination = str(row["destination"])
        if destination in seen:
            continue
        seen.add(destination)
        point_count = sum(len(series) for series in destination_series.get(destination, {}).values())
        ranked.append((float(row["price_total_cny"]), -point_count, destination))
    ranked.sort()
    return [destination for _, _, destination in ranked[:limit]]


def build_low_price_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return (
            "<p class='empty'>当前没有可展示的已验证官方低价。"
            "如果 live 模式下只有官方入口可达，但没有能自动核验的实时票价，本页会保持空态。</p>"
        )

    parts = [
        "<table class='fare-table'><thead><tr>"
        "<th>价格</th><th>出发日期</th><th>起飞时间</th><th>路线</th><th>实际航司</th><th>来源</th><th>出处</th>"
        "</tr></thead><tbody>"
    ]
    for row in rows:
        route = format_route(str(row["origin"]), str(row["destination"]))
        source_link = (
            f"<a href='{row['source_url']}' target='_blank' rel='noreferrer'>{row['source_display_name']}</a>"
            if row.get("source_url")
            else str(row["source_display_name"])
        )
        booking_link = (
            f"<a href='{row['booking_url']}' target='_blank' rel='noreferrer'>查看详情</a>"
            if row.get("booking_url")
            else "-"
        )
        parts.append(
            "<tr>"
            f"<td class='price-cell'>¥{float(row['price_total_cny']):.0f}</td>"
            f"<td>{row['depart_date']}</td>"
            f"<td>{row['depart_time']}</td>"
            f"<td>{route}</td>"
            f"<td>{row['carrier_display_name']}</td>"
            f"<td>{source_link}</td>"
            f"<td>{booking_link}</td>"
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "".join(parts)


def build_small_multiple_charts(
    destinations: list[str],
    destination_series: dict[str, dict[str, list[dict[str, object]]]],
    config: AppConfig,
) -> str:
    if not destinations:
        return "<p class='empty'>当前没有足够的已验证低价数据来绘制半年按周最低价走势。</p>"

    series_rows = [
        row
        for destination in destinations
        for origin_rows in destination_series.get(destination, {}).values()
        for row in origin_rows
    ]
    prices = [float(row["price_total_cny"]) for row in series_rows]
    min_price = min(prices) if prices else None
    max_price = max(prices) if prices else None
    spread = max(max_price - min_price, 1.0) if prices else None
    cards = ["<div class='small-multiples'>"]
    for destination in destinations:
        origin_series = {origin: destination_series.get(destination, {}).get(origin, []) for origin in config.origins}
        width = 340
        height = 180
        left = 42
        top = 18
        plot_w = width - left - 20
        plot_h = height - top - 32
        title = format_airport(destination)
        legend_items = []
        for line_index, origin in enumerate(config.origins):
            if not origin_series.get(origin):
                continue
            color = origin_chart_color(origin, line_index)
            legend_items.append(
                "<span class='legend-item'>"
                f"<span class='legend-swatch' style='background:{color}'></span>"
                f"{format_airport(origin)}"
                "</span>"
            )
        week_labels = sorted({week_start(str(row["depart_date"])) for rows in origin_series.values() for row in rows})
        if not week_labels or min_price is None or max_price is None or spread is None:
            cards.append(
                "<article class='mini-card'>"
                f"<h3>{title}</h3>"
                "<p>同图比较不同出发地去同一目的地的周最低价走势；基于已采集结果按周聚合，不会额外触发官网查询。</p>"
                "<div class='empty-chart'>当前采集范围内暂无该目的地的已验证票价数据。</div>"
                "</article>"
            )
            continue
        x_map = {
            label: left + plot_w * index / max(len(week_labels) - 1, 1)
            for index, label in enumerate(week_labels)
        }
        svg = [
            f"<svg viewBox='0 0 {width} {height}' class='mini-chart' role='img' aria-label='{title} 半年按周最低价走势'>",
            f"<rect x='0' y='0' width='{width}' height='{height}' fill='#fbfdff' rx='16'/>",
        ]
        for grid_index in range(3):
            y = top + plot_h * grid_index / 2
            label_value = max_price - spread * grid_index / 2
            svg.append(
                f"<line x1='{left}' y1='{y:.1f}' x2='{left + plot_w}' y2='{y:.1f}' stroke='#d7e3ef' stroke-width='1'/>"
            )
            svg.append(
                f"<text x='{left - 6}' y='{y + 4:.1f}' text-anchor='end' fill='#5a6474' font-size='10'>¥{label_value:.0f}</text>"
            )
        for line_index, origin in enumerate(config.origins):
            origin_rows = origin_series.get(origin, [])
            if not origin_rows:
                continue
            color = origin_chart_color(origin, line_index)
            points = []
            for row in origin_rows:
                bucket = week_start(str(row["depart_date"]))
                x = x_map[bucket]
                y = top + (max_price - float(row["price_total_cny"])) / spread * plot_h
                points.append((x, y, row))
            polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)
            svg.append(f"<polyline fill='none' stroke='{color}' stroke-width='2.5' points='{polyline}'/>")
            for x, y, _ in points:
                svg.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.2' fill='{color}'/>")
        for label, x in x_map.items():
            svg.append(
                f"<text x='{x:.1f}' y='{height - 10}' text-anchor='middle' fill='#5a6474' font-size='9'>{label[5:]}</text>"
            )
        svg.append("</svg>")
        cards.append(
            "<article class='mini-card'>"
            f"<h3>{title}</h3>"
            "<p>按目的地汇总，同图比较不同出发地的周最低价走势；仅基于已采集结果聚合，不会额外触发官网查询。</p>"
            f"<div class='chart-legend'>{''.join(legend_items)}</div>"
            f"{''.join(svg)}"
            "</article>"
        )
    cards.append("</div>")
    return "".join(cards)


def build_source_summary_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "<p class='empty'>暂无来源执行记录。</p>"
    parts = [
        "<table class='source-table'><thead><tr>"
        "<th>来源</th><th>主页</th><th>状态</th><th>查询数</th><th>写入票数</th><th>说明</th>"
        "</tr></thead><tbody>"
    ]
    for row in rows:
        source_key = str(row["source"])
        source_name = SOURCE_DISPLAY.get(source_key, source_key)
        source_url = SOURCE_HOME_URL.get(source_key, "")
        homepage = f"<a href='{source_url}' target='_blank' rel='noreferrer'>{source_url}</a>" if source_url else "-"
        parts.append(
            "<tr>"
            f"<td>{source_name}</td>"
            f"<td>{homepage}</td>"
            f"<td>{row['status']}</td>"
            f"<td>{row['queried_routes']}</td>"
            f"<td>{row['fare_count']}</td>"
            f"<td>{row['message']}</td>"
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "".join(parts)


def build_code_explanations(
    rows: list[dict[str, object]],
    source_rows: list[dict[str, object]],
    config: AppConfig,
) -> dict[str, list[str]]:
    airports = sorted(
        set(config.origins)
        | set(config.destinations)
        | {str(row["origin"]) for row in rows}
        | {str(row["destination"]) for row in rows}
    )
    carriers = sorted({str(row.get("carrier_display_name", "")) for row in rows if str(row.get("carrier_display_name", "")).strip()})
    active_source_keys = {
        str(row.get("source", "")).strip()
        for row in source_rows
        if str(row.get("source", "")).strip() and (
            str(row.get("status", "")).strip() != "skipped" or config.is_source_enabled(str(row.get("source", "")).strip())
        )
    }
    if not active_source_keys:
        active_source_keys = {key for key in SOURCE_DISPLAY if config.is_source_enabled(key)}
    sources = [
        f"{SOURCE_DISPLAY.get(key, key)}: {SOURCE_NOTES.get(key, '')} {SOURCE_HOME_URL.get(key, '')}".strip()
        for key in sorted(active_source_keys)
    ]
    return {
        "airports": [f"{code}: {AIRPORT_DISPLAY.get(code, code)}" for code in airports],
        "carriers": carriers,
        "sources": sources,
    }


def build_reliability_warning(source_rows: list[dict[str, object]], fare_count: int) -> str:
    failed = [row for row in source_rows if str(row.get("status")) == "failed"]
    partial = [row for row in source_rows if str(row.get("status")) == "partial"]
    affected = len(failed) + len(partial)
    if affected == 0:
        return ""
    if fare_count == 0:
        return (
            f"本次结果不能解读为“没有低价票”。共有 {affected} 个来源执行失败或不完整，"
            "当前空结果可能由官网限流、连接中断或反爬拦截导致。"
        )
    return (
        f"本次结果并不完整。共有 {affected} 个来源执行失败或不完整，"
        "当前榜单只代表成功获取并验证的那部分官方报价。"
    )


def build_dynamic_report_title(config: AppConfig) -> str:
    if config.report_title:
        return config.report_title
    return f"{join_display_codes(config.origins)}飞{summarize_destinations(config.destinations)}机票监控"


def build_dynamic_scope_description(config: AppConfig, source_rows: list[dict[str, object]]) -> str:
    if config.report_scope_description:
        return config.report_scope_description
    active_sources = [
        SOURCE_DISPLAY.get(key, key)
        for key in SOURCE_DISPLAY
        if config.is_source_enabled(key)
    ]
    source_text = "、".join(active_sources) if active_sources else "当前启用来源"
    return (
        f"当前默认监控{join_display_codes(config.origins)}飞往{join_display_codes(config.destinations)}的航班，"
        f"采集来源以{source_text}为准。"
    )


def build_dynamic_rules_description(config: AppConfig) -> str:
    if config.report_rules_description:
        return config.report_rules_description
    return (
        "页面规则：只展示已验证官方报价；"
        f"默认{'排除今天' if config.report_exclude_today else '包含今天'}；"
        "半年趋势按周最低价聚合；"
        f"首屏只展示未来最低价前 {config.report_top_n} 条。"
    )


REPORT_TEMPLATE = Template(
    """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ report_title }}</title>
  <style>
    :root {
      --paper: #f5f6fa;
      --card: #ffffff;
      --ink: #162033;
      --muted: #56637a;
      --line: #d7e3ef;
      --accent: #0b3954;
      --accent-2: #0f5c79;
      --sample: #d96c06;
    }
    body {
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", sans-serif;
      background:
        radial-gradient(circle at top left, #d8ecff 0, transparent 28%),
        radial-gradient(circle at top right, #f8d8de 0, transparent 24%),
        var(--paper);
      color: var(--ink);
    }
    .wrap { max-width: 1360px; margin: 0 auto; padding: 32px 20px 60px; }
    .hero { display: grid; gap: 18px; grid-template-columns: 2fr 1fr; margin-bottom: 24px; }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px;
      box-shadow: 0 16px 50px rgba(11, 57, 84, 0.08);
    }
    h1, h2, h3 { margin: 0 0 12px; }
    h1 { font-size: 34px; letter-spacing: 0.02em; }
    h2 { font-size: 20px; }
    h3 { font-size: 15px; color: var(--ink); }
    p { margin: 0 0 12px; color: var(--muted); line-height: 1.6; }
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
    .stat {
      background: linear-gradient(180deg, #fbfdff, #eef5fb);
      border-radius: 18px;
      padding: 16px;
      border: 1px solid var(--line);
    }
    .stat strong { display: block; font-size: 28px; margin-top: 8px; color: var(--accent); }
    .grid { display: grid; gap: 20px; }
    .lead-note {
      display: inline-block;
      padding: 6px 12px;
      border-radius: 999px;
      background: #edf7fb;
      color: var(--accent-2);
      font-size: 13px;
      font-weight: 700;
      margin-right: 8px;
      margin-bottom: 8px;
    }
    .lead-note.sample {
      background: #fff2df;
      color: var(--sample);
    }
    .alert {
      margin-top: 14px;
      padding: 14px 16px;
      border-radius: 16px;
      border: 1px solid #f0c36d;
      background: #fff7e8;
      color: #7a4b00;
      font-weight: 700;
      line-height: 1.5;
    }
    table { width: 100%; border-collapse: collapse; }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }
    thead th {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
    }
    .price-cell { font-weight: 800; color: var(--accent); }
    .section-title {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }
    .empty { color: var(--muted); font-style: italic; }
    .explain-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
    .explain-list { margin: 0; padding-left: 18px; color: var(--muted); }
    .small-multiples { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
    .mini-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: linear-gradient(180deg, #ffffff, #f7fbff);
    }
    .mini-card p { font-size: 12px; margin-bottom: 10px; }
    .chart-legend {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      margin-bottom: 10px;
    }
    .legend-item {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }
    .legend-swatch {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      flex: 0 0 auto;
      box-shadow: inset 0 0 0 1px rgba(0, 0, 0, 0.08);
    }
    .empty-chart {
      min-height: 180px;
      display: flex;
      align-items: center;
      justify-content: center;
      border: 1px dashed var(--line);
      border-radius: 16px;
      background: #fbfdff;
      color: var(--muted);
      font-style: italic;
      text-align: center;
      padding: 16px;
    }
    .mini-chart { width: 100%; height: auto; display: block; }
    a { color: #0b5cad; text-decoration: none; }
    a:hover { text-decoration: underline; }
    @media (max-width: 900px) {
      .hero { grid-template-columns: 1fr; }
      .stats { grid-template-columns: 1fr; }
      .explain-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="panel">
        <h1>{{ report_title }}</h1>
        <p>{{ scope_description }}</p>
        <p>{{ rules_description }}</p>
        <p>最近一次采集：{{ generated_at }} | Collection ID: <code>{{ collection_id }}</code></p>
        <span class="lead-note">仅展示已验证官方报价</span>
        <span class="lead-note">{{ exclude_today_note }}</span>
        <span class="lead-note">半年趋势按周最低价</span>
        {% if is_sample %}
        <span class="lead-note sample">当前为样例数据，不代表实时可售价格</span>
        {% endif %}
        {% if reliability_warning %}
        <div class="alert">{{ reliability_warning }}</div>
        {% endif %}
      </div>
      <div class="panel stats">
        <div class="stat"><span>全部记录</span><strong>{{ total_count }}</strong></div>
        <div class="stat"><span>{{ threshold_label }} 元内已验证票数</span><strong>{{ future_qualified_count }}</strong></div>
        <div class="stat"><span>首页低价榜展示</span><strong>{{ displayed_count }}</strong></div>
      </div>
    </section>
    <div class="grid">
      <section class="panel">
        <div class="section-title"><h2>{{ threshold_label }} 元以内票价</h2><span>{{ top_n_note }}</span></div>
        {{ low_price_table | safe }}
      </section>
      <section class="panel">
        <div class="section-title"><h2>全部目的地半年按周最低价走势</h2><span>按全部已配置目的地展示；基于已采集结果按周聚合，不会额外触发官网查询</span></div>
        {{ small_multiple_charts | safe }}
      </section>
      {% if show_connection_candidates %}
      <section class="panel">
        <div class="section-title"><h2>连接参考</h2><span>按当前配置额外启用的连接候选区块</span></div>
        <p class="empty">当前版本未启用连接参考明细表。</p>
      </section>
      {% endif %}
      <section class="panel">
        <div class="section-title"><h2>代码与缩写说明</h2><span>帮助快速看懂机场代码、航司和来源</span></div>
        <div class="explain-grid">
          <div>
            <h3>机场代码</h3>
            <ul class="explain-list">
              {% for item in airport_explanations %}
              <li>{{ item }}</li>
              {% endfor %}
            </ul>
          </div>
          <div>
            <h3>航司显示</h3>
            <ul class="explain-list">
              {% for item in carrier_explanations %}
              <li>{{ item }}</li>
              {% endfor %}
            </ul>
          </div>
          <div>
            <h3>来源说明</h3>
            <ul class="explain-list">
              {% for item in source_explanations %}
              <li>{{ item }}</li>
              {% endfor %}
            </ul>
          </div>
        </div>
      </section>
      <section class="panel">
        <div class="section-title"><h2>来源执行摘要</h2><span>{{ source_count }} 个来源</span></div>
        {{ source_summary | safe }}
      </section>
    </div>
  </div>
</body>
</html>
"""
)


def generate_report(config: AppConfig, collection_id: str | None = None) -> Path:
    storage = Storage(config.database_path)
    collection_id = collection_id or storage.latest_collection_id()
    if collection_id is None:
        raise RuntimeError("No collection found. Run collect first.")

    all_rows = storage.fares_for_collection(collection_id)
    source_rows = storage.source_runs_for_collection(collection_id)
    today = date.today()
    future_qualified_rows = filter_verified_future_qualified_rows(
        all_rows,
        today=today,
        threshold=config.qualified_threshold,
        exclude_today=config.report_exclude_today,
    )
    top_rows = future_qualified_rows[: config.report_top_n]
    destination_series = build_destination_weekly_series(future_qualified_rows)
    chart_destinations = list(config.destinations)
    explanations = build_code_explanations(top_rows or future_qualified_rows or all_rows, source_rows, config)
    is_sample = any(bool(row.get("is_sample")) for row in all_rows)
    threshold_label = f"{config.qualified_threshold:.0f}"
    reliability_warning = build_reliability_warning(source_rows, len(future_qualified_rows))
    report_title = build_dynamic_report_title(config)
    scope_description = build_dynamic_scope_description(config, source_rows)
    rules_description = build_dynamic_rules_description(config)

    html = REPORT_TEMPLATE.render(
        report_title=report_title,
        scope_description=scope_description,
        rules_description=rules_description,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        collection_id=collection_id,
        total_count=len(all_rows),
        future_qualified_count=len(future_qualified_rows),
        displayed_count=len(top_rows),
        source_count=len(source_rows),
        is_sample=is_sample,
        reliability_warning=reliability_warning,
        threshold_label=threshold_label,
        top_n=config.report_top_n,
        exclude_today_label="排除今天" if config.report_exclude_today else "包含今天",
        exclude_today_note="默认排除今天" if config.report_exclude_today else "默认包含今天",
        top_n_note=(
            f"已排除今天，仅显示未来最低价前 {config.report_top_n} 条"
            if config.report_exclude_today
            else f"包含今天，显示全局最低价前 {config.report_top_n} 条"
        ),
        low_price_table=build_low_price_table(top_rows),
        small_multiple_charts=build_small_multiple_charts(chart_destinations, destination_series, config),
        show_connection_candidates=config.report_show_connection_candidates,
        airport_explanations=explanations["airports"],
        carrier_explanations=explanations["carriers"],
        source_explanations=explanations["sources"],
        source_summary=build_source_summary_table(source_rows),
    )

    report_path = config.output_dir / "latest" / "report.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(html, encoding="utf-8")
    return report_path
