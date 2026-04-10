from bs4 import BeautifulSoup

from fare_monitor.collector import collect
from fare_monitor.config import AppConfig
from fare_monitor.reporting import generate_report
from fare_monitor.stage_logging import StageLogger


def test_end_to_end_sample_run_creates_outputs(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    config.ensure_dirs()
    logger = StageLogger(enabled=True, log_path=config.log_file_path())
    artifacts = collect(config=config, days=45, sample_mode=True, logger=logger)
    report_path = generate_report(config=config, collection_id=artifacts.collection_id)
    html = report_path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    section_titles = [node.get_text(strip=True) for node in soup.select("h2")]
    section_subtitles = [node.get_text(strip=True) for node in soup.select(".section-title span")]
    lead_notes = [node.get_text(strip=True) for node in soup.select(".lead-note")]

    assert artifacts.total_fares > 0
    assert artifacts.qualified_fares > 0
    assert artifacts.fares_csv.exists()
    assert artifacts.qualified_csv.exists()
    assert report_path.exists()
    assert config.log_file_path().exists()
    assert "北京首都 (PEK)" in page_text
    assert "天津滨海 (TSN)" in page_text
    assert "石家庄正定 (SJW)" in page_text
    assert "仅展示已验证官方报价" in page_text
    assert "默认排除今天" in page_text
    assert "半年按周最低价" in page_text
    assert "前 25 条" in page_text
    assert "全部目的地半年按周最低价走势" in section_titles
    assert "按全部已配置目的地展示；基于已采集结果按周聚合，不会额外触发官网查询" in section_subtitles
    assert "当前为样例数据，不代表实时可售价格" in lead_notes
    assert "Spring Airlines 官网" in page_text
    assert "SPRING JAPAN 官网" in page_text
    assert "Peach 官网" in page_text
    assert "Jetstar Japan 官网" in page_text
    assert "经上海转飞参考" not in page_text
    assert "上海虹桥 (SHA)" not in page_text
    assert "上海浦东 (PVG)" not in page_text

    fare_table = soup.select_one(".fare-table")
    assert fare_table is not None
    body_rows = fare_table.select("tbody tr")
    assert len(body_rows) <= config.report_top_n

    chart_cards = soup.select(".mini-card")
    assert len(chart_cards) == len(config.destinations)
    assert len(soup.select(".empty-chart")) >= 1
    assert len(soup.select(".chart-legend")) >= 1
