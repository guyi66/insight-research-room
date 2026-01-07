# finance_tools/fundamental_visual_ir.py
from dataclasses import dataclass, field
from typing import List, Literal, Dict, Any, Optional


ChartType = Literal["line", "bar"]

@dataclass
class ChartSeriesSpec:
    """一条曲线/柱子的定义"""
    name: str        # 图例名，如 "ROE"
    field: str       # 对应 JSON 里的字段路径，比如 "profit.roeAvg"
    unit: str = ""   # 单位，可选

@dataclass
class ChartSpec:
    """单个图表的 IR 描述"""
    id: str
    title: str
    description: str
    chart_type: ChartType
    x_field: str              # 横轴字段，比如 "statDate"
    series: List[ChartSeriesSpec]
    max_points: int = 12      # 最多展示多少期，避免太挤

@dataclass
class TableColumnSpec:
    name: str    # 列名
    field: str   # JSON 字段路径

@dataclass
class TableSpec:
    """单个表格的 IR 描述"""
    id: str
    title: str
    description: str
    columns: List[TableColumnSpec]
    sort_by: str = "statDate"
    limit: int = 8

@dataclass
class FundamentalVisualPlan:
    """LLM 返回的总规划"""
    charts: List[ChartSpec] = field(default_factory=list)
    tables: List[TableSpec] = field(default_factory=list)


def _get(d: Dict[str, Any], path: str, default=None):
    """简单的 'a.b.c' 路径取值工具"""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def parse_visual_plan(raw: Dict[str, Any]) -> FundamentalVisualPlan:
    """把 LLM 返回的 dict 转成 dataclass，容错一点"""
    charts: List[ChartSpec] = []
    for ch in raw.get("charts", []):
        series_list = [
            ChartSeriesSpec(
                name=s.get("name", s.get("field", "")),
                field=s["field"],
                unit=s.get("unit", "")
            )
            for s in ch.get("series", []) if "field" in s
        ]
        charts.append(ChartSpec(
            id=ch.get("id", ch.get("title", "chart")),
            title=ch.get("title", "未命名图表"),
            description=ch.get("description", ""),
            chart_type=ch.get("chart_type", "line"),
            x_field=ch.get("x_field", "statDate"),
            series=series_list,
            max_points=ch.get("max_points", 12),
        ))

    tables: List[TableSpec] = []
    for tb in raw.get("tables", []):
        cols = [
            TableColumnSpec(
                name=c.get("name", c.get("field", "")),
                field=c["field"],
            )
            for c in tb.get("columns", []) if "field" in c
        ]
        tables.append(TableSpec(
            id=tb.get("id", tb.get("title", "table")),
            title=tb.get("title", "未命名表格"),
            description=tb.get("description", ""),
            columns=cols,
            sort_by=tb.get("sort_by", "statDate"),
            limit=tb.get("limit", 8),
        ))

    return FundamentalVisualPlan(charts=charts, tables=tables)


