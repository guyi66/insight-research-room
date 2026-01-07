# finance_tools/fundamental_chart_renderer.py
import os
from typing import Dict, Any, List
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .fundamental_visual_ir import (
    FundamentalVisualPlan,
    ChartSpec,
    TableSpec,
    _get,
)


def _extract_timeseries(
    fundamentals: Dict[str, Any],
    module: str,
    x_field: str,
    y_field: str,
) -> pd.DataFrame:
    """
    从 fundamentals[module] 里抽出 (x, y) 序列。
    假设 fundamentals[module] 是一个 list，每个元素有 statDate 等字段。
    """
    records = fundamentals.get(module, [])
    rows = []
    for item in records:
        x = item.get(x_field)
        y = item.get(y_field.split(".")[-1])  # 简单写法：module 已经分过
        if x is not None and y is not None:
            rows.append({"x": x, "y": float(y)})
    if not rows:
        return pd.DataFrame(columns=["x", "y"])
    df = pd.DataFrame(rows).dropna()
    df = df.sort_values("x")
    return df


def render_charts(
    symbol: str,
    plan: FundamentalVisualPlan,
    fundamentals: Dict[str, Any],
    out_dir: str,
) -> List[Dict[str, str]]:
    """
    根据 plan.charts 画图，返回 [{id, title, path, description}, ...]
    """
    os.makedirs(out_dir, exist_ok=True)
    results: List[Dict[str, str]] = []

    for chart in plan.charts:
        if not chart.series:
            continue

        # 解析 module 名：假设 field 形如 "profit.roeAvg"
        module = chart.series[0].field.split(".")[0]

        fig, ax = plt.subplots()
        for s in chart.series:
            module = s.field.split(".")[0]
            field_name = s.field.split(".")[-1]
            df = _extract_timeseries(
                fundamentals=fundamentals,
                module=module,
                x_field=chart.x_field,
                y_field=s.field,
            )
            if df.empty:
                continue
            # 只取最近 max_points 期
            df = df.tail(chart.max_points)
            if chart.chart_type == "bar":
                ax.bar(df["x"], df["y"], label=s.name)
            else:
                ax.plot(df["x"], df["y"], marker="o", label=s.name)

        ax.set_title(chart.title)
        ax.set_xlabel(chart.x_field)
        ax.legend()
        fig.autofmt_xdate(rotation=45)

        filename = f"{symbol}_{chart.id}.png"
        path = os.path.join(out_dir, filename)
        fig.tight_layout()
        fig.savefig(path, dpi=150)
        plt.close(fig)

        results.append(
            {
                "id": chart.id,
                "title": chart.title,
                "description": chart.description,
                "path": path,
            }
        )

    return results


def render_tables_latex(
    plan: FundamentalVisualPlan,
    fundamentals: Dict[str, Any],
) -> List[str]:
    """
    根据 plan.tables 生成一组 LaTeX tabular 代码字符串。
    """
    tables_tex: List[str] = []

    for tb in plan.tables:
        # 暂时只从某一个模块里取数据，约定第一个字段的 module
        if not tb.columns:
            continue
        first_field = tb.columns[0].field
        module = first_field.split(".")[0]
        records = fundamentals.get(module, [])
        if not records:
            continue

        rows = []
        for item in records:
            row = []
            for col in tb.columns:
                key = col.field.split(".")[-1]
                row.append(str(item.get(key, "")))
            # 附一个 statDate 排序 key
            row_date = item.get(tb.sort_by, "")
            rows.append((row_date, row))

        # 按日期排序，取最近 N 期
        rows = sorted(rows, key=lambda x: x[0])[-tb.limit :]
        data_rows = [r[1] for r in rows]

        # 拼 LaTeX
        col_names = [c.name for c in tb.columns]
        col_fmt = " | ".join(["c"] * len(col_names))
        header = " & ".join(col_names) + r" \\ \hline"
        body_lines = [" & ".join(r) + r" \\" for r in data_rows]

        tex = rf"""
\subsection*{{{tb.title}}}
{tb.description}

\begin{tabular}{{{col_fmt}}}
\hline
{header}
{chr(10).join(body_lines)}
\hline
\end{tabular}
"""
        tables_tex.append(tex)

    return tables_tex
