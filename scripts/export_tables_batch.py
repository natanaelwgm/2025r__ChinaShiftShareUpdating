#!/usr/bin/env python3
"""Generate multi-spec HTML tables from regression results and companion etables."""
from __future__ import annotations

import argparse
import math
import subprocess
from pathlib import Path

import pandas as pd

PERIOD_ORDER = ["2000_2007", "2008_2012", "2000_2015", "2005_2010", "2005_2012"]
TRANSFORM_ORDER = [
    "level_t",
    "diff_t",
    "difflag_t",
    "log_t",
    "logdiff_t",
    "logdifflag_t",
    "level_xtlag",
    "diff_xtlag",
    "difflag_xtlag",
    "log_xtlag",
    "logdiff_xtlag",
    "logdifflag_xtlag",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export grouped regression tables to HTML")
    parser.add_argument("--mode", default="dummy", choices=["dummy", "real"],
                        help="Execution mode to read results from")
    parser.add_argument("--outcome", default="p0",
                        help="Outcome to export (e.g., p0, p1, p2)")
    parser.add_argument("--num", type=int, default=5,
                        help="Number of flow-partner groups to export (default: 5)")
    return parser.parse_args()


def read_results(mode: str, outcome: str) -> pd.DataFrame:
    results_path = Path("data_work") / "output" / mode / "results" / f"main_{outcome}.csv"
    if not results_path.exists():
        raise FileNotFoundError(f"Results file not found: {results_path}")
    df = pd.read_csv(results_path)
    if "transform_id" in df.columns:
        df["transform_id"] = pd.Categorical(df["transform_id"], categories=TRANSFORM_ORDER, ordered=True)
    df["period_order"] = df["period"].apply(lambda p: PERIOD_ORDER.index(p)
                                              if p in PERIOD_ORDER else len(PERIOD_ORDER))
    sort_cols = ["flow", "partner"]
    if "transform_id" in df.columns:
        sort_cols.append("transform_id")
    sort_cols.extend(["base_year", "period_order", "iv_label"])
    df.sort_values(sort_cols, inplace=True)
    return df


def add_stars(beta: float, se: float) -> tuple[str, str]:
    if pd.isna(beta) or pd.isna(se):
        return "NA", "(NA)"
    if se == 0:
        stars = ""
    else:
        t_stat = beta / se
        p_val = 2 * (1 - 0.5 * (1 + math.erf(abs(t_stat) / math.sqrt(2))))
        if p_val < 0.001:
            stars = "***"
        elif p_val < 0.01:
            stars = "**"
        elif p_val < 0.05:
            stars = "*"
        else:
            stars = ""
    coef = f"{beta:.3f}{stars}"
    se_str = f"({se:.3f})"
    return coef, se_str


def build_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.sort_values(["base_year", "period_order"], inplace=True)
    columns = [f"Base {row.base_year}, {row.period.replace('_', '–')}" for _, row in df.iterrows()]

    rows = []
    coef_cells, se_cells = [], []
    for _, row in df.iterrows():
        coef, se = add_stars(row.coef, row.se)
        coef_cells.append(coef)
        se_cells.append(se)
    rows.append(["Exposure coefficient", *coef_cells])
    rows.append(["Standard error", *se_cells])
    rows.append(["Observations", *df["N"].astype(str).tolist()])
    rows.append(["Clusters", *df["clusters"].astype(str).tolist()])
    rows.append(["Base year", *df["base_year"].astype(str).tolist()])
    rows.append(["Period", *df["period"].str.replace("_", "–").tolist()])
    rows.append(["Instrument", *df["iv_label"].tolist()])
    rows.append(["Fixed effects", *df["fe_flags"].tolist()])

    table = pd.DataFrame(rows, columns=["Variables", *columns])
    return table


def render_html(mode: str, outcome: str,
                grouped: list[tuple[str, str, list[tuple[str, str, str, str, str, pd.DataFrame]]]],
                output_path: Path) -> None:
    css = """
    <style>
      body { font-family: 'Helvetica Neue', Arial, sans-serif; margin: 2rem; }
      h2 { margin-top: 3rem; }
      table { border-collapse: collapse; margin-bottom: 1rem; }
      th, td { border: 1px solid #999; padding: 6px 10px; text-align: center; }
      th:first-child, td:first-child { text-align: left; font-weight: 600; }
      .spec-block { page-break-after: always; }
      .spec-block:last-child { page-break-after: auto; }
      .meta { font-style: italic; margin: 0.5rem 0; color: #555; }
    </style>
    """
    parts = ["<html><head>", css, "</head><body>"]
    parts.append(f"<h1>Specification Tables &mdash; Outcome {outcome.upper()} (mode: {mode})</h1>")
    parts.append("<p class='meta'>Each block corresponds to one flow/partner combination. Exposure coefficients display conventional significance stars based on cluster-robust SEs.</p>")

    for idx, (flow, partner, transform_tables) in enumerate(grouped, start=1):
        spec_id = f"Spec {idx:02d}"
        parts.append("<div class='spec-block'>")
        parts.append(f"<h2>{spec_id}: flow = {flow}, partner = {partner}</h2>")
        parts.append(f"<p class='meta'>Start of {spec_id}</p>")
        for transform_id, label, x_timing, exposure_var, instrument_var, table_df in transform_tables:
            parts.append(f"<h3>{label}</h3>")
            parts.append(
                f"<p class='meta'>Transform ID: {transform_id} | X timing: {x_timing} | "
                f"Exposure var: {exposure_var} | Instrument var: {instrument_var}</p>"
            )
            parts.append(table_df.to_html(index=False, escape=False, border=0))
        parts.append(f"<p class='meta'>End of {spec_id}</p>")
        parts.append("</div>")

    summary_rows = []
    for idx, (flow, partner, transform_tables) in enumerate(grouped, start=1):
        columns = []
        transform_labels = []
        for transform_id, label, _x_timing, _exp_var, _iv_var, table_df in transform_tables:
            transform_labels.append(f"{transform_id} ({label})")
            columns.append(f"{label}: " + ", ".join(table_df.columns[1:]))
        summary_rows.append({
            "Spec": f"Spec {idx:02d}",
            "Flow": flow,
            "Partner": partner,
            "Transforms": "<br>".join(transform_labels),
            "Columns": "<br>".join(columns)
        })
    summary_df = pd.DataFrame(summary_rows)
    parts.append("<h2>Summary of included specifications</h2>")
    parts.append(summary_df.to_html(index=False, escape=False, border=0))

    parts.append("</body></html>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def call_etable(mode: str, outcome: str, flow: str, partner: str, max_cols: int) -> None:
    r_script = Path(__file__).resolve().parent / "R" / "run_etable.R"
    if not r_script.exists():
        return
    cmd = [
        "Rscript",
        str(r_script),
        "--mode", mode,
        "--outcome", outcome,
        "--flow", flow,
        "--partner", partner,
        "--num", str(max_cols)
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print(f"[warn] etable generation failed for {flow}/{partner}")


def main():
    args = parse_args()
    df = read_results(args.mode, args.outcome)

    groups: list[tuple[str, str, list[tuple[str, str, str, str, str, pd.DataFrame]]]] = []
    seen = set()
    grouped = df.groupby(["flow", "partner"], sort=False)
    for (flow, partner), group_df in grouped:
        key = (flow, partner)
        if key in seen:
            continue

        transform_tables: list[tuple[str, str, str, str, str, pd.DataFrame]] = []
        for transform_id, transform_df in group_df.groupby("transform_id", sort=False, observed=False):
            if transform_df.empty:
                continue
            label = transform_df.get("transform_label", pd.Series(["(unknown)"])).iloc[0]
            x_timing = transform_df.get("x_timing", pd.Series(["t"])).iloc[0]
            exposure_var = transform_df.get("exposure_var", pd.Series(["exposure_log_diff"])).iloc[0]
            instrument_var = transform_df.get("instrument_var", pd.Series(["instrument_log_diff_lag1"])).iloc[0]
            table_df = build_table(transform_df)
            transform_tables.append((transform_id, label, x_timing, exposure_var, instrument_var, table_df))

        if not transform_tables:
            continue

        transform_tables.sort(
            key=lambda item: TRANSFORM_ORDER.index(item[0]) if item[0] in TRANSFORM_ORDER else len(TRANSFORM_ORDER)
        )
        groups.append((flow, partner, transform_tables))
        seen.add(key)
        if len(groups) >= args.num:
            break

    if not groups:
        raise SystemExit("No groups selected. Adjust --num or check results data.")

    tables_dir = Path("data_work") / "output" / args.mode / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    output_path = tables_dir / f"{args.outcome}_batch.html"
    render_html(args.mode, args.outcome, groups, output_path)

    for flow, partner, transform_tables in groups:
        max_cols = 1
        for _, _, _, _, _, table_df in transform_tables:
            max_cols = max(max_cols, len(table_df.columns) - 1)
        call_etable(args.mode, args.outcome, flow, partner, max_cols)

    print(f"Wrote HTML tables to {output_path.resolve()}")


if __name__ == "__main__":
    main()
