#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
from pathlib import Path

def latex_escape(value: str) -> str:
    replacements = {
        "\\": r"\\textbackslash{}",
        "_": r"\\textunderscore{}",
        "%": r"\\%",
        "#": r"\\#",
        "&": r"\\&",
        "{": r"\\{",
        "}": r"\\}",
    }
    result = str(value)
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result


def sanitize_label(value: str) -> str:
    return ''.join(ch if ch.isalnum() else '_' for ch in value)

def read_cluster_label() -> str:
    cluster = "district_bps_2000"
    params_path = Path("config") / "params.yaml"
    if params_path.exists():
        for line in params_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("cluster:"):
                value = stripped.split(":", 1)[1].strip()
                if value:
                    cluster = value
                break
    return cluster


def build_table_rows(spec, cluster_label: str):
    beta = float(spec["coef"])
    se = float(spec["se"])
    if se == 0:
        stars = ""
    else:
        t_stat = abs(beta / se)
        from math import erf, sqrt
        p = 2 * (1 - 0.5 * (1 + erf(t_stat / sqrt(2))))
        if p < 0.001:
            stars = "***"
        elif p < 0.01:
            stars = "**"
        elif p < 0.05:
            stars = "*"
        else:
            stars = ""
    coef_str = f"{beta:.3f}{stars}"
    se_str = f"({se:.3f})"
    rows = [
        ("Exposure coefficient", coef_str),
        ("Standard error", se_str),
        ("Observations", spec["N"]),
        ("Clusters", spec["clusters"]),
        ("Flow", spec["flow"]),
        ("Partner", spec["partner"]),
        ("Base year", spec["base_year"]),
        ("Period", spec["period"].replace("_", "--")),
        ("Instrument", spec["iv_label"]),
        ("Outcome", spec["outcome"]),
        ("Fixed effects", spec["fe_flags"]),
        ("Cluster level", cluster_label),
    ]
    return rows

def main():
    parser = argparse.ArgumentParser(description="Build PDF spec booklet from regression results.")
    parser.add_argument('--mode', default='dummy', choices=['dummy', 'real'])
    parser.add_argument('--outcome', default='p0')
    parser.add_argument('--num', type=int, default=10)
    args = parser.parse_args()

    results_dir = Path('data_work') / 'output' / args.mode / 'results'
    csv_path = results_dir / f'main_{args.outcome}.csv'
    if not csv_path.exists():
        raise SystemExit(f"Results file not found: {csv_path}")

    specs = []
    with csv_path.open(newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            specs.append(row)

    specs.sort(key=lambda r: (r['flow'], r['partner'], r['base_year'], r['period'], r['iv_label']))
    specs = specs[:args.num]
    if not specs:
        raise SystemExit("No specifications available for requested settings")

    tables_dir = Path('data_work') / 'output' / args.mode / 'tables'
    tables_dir.mkdir(parents=True, exist_ok=True)
    tex_path = tables_dir / f'{args.outcome}_specbook.tex'

    lines = []
    lines.append(r"\documentclass[12pt]{article}")
    lines.append(r"\usepackage[margin=1in]{geometry}")
    lines.append(r"\usepackage{booktabs}")
    lines.append(r"\usepackage{pdflscape}")
    lines.append(r"\usepackage{longtable}")
    lines.append(r"\usepackage{caption}")
    lines.append(r"\usepackage{array}")
    lines.append(r"\begin{document}")
    lines.append(f"\\section*{{Specification Booklet: Outcome {args.outcome.upper()} (mode: {args.mode})}}")
    lines.append(r"\tableofcontents")
    lines.append(r"\newpage")

    summary = []

    for idx, spec in enumerate(specs, start=1):
        label = f"Spec {idx:02d}"
        title = (f"{label}: {spec['flow']} {spec['outcome']} exposure, partner {spec['partner']}, "
                 f"base {spec['base_year']}, period {spec['period']}, IV {spec['iv_label']}")
        lines.append(f"\\section*{{{latex_escape(title)}}}")
        lines.append(f"\\label{{sec:{sanitize_label(label)}}}")
        lines.append(f"This page marks the start of {label}.")
        lines.append(r"\newpage")

        lines.append(r"\begin{table}[htbp]")
        lines.append(r"\centering")
        lines.append(r"\begin{tabular}{p{5cm}p{9cm}}")
        lines.append(r"\toprule")
        lines.append(r"Variable & Value \\\")
        lines.append(r"\midrule")
        for var, val in build_table_rows(spec, cluster_label):
            lines.append(f"{latex_escape(var)} & {latex_escape(val)} \\\\")
        lines.append(r"\bottomrule")
        lines.append(r"\end{tabular}")
        lines.append(f"\\caption{{{latex_escape(label + ' detail table')}}}")
        lines.append(f"\\label{{tab:{sanitize_label(label)}}}")
        lines.append(r"\end{table}")
        lines.append(r"\newpage")
        lines.append(f"End of {label}.")
        lines.append(r"\newpage")

        summary.append({
            'Spec': label,
            'Outcome': spec['outcome'],
            'Flow': spec['flow'],
            'Partner': spec['partner'],
            'Base': spec['base_year'],
            'Period': spec['period'],
            'Instrument': spec['iv_label']
        })

    lines.append(r"\section*{Specification Summary}")
    lines.append(r"\begin{longtable}{p{2cm}p{2cm}p{2cm}p{2cm}p{2cm}p{2.5cm}p{3cm}}")
    lines.append(r"\toprule")
    lines.append(r"Spec & Outcome & Flow & Partner & Base & Period & Instrument \\\")
    lines.append(r"\midrule")
    lines.append(r"\endfirsthead")
    lines.append(r"\toprule")
    lines.append(r"Spec & Outcome & Flow & Partner & Base & Period & Instrument \\\")
    lines.append(r"\midrule")
    lines.append(r"\endhead")
    for entry in summary:
        lines.append(" & ".join(latex_escape(entry[key]) for key in ['Spec','Outcome','Flow','Partner','Base','Period','Instrument']) + r" \\\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{longtable}")
    lines.append(r"\end{document}")

    tex_path.write_text("\n".join(lines), encoding='utf-8')

    old_cwd = os.getcwd()
    os.chdir(tables_dir)
    try:
        subprocess.run(['pdflatex', '-interaction=nonstopmode', tex_path.name], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(['pdflatex', '-interaction=nonstopmode', tex_path.name], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    finally:
        os.chdir(old_cwd)

    pdf_path = tables_dir / f'{args.outcome}_specbook.pdf'
    if pdf_path.exists():
        print(f'Specification booklet written to {pdf_path.resolve()}')
    else:
        raise SystemExit('PDF generation failed; check LaTeX logs in tables directory.')

if __name__ == '__main__':
    main()
    cluster_label = read_cluster_label()
