#!/usr/bin/env python3
"""Benchmark visualization for Pangeo showcase.

Reads CSV results and produces charts comparing COG vs Zarr v2 vs Zarr v3.
"""
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

RESULTS_DIR = Path(__file__).parent / "results"
OUT_DIR = Path(__file__).parent / "charts"

# Use only specific CSVs to avoid mixing GDAL versions.
# bench_after_3130dev_cloud.csv has local+cloud data from GDAL 3.13.0dev.
# bench_cloud_v3.csv has cloud v3 sharded data (if available).
DEFAULT_CSVS = [
    "bench_local_fair_2026-03-03.csv",
]

PR_IMPACT_CSV = "bench_pr_impact_2026-03-03.csv"

# Colors
C_COG = "#2196F3"       # blue
C_COG_ZSTD = "#1565C0"  # dark blue
C_COG_NOOVR = "#90CAF9" # light blue
C_V2 = "#FF9800"        # orange
C_V3_ZSTD = "#9C27B0"   # purple
C_V3_BLOSC = "#CE93D8"  # light purple
C_V3_OVR = "#4CAF50"    # green - the hero
C_V3_CLOUD = "#66BB6A"  # lighter green

FORMAT_COLORS = {
    "cog": C_COG,
    "cog_zstd": C_COG_ZSTD,
    "cog_noovr": C_COG_NOOVR,
    "zarr_v2": C_V2,
    "zarr_v3_zstd": C_V3_ZSTD,
    "zarr_v3_blosc": C_V3_BLOSC,
    "zarr_v3_zstd_ovr": C_V3_OVR,
    "zarr_v3": C_V3_CLOUD,
}

FORMAT_LABELS = {
    "cog": "COG (DEFLATE)",
    "cog_zstd": "COG (ZSTD)",
    "cog_noovr": "COG (no ovr)",
    "zarr_v2": "Zarr v2",
    "zarr_v3_zstd": "Zarr v3 zstd",
    "zarr_v3_blosc": "Zarr v3 blosc",
    "zarr_v3_zstd_ovr": "Zarr v3 + ovr",
    "zarr_v3": "Zarr v3 sharded",
}

SCENARIO_LABELS = {
    "single_band": "Single band",
    "rgb_stacked": "RGB (stacked)",
    "rgb_vrt": "RGB (VRT)",
    "zoom_25": "RGB 4x zoom",
    "zoom_5": "RGB 20x zoom",
    "full_extent": "Full extent",
}


def load_csv(path):
    """Load benchmark CSV, return list of dicts."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["open_ms"] = float(row["open_ms"])
            row["read_ms"] = float(row["read_ms"])
            rows.append(row)
    return rows


def aggregate(rows):
    """Group by (format, source, scenario, gdal_config) and compute mean/std."""
    groups = defaultdict(list)
    for r in rows:
        key = (r["format"], r["source"], r["scenario"], r["gdal_config"])
        groups[key].append(r["read_ms"])

    stats = {}
    for key, values in groups.items():
        stats[key] = {
            "mean": np.mean(values),
            "std": np.std(values),
            "n": len(values),
        }
    return stats


def load_results(csv_names=None):
    """Load specific result CSVs (or all if none specified)."""
    all_rows = []
    if csv_names:
        for name in csv_names:
            path = RESULTS_DIR / name
            if path.exists():
                rows = load_csv(path)
                all_rows.extend(rows)
                print(f"  Loaded {name}: {len(rows)} rows")
            else:
                print(f"  Skipped {name} (not found)")
    else:
        for csv_path in sorted(RESULTS_DIR.glob("*.csv")):
            rows = load_csv(csv_path)
            all_rows.extend(rows)
            print(f"  Loaded {csv_path.name}: {len(rows)} rows")
    return all_rows


def chart_local_hero(stats, out_path):
    """Chart 1: Local read times - all formats, key scenarios."""
    fig, axes = plt.subplots(1, 4, figsize=(16, 5), sharey=False)
    fig.suptitle("Local read performance: COG vs Zarr (GDAL 3.13.0dev)",
                 fontsize=14, fontweight="bold", y=0.98)

    scenarios = ["single_band", "rgb_vrt", "zoom_25", "zoom_5"]
    # Order: COG first, then v2, then v3 variants, hero last
    fmt_order = ["cog", "cog_zstd", "cog_noovr", "zarr_v2", "zarr_v3_zstd",
                 "zarr_v3_blosc", "zarr_v3_zstd_ovr"]

    for ax, scenario in zip(axes, scenarios):
        means = []
        stds = []
        colors = []
        labels = []

        for fmt in fmt_order:
            # Use default config, local source
            key = (fmt, "local", scenario, "default")
            # rgb_stacked only applies to COG formats
            if scenario == "rgb_vrt" and fmt in ("cog", "cog_zstd", "cog_noovr"):
                key = (fmt, "local", "rgb_stacked", "default")

            if key in stats:
                means.append(stats[key]["mean"])
                stds.append(stats[key]["std"])
                colors.append(FORMAT_COLORS.get(fmt, "#999"))
                labels.append(FORMAT_LABELS.get(fmt, fmt))

        if not means:
            ax.set_visible(False)
            continue

        y_pos = np.arange(len(labels))
        bars = ax.barh(y_pos, means, xerr=stds, color=colors,
                       edgecolor="white", linewidth=0.5, capsize=3, height=0.6)

        # Add value labels
        for bar, mean in zip(bars, means):
            ax.text(bar.get_width() + max(means) * 0.02, bar.get_y() + bar.get_height() / 2,
                    f"{mean:.0f}ms", va="center", fontsize=9)

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlabel("Read time (ms)", fontsize=10)
        ax.set_title(SCENARIO_LABELS.get(scenario, scenario), fontsize=11)
        ax.invert_yaxis()
        ax.set_xlim(0, max(means) * 1.25)
        ax.xaxis.set_major_locator(ticker.MaxNLocator(5))

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def chart_journey(stats, out_path):
    """Chart 3: The journey - single band read time progression."""
    fig, ax = plt.subplots(figsize=(10, 5))

    # Order tells the story
    fmt_order = ["zarr_v2", "zarr_v3_zstd", "zarr_v3_blosc",
                 "zarr_v3_zstd_ovr", "cog_zstd", "cog"]
    labels = []
    means = []
    colors = []

    for fmt in fmt_order:
        key = (fmt, "local", "single_band", "default")
        if key in stats:
            labels.append(FORMAT_LABELS.get(fmt, fmt))
            means.append(stats[key]["mean"])
            colors.append(FORMAT_COLORS.get(fmt, "#999"))

    if not means:
        print("  No data for journey chart")
        return

    x_pos = np.arange(len(labels))
    bars = ax.bar(x_pos, means, color=colors, edgecolor="white",
                  linewidth=1, width=0.6)

    # Add value labels on top of bars
    for bar, mean in zip(bars, means):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                f"{mean:.0f}ms", ha="center", va="bottom", fontsize=12,
                fontweight="bold")

    # Add comparison annotations
    if len(means) >= 2:
        cog_mean = means[-1]  # COG is last
        v2_mean = means[0]
        ovr_mean = means[-2] if len(means) >= 5 else None

        # v2 vs COG
        ratio_v2 = v2_mean / cog_mean
        ax.annotate(f"{ratio_v2:.0f}x slower than COG",
                    xy=(0, v2_mean), xytext=(0.5, v2_mean * 0.85),
                    fontsize=10, color="#666",
                    arrowprops=dict(arrowstyle="->", color="#666"))

        # ovr vs COG - no "faster" claim; local single-band doesn't generalize
        if ovr_mean is not None:
            if ovr_mean < cog_mean:
                label = "within COG range"
            else:
                ratio = ovr_mean / cog_mean
                label = f"{ratio:.1f}x slower"
            ax.annotate(label,
                        xy=(len(means) - 2, ovr_mean),
                        xytext=(len(means) - 2.5, ovr_mean + max(means) * 0.15),
                        fontsize=11, color=C_V3_OVR, fontweight="bold",
                        arrowprops=dict(arrowstyle="->", color=C_V3_OVR))

    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, fontsize=11, rotation=15, ha="right")
    ax.set_ylabel("Read time (ms)", fontsize=12)
    ax.set_title("Single band read (local): the journey from 10x slower to competitive with COG",
                 fontsize=13, fontweight="bold", pad=15)

    # Add a horizontal line at COG level
    if means:
        cog_val = means[-1]
        ax.axhline(y=cog_val, color=C_COG, linestyle="--", alpha=0.5, linewidth=1)
        ax.text(len(means) - 0.5, cog_val + 3, "COG baseline",
                fontsize=9, color=C_COG, alpha=0.7)

    ax.set_ylim(0, max(means) * 1.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def chart_pr_impact(stats, out_path):
    """Chart 4: PR impact - default vs parallel for each format.

    Shows the effect of GDAL_NUM_THREADS=ALL_CPUS (auto-parallel IRead PR).
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("PR impact: GDAL_NUM_THREADS=ALL_CPUS (auto-parallel decode)",
                 fontsize=13, fontweight="bold", y=0.98)

    scenarios = [("single_band", "Single band read"),
                 ("zoom_5", "RGB 20x zoom (QGIS viewport)")]

    fmt_order = ["cog_zstd", "zarr_v2", "zarr_v3_zstd",
                 "zarr_v3_blosc", "zarr_v3_zstd_ovr"]

    for ax, (scenario, title) in zip(axes, scenarios):
        labels = []
        default_means = []
        parallel_means = []
        colors = []

        for fmt in fmt_order:
            scen = scenario
            if scen == "single_band":
                pass  # single_band is the same for all
            # For zoom_5, COG uses rgb_stacked if stacked COG exists, else skip
            d_key = (fmt, "local", scen, "default")
            p_key = (fmt, "local", scen, "parallel")

            if d_key in stats and p_key in stats:
                labels.append(FORMAT_LABELS.get(fmt, fmt))
                default_means.append(stats[d_key]["mean"])
                parallel_means.append(stats[p_key]["mean"])
                colors.append(FORMAT_COLORS.get(fmt, "#999"))

        if not labels:
            ax.set_visible(False)
            continue

        y_pos = np.arange(len(labels))
        h = 0.35

        bars_d = ax.barh(y_pos + h / 2, default_means, h,
                         color=[c + "80" for c in colors],  # 50% alpha
                         edgecolor="white", linewidth=0.5, label="1 thread")
        bars_p = ax.barh(y_pos - h / 2, parallel_means, h,
                         color=colors, edgecolor="white", linewidth=0.5,
                         label="ALL_CPUS")

        # Value labels + speedup
        for bd, bp, dm, pm in zip(bars_d, bars_p, default_means, parallel_means):
            max_m = max(default_means)
            ax.text(bd.get_width() + max_m * 0.02,
                    bd.get_y() + bd.get_height() / 2,
                    f"{dm:.0f}ms", va="center", fontsize=8, color="#666")
            speedup = dm / pm if pm > 0 else 0
            suffix = f"  ({speedup:.1f}x)" if speedup > 1.1 else ""
            ax.text(bp.get_width() + max_m * 0.02,
                    bp.get_y() + bp.get_height() / 2,
                    f"{pm:.0f}ms{suffix}", va="center", fontsize=9,
                    fontweight="bold")

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlabel("Read time (ms)", fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.invert_yaxis()
        ax.set_xlim(0, max(default_means) * 1.35)
        ax.legend(loc="lower right", fontsize=9)
        ax.xaxis.set_major_locator(ticker.MaxNLocator(5))

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def chart_cloud(stats, out_path):
    """Chart 2: Cloud read times - COG vs Zarr v2 vs Zarr v3."""
    fig, ax = plt.subplots(figsize=(12, 6))

    scenarios = ["single_band", "rgb_vrt", "zoom_25", "zoom_5", "full_extent"]
    fmt_order = ["cog", "zarr_v2", "zarr_v3"]

    x = np.arange(len(scenarios))
    width = 0.25
    offsets = [-width, 0, width]

    has_data = False
    for i, fmt in enumerate(fmt_order):
        means = []
        stds = []
        for scenario in scenarios:
            key = (fmt, "cloud", scenario, "default")
            if key in stats:
                means.append(stats[key]["mean"] / 1000)  # Convert to seconds
                stds.append(stats[key]["std"] / 1000)
            else:
                means.append(0)
                stds.append(0)

        if any(m > 0 for m in means):
            has_data = True
            bars = ax.bar(x + offsets[i], means, width, yerr=stds,
                          label=FORMAT_LABELS.get(fmt, fmt),
                          color=FORMAT_COLORS.get(fmt, "#999"),
                          edgecolor="white", linewidth=0.5, capsize=3)

            for bar, mean in zip(bars, means):
                if mean > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height() + 0.3,
                            f"{mean:.1f}s", ha="center", va="bottom",
                            fontsize=9, fontweight="bold")

    if not has_data:
        ax.text(0.5, 0.5, "No cloud data available yet",
                transform=ax.transAxes, ha="center", fontsize=14, color="#999")

    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIO_LABELS.get(s, s) for s in scenarios], fontsize=11)
    ax.set_ylabel("Read time (seconds)", fontsize=12)
    ax.set_title("Cloud read performance: COG vs Zarr v2 vs Zarr v3 sharded",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=11)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--all", action="store_true",
                   help="Load all CSVs instead of just the latest")
    p.add_argument("csvs", nargs="*", help="Specific CSV filenames to load")
    args = p.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    print("Loading results...")

    if args.csvs:
        rows = load_results(args.csvs)
    elif args.all:
        rows = load_results()
    else:
        rows = load_results(DEFAULT_CSVS)

    stats = aggregate(rows)
    print(f"  {len(stats)} unique groups")

    print("\nGenerating charts...")
    chart_local_hero(stats, OUT_DIR / "local_hero.png")
    chart_journey(stats, OUT_DIR / "journey.png")
    chart_cloud(stats, OUT_DIR / "cloud.png")

    # PR impact chart uses its own CSV (has both default + parallel configs)
    pr_path = RESULTS_DIR / PR_IMPACT_CSV
    if pr_path.exists():
        pr_rows = load_csv(pr_path)
        pr_stats = aggregate(pr_rows)
        print(f"  PR impact: {len(pr_rows)} rows, {len(pr_stats)} groups")
        chart_pr_impact(pr_stats, OUT_DIR / "pr_impact.png")
    else:
        print(f"  Skipped pr_impact chart ({PR_IMPACT_CSV} not found)")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
