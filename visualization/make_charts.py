"""PART 4 - Static visualizations (PNG) built from the CSV results in output/results.

Each chart is skipped with a message when its input CSV has not been produced yet
(e.g. MongoDB / integration results before the databases are loaded).

Usage:  python visualization/make_charts.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

from common.config import FIGURES_DIR, RESULTS_DIR

# Colour palette: colour-blind-safe categorical colours in a fixed order + a single-hue blue ramp.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SEQ_BLUE = LinearSegmentedColormap.from_list(
    "seq_blue", ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"])
ORDINAL_BLUE = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "axes.titlecolor": INK,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "xtick.color": INK_2, "ytick.color": INK_2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False,
})

money = mticker.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M" if abs(v) >= 1e6 else f"${v / 1e3:.0f}K")


def load(name):
    path = os.path.join(RESULTS_DIR, name)
    if not os.path.exists(path):
        print(f"  skip: {name} not found")
        return None
    return pd.read_csv(path)


def save(fig, name, note=None):
    if note:
        fig.text(0.01, -0.03, note, fontsize=8, color=INK_2)
    os.makedirs(FIGURES_DIR, exist_ok=True)
    fig.savefig(os.path.join(FIGURES_DIR, name), dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {name}")


def daily_revenue():
    df = load("spark_sql_A1_daily_revenue.csv")
    if df is None:
        return
    df["txn_date"] = pd.to_datetime(df["txn_date"])
    full = df.iloc[1:-1]  # first and last day of the 90-day window are partial days
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(full.txn_date, full.revenue, color=BLUE, linewidth=1, alpha=0.45, label="Daily revenue")
    ma = full.revenue.rolling(7).mean()
    ax.plot(full.txn_date, ma, color=BLUE, linewidth=2.2, label="7-day moving average")
    ax.yaxis.set_major_formatter(money)
    ax.set_ylim(0, full.revenue.max() * 1.15)
    ax.set_title("Daily revenue is flat at about $2.8M/day across the 90-day window")
    ax.set_ylabel("Revenue")
    ax.legend(loc="lower right")
    last = full.iloc[-1]
    ax.annotate(f"7-day avg {money(ma.iloc[-1], None)}", (last.txn_date, ma.iloc[-1]),
                xytext=(-10, 12), textcoords="offset points", ha="right", color=INK, fontsize=9)
    save(fig, "fig1_daily_revenue.png", "Source: Spark SQL A1 over curated transactions (partial first/last days excluded).")


def category_revenue():
    df = load("spark_sql_A2_category_scorecard.csv")
    if df is None:
        return
    df = df.sort_values("revenue")
    fig, ax = plt.subplots(figsize=(10, 7))
    y = range(len(df))
    ax.barh(y, df.revenue, color=BLUE, height=0.42, label="Revenue", align="edge")
    ax.barh([i - 0.42 for i in y], df.est_gross_profit, color=ORANGE, height=0.42,
            label="Estimated gross profit", align="edge")
    ax.set_yticks([i for i in y])
    ax.set_yticklabels(df.category_name, fontsize=8)
    ax.xaxis.set_major_formatter(money)
    ax.grid(axis="y", visible=False)
    ax.set_title("Revenue is evenly spread across categories, but margins change the profit ranking")
    ax.legend(loc="lower right")
    top_profit = df.loc[df.est_gross_profit.idxmax()]
    ax.text(0.99, 0.12, f"Most profitable: {top_profit.category_name}\n"
                        f"({top_profit.avg_profit_margin:.0%} avg margin, {money(top_profit.est_gross_profit, None)})",
            transform=ax.transAxes, ha="right", fontsize=9, color=INK)
    save(fig, "fig2_category_revenue_profit.png",
         "Source: Spark SQL A2. Profit = revenue x average subcategory profit margin of the category.")


def conversion_funnel():
    df = load("spark_sql_A3_funnel_by_device_referrer.csv")
    if df is None:
        return
    total = df[df.device_type.isna() & df.referrer.isna()].iloc[0]
    stages = [("Sessions", total.sessions), ("Viewed a product", total.viewed_product),
              ("Added to cart", total.added_to_cart), ("Reached checkout", total.reached_checkout),
              ("Purchased", total.converted)]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(12, 4.4), gridspec_kw={"width_ratios": [1.6, 1]})
    labels, values = zip(*stages)
    ax.barh(range(len(stages))[::-1], values, color=ORDINAL_BLUE, height=0.65)
    for i, (lab, v) in enumerate(stages):
        pct = v / stages[0][1]
        step = "" if i == 0 else f"  ({v / stages[i - 1][1]:.0%} of previous)"
        ax.text(v + 15000, len(stages) - 1 - i, f"{v:,.0f} | {pct:.0%}{step}", va="center", fontsize=8.5, color=INK)
    ax.set_yticks(range(len(stages))[::-1])
    ax.set_yticklabels(labels)
    ax.set_xlim(0, stages[0][1] * 1.75)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1e3:.0f}K"))
    ax.grid(axis="y", visible=False)
    ax.set_title("Checkout is the biggest leak:\n54% of checkouts do not complete")

    dev = df[df.device_type.notna() & df.referrer.isna()].sort_values("conversion_rate_pct")
    ax2.barh(dev.device_type, dev.conversion_rate_pct, color=BLUE, height=0.5)
    for i, v in enumerate(dev.conversion_rate_pct):
        ax2.text(v + 0.3, i, f"{v:.2f}%", va="center", fontsize=9, color=INK)
    ax2.set_xlim(0, 25)
    ax2.set_title("Conversion by device")
    ax2.set_xlabel("Sessions that purchased (%)")
    ax2.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "fig3_conversion_funnel.png", "Source: Spark SQL A3 over 1M sessions.")


def viewed_vs_sold():
    df = load("spark_sql_A4_viewed_vs_sold.csv")
    if df is None:
        return
    sold_out = df.current_stock == 0
    fig, ax = plt.subplots(figsize=(10, 5))
    for mask, color, label in [(~sold_out, BLUE, "In stock"), (sold_out, ORANGE, "Sold out by end of window")]:
        part = df[mask]
        ax.scatter(part.views, part.units_sold, color=color, s=9, alpha=0.6, linewidths=0,
                   label=f"{label} ({len(part):,} products, avg {part.units_sold.mean():.0f} units)")
    top_rev = df.nlargest(1, "revenue").iloc[0]
    ax.scatter(top_rev.views, top_rev.units_sold, s=70, facecolors="none", edgecolors=INK, linewidths=1.5)
    ax.annotate(f"Top revenue: {top_rev.product_id}\n{top_rev.views:.0f} views, {top_rev.units_sold:.0f} units",
                (top_rev.views, top_rev.units_sold), xytext=(-175, -4), textcoords="offset points", fontsize=8.5, color=INK)
    ax.set_xlim(0, df.views.max() * 1.05)
    ax.set_ylim(0, df.units_sold.max() * 1.12)
    ax.set_title(f"Stock-outs cap demand: {sold_out.mean():.0%} of products sold out and lost visibility")
    ax.set_xlabel("Product detail views (1M sessions)")
    ax.set_ylabel("Units sold")
    ax.legend(loc="lower right")
    save(fig, "fig4_viewed_vs_sold.png",
         "Source: Spark SQL A4 (FULL OUTER JOIN of page views, cart items and transaction items).")


def cohort_heatmap():
    df = load("spark_cohort_matrix.csv")
    if df is None:
        return
    pv = df.pivot(index="registration_month", columns="txn_month", values="revenue_per_cohort_user").fillna(0)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    im = ax.imshow(pv.values, cmap=SEQ_BLUE, aspect="auto")
    ax.set_xticks(range(len(pv.columns)), [c + ("*" if c == pv.columns[-1] else "") for c in pv.columns])
    ax.set_yticks(range(len(pv.index)), pv.index)
    ax.grid(False)
    for i in range(pv.shape[0]):
        for j in range(pv.shape[1]):
            v = pv.values[i, j]
            ax.text(j, i, f"${v:,.0f}", ha="center", va="center", fontsize=8.5,
                    color="#ffffff" if v > pv.values.max() * 0.6 else INK)
    fig.colorbar(im, ax=ax, pad=0.01).set_label("Revenue per cohort user ($)", color=INK_2)
    ax.set_xlabel("Purchase month")
    ax.set_ylabel("Registration cohort")
    ax.set_title("Every registration cohort spends almost the same per user each month")
    save(fig, "fig5_cohort_heatmap.png", "Source: Spark cohort job. *Aug and Nov are partial months of the 90-day window.")


def weekday_hour():
    df = load("spark_sql_A5_weekday_hour.csv")
    if df is None:
        return
    pv = df.pivot(index="weekday_num", columns="hour", values="sessions")
    names = df.drop_duplicates("weekday_num").set_index("weekday_num").weekday
    fig, ax = plt.subplots(figsize=(11, 3.6))
    im = ax.imshow(pv.values, cmap=SEQ_BLUE, aspect="auto")
    ax.set_yticks(range(len(pv.index)), [names[i] for i in pv.index])
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}h" for h in range(0, 24, 2)])
    ax.grid(False)
    fig.colorbar(im, ax=ax, pad=0.01).set_label("Sessions", color=INK_2)
    ax.set_title("Traffic is uniform by weekday and hour (no peak-time signal in synthetic data)")
    save(fig, "fig6_weekday_hour_traffic.png", "Source: Spark SQL A5 (UTC).")


def mongo_segments():
    df = load("mongo_user_segmentation.csv")
    if df is None:
        return
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(df.segment, df.users, color=BLUE, width=0.6)
    for i, row in df.reset_index().iterrows():
        ax.text(i, row.users, f"{row.users:,}\navg ${row.avg_spent:,.0f}", ha="center", va="bottom", fontsize=8.5, color=INK)
    ax.set_ylabel("Users")
    ax.set_ylim(0, df.users.max() * 1.25)
    ax.grid(axis="x", visible=False)
    ax.tick_params(axis="x", labelsize=8.5)
    ax.set_title("Purchase-frequency segments (MongoDB $bucket)")
    save(fig, "fig7_mongo_user_segments.png", "Source: MongoDB aggregation pipeline 2 (user_segmentation).")


def clv_segments():
    seg = load("integration_clv_segments.csv")
    dec = load("integration_clv_deciles.csv")
    if seg is None or dec is None:
        return
    seg = seg.sort_values("avg_clv_12m")
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.3), gridspec_kw={"width_ratios": [1.2, 1]})
    ax.barh(seg.segment, seg.avg_clv_12m, color=BLUE, height=0.55)
    for i, row in enumerate(seg.itertuples()):
        ax.text(row.avg_clv_12m, i, f"  ${row.avg_clv_12m:,.0f}  ({row.users:,} users)", va="center", fontsize=8.5, color=INK)
    ax.set_xlim(0, seg.avg_clv_12m.max() * 1.6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v / 1e3:.0f}K"))
    ax.set_xlabel("Average predicted 12-month CLV (gross profit)")
    ax.grid(axis="y", visible=False)
    ax.set_title("Predicted 12-month CLV by RFM segment")

    ax2.bar(dec.clv_decile.astype(str), dec.share_of_clv_pct, color=BLUE, width=0.6)
    top = dec.iloc[0]
    ax2.text(0, top.share_of_clv_pct, f"{top.share_of_clv_pct:.1f}%", ha="center", va="bottom", fontsize=9, color=INK)
    ax2.set_xlabel("CLV decile (1 = highest)")
    ax2.set_ylabel("Share of total predicted CLV (%)")
    ax2.grid(axis="x", visible=False)
    ax2.set_title("Concentration of value by decile")
    fig.tight_layout()
    save(fig, "fig8_clv_segments.png", "Source: integration/clv_integration.py (MongoDB users + transactions, HBase sessions, Spark).")


def architecture():
    """System architecture diagram used in the report (not data-driven)."""
    from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

    fig, ax = plt.subplots(figsize=(11, 4.6))
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 4.6)
    ax.axis("off")

    def box(x, y, w, h, title, body, edge=BLUE):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                    facecolor="#ffffff", edgecolor=edge, linewidth=1.6))
        ax.text(x + w / 2, y + h - 0.22, title, ha="center", va="top", fontsize=10, fontweight="bold", color=INK)
        ax.text(x + w / 2, y + h - 0.55, body, ha="center", va="top", fontsize=8, color=INK_2, linespacing=1.35)

    def arrow(x1, y1, x2, y2, label="", at=None):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, color=INK_2, linewidth=1.2))
        if label:
            lx, ly = at or ((x1 + x2) / 2, (y1 + y2) / 2 + 0.1)
            ax.text(lx, ly, label, ha="center", fontsize=7.5, color=INK_2,
                    bbox=dict(facecolor=SURFACE, edgecolor="none", pad=0.5))

    box(0.1, 1.5, 1.9, 1.6, "Generator", "dataset_generator.py\nJSON arrays\n1M sessions\n250k transactions")
    box(2.6, 1.5, 2.1, 1.6, "Spark batch", "01_clean_normalize\nexplicit schemas\ncleaning, explode\n-> Parquet (curated)", ORANGE)
    box(5.4, 3.0, 2.3, 1.45, "MongoDB", "categories, products\nusers (+purchase_summary)\ntransactions (items)\nsessions sample")
    box(5.4, 0.15, 2.3, 1.45, "HBase", "user_sessions\n user#rev_ts#session\n families s / pv / c\nproduct_daily_metrics")
    box(5.4, 1.75, 2.3, 1.1, "Parquet lake", "C:/aucadata/curated\n10 tables", ORANGE)
    box(8.4, 1.5, 2.5, 1.6, "Spark analytics", "02 recommendations\n03 cohorts, 04 Spark SQL\nCLV integration (RFM)\n-> CSV -> figures/report", ORANGE)

    arrow(2.0, 2.3, 2.6, 2.3, "raw JSON")
    arrow(2.0, 2.9, 5.4, 3.7, "pymongo loader", at=(3.4, 3.5))
    arrow(2.0, 1.7, 5.4, 0.9, "happybase (Thrift)", at=(3.4, 1.05))
    arrow(4.7, 2.3, 5.4, 2.3)
    arrow(4.7, 1.8, 5.4, 0.6, "daily metrics", at=(4.75, 1.25))
    arrow(7.7, 3.7, 8.4, 2.9, "$group pushdown", at=(8.6, 3.55))
    arrow(7.7, 2.3, 8.4, 2.3)
    arrow(7.7, 0.9, 8.4, 1.7, "prefix / range scans", at=(8.75, 1.0))
    save(fig, "fig0_architecture.png")


if __name__ == "__main__":
    print("Building figures in", FIGURES_DIR)
    architecture()
    for chart in [daily_revenue, category_revenue, conversion_funnel, viewed_vs_sold,
                  cohort_heatmap, weekday_hour, mongo_segments, clv_segments]:
        chart()
