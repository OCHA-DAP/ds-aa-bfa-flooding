"""BFA flooding AA — SEAS5 monthly trigger monitoring plot.

Run monthly (Jul/Aug/Sep) once the new SEAS5 forecast lands in the prod DB.
SEAS5 is released on the 5th of the month; the DB `issued_date` is the 1st
by convention, so quote the 5th when stating the issuance date publicly.

    python -m src.monitoring.seas5_monitoring \
        plots/bfa_seas5_monitoring_YYYY-MM.png

Prints each region's leadtime-0 forecast against its threshold and whether
the trigger is reached, then writes the three-panel JAS plot.

Mirrors the "Monitoring" section of
analysis/seas5_trigger_individual_extraregions.ipynb.

Thresholds are FIXED from the 2025 framework: empirical 3-year RP per
region-month, leadtime-0 (issued_date == valid_date) JAS forecasts,
baseline years 1981-2024. Do NOT extend the baseline when rerunning — the
`valid_date.dt.year < 2025` filter below is deliberate. The thresholds this
reproduces are checked against the notebook's stored `df_threshs` output.
"""

import calendar
import sys

import matplotlib.pyplot as plt
import numpy as np
import ocha_stratus as stratus
import pandas as pd

from src.constants import ADM1_AOI_PCODES_EXTRA, FRENCH_MONTHS
from src.utils import rp_calc

OUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "monitoring.png"

season_months = [7, 8, 9]
rp_ind = 3
short_names = {
    "Boucle du Mouhoun": "B. Mouhoun",
    "Centre-Nord": "C.-Nord",
    "Sud-Ouest": "S.-Ouest",
}
trig_colors = ["orange", "dodgerblue", "salmon", "limegreen", "orchid"]

# ---------------------------------------------------------------- load data
engine = stratus.get_engine("prod")
pcodes = ADM1_AOI_PCODES_EXTRA

df_adm = pd.read_sql(
    f"SELECT * FROM public.polygon WHERE pcode IN {tuple(pcodes)}", engine
)
df_seas5 = pd.read_sql(
    f"SELECT * FROM public.seas5 WHERE pcode in {tuple(pcodes)}",
    engine,
    parse_dates=["valid_date", "issued_date"],
)
df_seas5 = df_seas5.merge(df_adm)
names_str = ", ".join(df_seas5["name"].unique())

df_seas5_grouped = (
    df_seas5.groupby(["name", "pcode", "valid_date", "issued_date"])["mean"]
    .mean()
    .reset_index()
)

# ------------------------------------------- baseline (1981-2024) thresholds
df_seas5_season = df_seas5_grouped[
    (df_seas5_grouped["valid_date"] == df_seas5_grouped["issued_date"])
    & (df_seas5_grouped["valid_date"].dt.month.isin(season_months))
    & (df_seas5_grouped["valid_date"].dt.year < 2025)
].copy()
df_seas5_season["year"] = df_seas5_season["valid_date"].dt.year
df_seas5_season["month"] = df_seas5_season["valid_date"].dt.month
df_seas5_season = df_seas5_season.drop(columns=["valid_date", "issued_date"])
df_seas5_season = rp_calc.calculate_groups_rp(
    df_seas5_season, by=["pcode", "month"], ascending=False
)
df_seas5_season["short_name"] = df_seas5_season["name"].replace(short_names)
total_years = df_seas5_season["year"].nunique()

thresh_dicts = []
for month, group in df_seas5_season.groupby("month"):
    group = group.sort_values("mean_rp")
    for name, name_group in group.groupby("short_name"):
        thresh_dicts.append(
            {
                "month": month,
                "pcode": name_group.iloc[0]["pcode"],
                "short_name": name,
                "thresh": np.interp(
                    rp_ind, name_group["mean_rp"], name_group["mean"]
                ),
            }
        )
df_threshs = pd.DataFrame(thresh_dicts)

# ------------------------------------------------------- monitoring dataset
df_recent = df_seas5_grouped[
    (df_seas5_grouped["valid_date"] == df_seas5_grouped["issued_date"])
    & (df_seas5_grouped["valid_date"].dt.month.isin(season_months))
].copy()
df_recent["year"] = df_recent["valid_date"].dt.year
df_recent["month"] = df_recent["valid_date"].dt.month
df_recent = df_recent.drop(columns=["valid_date", "issued_date"])
df_recent["short_name"] = df_recent["name"].replace(short_names)

# ------------------------------------------------------------------- plot
min_year = 2020
fig, axs = plt.subplots(nrows=3, figsize=(10, 10), sharex=True, dpi=100)

df_plot = df_recent.copy()
df_plot["trig"] = False
years = range(min_year, df_plot["year"].max() + 1)

for i, (month, group) in enumerate(df_plot.groupby("month")):
    ax = axs[i]

    for j, (name, name_group) in enumerate(group.groupby("short_name")):
        color = trig_colors[j]
        pcode = name_group.iloc[0]["pcode"]
        thresh = df_threshs[
            (df_threshs["pcode"] == pcode) & (df_threshs["month"] == month)
        ].iloc[0]["thresh"]
        name_group["trig"] = name_group["mean"] >= thresh

        df_plot.loc[
            (df_plot["mean"] >= thresh)
            & (df_plot["short_name"] == name)
            & (df_plot["month"] == month),
            "trig",
        ] = True

        name_group_plot = name_group[
            name_group["year"] >= min_year
        ].sort_values("year")

        x = np.arange(len(name_group_plot.index))
        bar_width = 0.15
        values = name_group_plot["mean"]
        offset = (j - 2) * bar_width
        alphas = name_group_plot["trig"].apply(lambda v: 1 if v else 0.2)

        for xi, yi, alpha in zip(x, values, alphas):
            ax.bar(xi + offset, yi, width=bar_width, color=color, alpha=alpha)

        ax.axhline(thresh, color=color, linestyle="--", linewidth=1)
        annotation = f" {name} : " + f"{thresh:.2f} mm".replace(".", ",")
        y_adj = 0
        if month == 7:
            if name in ["C.-Nord", "Nord"]:
                y_adj = 0.15
            if name == "S.-Ouest":
                y_adj = -0.15
        if month == 9:
            if name == "Nord":
                y_adj = 0.1
            if name == "C.-Nord":
                y_adj = -0.1

        ax.annotate(
            annotation,
            (len(years) - 0.5, thresh + y_adj),
            va="center",
            color=color,
            fontsize=8,
        )

    ax.set_xticks(range(len(years)))
    ax.set_xlim((-0.75, len(years) - 0.5))
    ax.set_xticklabels(years, rotation=90)
    ax.set_title(FRENCH_MONTHS[calendar.month_abbr[month]].capitalize())

    if i == 1:
        ax.set_ylabel(
            "Précipitations quotidiennes moyennes prévues (mm) [SEAS5]"
        )

    ax.spines.top.set_visible(False)
    ax.spines.right.set_visible(False)

rp_region = (total_years + 1) / df_plot[df_plot["trig"]].groupby("name")[
    "year"
].nunique().mean()
rp_com = (total_years + 1) / df_plot[df_plot["trig"]]["year"].nunique()

axs[-1].set_xlabel("Année")
fig.suptitle(
    f"Précipitations sur {names_str},\nmoyenne sur toute la région, "
    "prévues avec délai de 0 mois\n",
    y=0.99,
)
rp_str = (
    "Périodes de retour :\n"
    f"Par mois, par région = {rp_ind:.1f} ans ; "
    f"Par région (moyenne) = {rp_region:.1f} ans ; "
    f"Globale = {rp_com:.1f} ans"
).replace(".", ",")
fig.text(0.5, 0.92, rp_str, ha="center", fontsize=10, color="gray")

fig.savefig(OUT_PATH, bbox_inches="tight")
print(f"saved {OUT_PATH}")

# ------------------------------------------------------------- status report
print("\n=== thresholds (fixed, 1981-2024 baseline) ===")
print(df_threshs.to_string(index=False))

current_year = df_recent["year"].max()
current = df_recent[df_recent["year"] == current_year].merge(
    df_threshs[["month", "pcode", "thresh"]], on=["month", "pcode"]
)
current["trig"] = current["mean"] >= current["thresh"]
current["pct_of_thresh"] = 100 * current["mean"] / current["thresh"]
print(f"\n=== {current_year} leadtime-0 forecasts vs thresholds ===")
print(
    current[["month", "short_name", "mean", "thresh", "pct_of_thresh", "trig"]]
    .sort_values(["month", "short_name"])
    .to_string(index=False)
)
print(f"\nAny {current_year} trigger: {current['trig'].any()}")
print(f"Latest issued_date in DB: {df_seas5_grouped['issued_date'].max()}")
