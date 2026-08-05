"""Single-month, leadtime-0 SEAS5 forecast skill per admin1 region.

The team's `ds-seas5-skill` repo computes skill per issue-month x target
TRIMESTER at admin0/pixel level. The BFA flooding trigger uses single-month,
leadtime-0 forecasts per admin1, so this applies the same methodology at that
granularity:

  1. Aggregate SEAS5 (issued month == valid month) and ERA5 to one value per
     year for the target calendar month.
  2. log1p-transform both (precipitation -> closer to Gaussian).
  3. Normalize SEAS5 in log space to match ERA5 mean/std over the overlap.
  4. Pearson r between normalized forecast and observation = the skill metric.

Also computes upper-tail ROC-AUC (does a high forecast actually discriminate
a wet month?), which is the alert-relevant question here — the trigger fires
on the upper tail, and overall correlation can look fine while upper-tail
discrimination is poor.

    python -m src.monitoring.seas5_skill <out.png>

Reference: OCHA-DAP/ds-seas5-skill, src/skill.py (normalize_seas5,
compute_skill_metrics, compute_roc_auc).
"""

import sys

import matplotlib.pyplot as plt
import numpy as np
import ocha_stratus as stratus
import pandas as pd
from matplotlib.colors import TwoSlopeNorm
from scipy.stats import mannwhitneyu, pearsonr

from src.constants import ADM1_AOI_PCODES_EXTRA, FRENCH_MONTHS

OUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "skill.png"

SEASON_MONTHS = [7, 8, 9]
MIN_YEARS = 10  # matches ds-seas5-skill constants.MIN_YEARS
SHORT_NAMES = {
    "Boucle du Mouhoun": "B. Mouhoun",
    "Centre-Nord": "C.-Nord",
    "Sud-Ouest": "S.-Ouest",
}


def normalize_forecast(f_log: np.ndarray, o_log: np.ndarray) -> np.ndarray:
    """Scale forecast so mean/std match obs over the overlap (log space)."""
    if len(f_log) < 2:
        return f_log
    f_sd = max(f_log.std(ddof=1), 1e-9)
    return (f_log - f_log.mean()) / f_sd * o_log.std(ddof=1) + o_log.mean()


def roc_auc(f: np.ndarray, labels: np.ndarray) -> tuple[float, float]:
    """AUC for 'higher forecast => event', via Mann-Whitney U.

    Returns (auc, one-sided p-value against the no-skill null AUC = 0.5).
    """
    pos, neg = f[labels == 1], f[labels == 0]
    if len(pos) < 2 or len(neg) < 2:
        return float("nan"), float("nan")
    stat, p = mannwhitneyu(pos, neg, alternative="greater")
    return float(stat / (len(pos) * len(neg))), float(p)


# ------------------------------------------------------------------ load data
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
df_era5 = pd.read_sql(
    f"SELECT * FROM public.era5 WHERE pcode in {tuple(pcodes)}",
    engine,
    parse_dates=["valid_date"],
)

seas5 = (
    df_seas5.groupby(["pcode", "valid_date", "issued_date"])["mean"]
    .mean()
    .reset_index()
)
era5 = df_era5.groupby(["pcode", "valid_date"])["mean"].mean().reset_index()
names = df_adm.set_index("pcode")["name"].to_dict()

# ------------------------------------------------------------------ compute
rows = []
for pcode in pcodes:
    for month in SEASON_MONTHS:
        f = seas5[
            (seas5["pcode"] == pcode)
            & (seas5["issued_date"] == seas5["valid_date"])
            & (seas5["valid_date"].dt.month == month)
        ]
        o = era5[
            (era5["pcode"] == pcode) & (era5["valid_date"].dt.month == month)
        ]
        f_yr = f.set_index(f["valid_date"].dt.year)["mean"]
        o_yr = o.set_index(o["valid_date"].dt.year)["mean"]
        yrs = sorted(set(f_yr.index) & set(o_yr.index))
        if len(yrs) < MIN_YEARS:
            continue

        f_log = np.log1p(f_yr.loc[yrs].clip(lower=0).values)
        o_log = np.log1p(o_yr.loc[yrs].clip(lower=0).values)
        f_norm = normalize_forecast(f_log, o_log)

        upper_tercile = o_log >= np.percentile(o_log, 200 / 3)
        auc, auc_p = roc_auc(f_norm, upper_tercile.astype(int))
        r, r_p = pearsonr(f_norm, o_log)
        rows.append(
            {
                "pcode": pcode,
                "name": names[pcode],
                "short_name": SHORT_NAMES.get(names[pcode], names[pcode]),
                "month": month,
                "n_years": len(yrs),
                "year_min": min(yrs),
                "year_max": max(yrs),
                "pearson_r": float(r),
                "pearson_p": float(r_p / 2),  # one-sided: skill > 0
                "rmse": float(np.sqrt(((o_log - f_norm) ** 2).mean())),
                "auc_upper": auc,
                "auc_p": auc_p,
            }
        )

df_skill = pd.DataFrame(rows)
print(df_skill.to_string(index=False))

# ------------------------------------------------------------------ plot
order = ["B. Mouhoun", "C.-Nord", "Nord", "S.-Ouest", "Sahel"]


def _pivot(col):
    return df_skill.pivot(
        index="short_name", columns="month", values=col
    ).reindex(order)


r_mat, a_mat = _pivot("pearson_r"), _pivot("auc_upper")
r_p_mat, a_p_mat = _pivot("pearson_p"), _pivot("auc_p")
ALPHA = 0.05

month_labels = [
    FRENCH_MONTHS[
        ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep"][m]
    ].capitalize()
    for m in SEASON_MONTHS
]

fig, axs = plt.subplots(
    ncols=2, figsize=(10.5, 3.4), dpi=200, gridspec_kw={"wspace": 0.45}
)

panels = [
    (
        axs[0],
        r_mat,
        r_p_mat,
        "Corrélation (r) prévision / observation",
        0.0,
        0.7,
    ),
    (
        axs[1],
        a_mat,
        a_p_mat,
        "Discrimination du tiers le plus humide (ROC-AUC)",
        0.5,
        0.9,
    ),
]

for ax, mat, p_mat, title, center, vmax in panels:
    norm = TwoSlopeNorm(vmin=2 * center - vmax, vcenter=center, vmax=vmax)
    ax.imshow(mat.values, cmap="RdBu", norm=norm, aspect="auto")

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat.values[i, j]
            if np.isnan(v):
                continue
            # Not distinguishable from no skill -> hatch, so colour alone never
            # implies a cell is informative when it isn't.
            if p_mat.values[i, j] >= ALPHA:
                ax.add_patch(
                    plt.Rectangle(
                        (j - 0.5, i - 0.5),
                        1,
                        1,
                        fill=False,
                        hatch="////",
                        edgecolor="#8c9698",
                        linewidth=0,
                    )
                )
            ax.text(
                j,
                i,
                f"{v:.2f}".replace(".", ","),
                ha="center",
                va="center",
                fontsize=9,
                color="#1f2324",
            )

    ax.set_xticks(range(len(month_labels)))
    ax.set_xticklabels(month_labels, fontsize=9)
    ax.set_yticks(range(len(mat.index)))
    ax.set_yticklabels(mat.index, fontsize=9)
    ax.set_title(title, fontsize=9.5, pad=8)
    ax.set_xticks(np.arange(-0.5, mat.shape[1], 1), minor=True)
    ax.set_yticks(np.arange(-0.5, mat.shape[0], 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=2)
    ax.tick_params(which="minor", length=0)
    for s in ax.spines.values():
        s.set_visible(False)

n_min, n_max = df_skill["n_years"].min(), df_skill["n_years"].max()
y0, y1 = df_skill["year_min"].min(), df_skill["year_max"].max()
n_str = f"{n_min}" if n_min == n_max else f"{n_min}–{n_max}"
fig.suptitle(
    "Compétence des prévisions SEAS5 à délai de 0 mois, "
    "par région et par mois",
    fontsize=11,
    y=1.06,
)
fig.text(
    0.5,
    -0.16,
    f"SEAS5 vs ERA5, {y0}–{y1} ({n_str} ans). Gauche : corrélation de "
    "Pearson (0 = aucune compétence).\nDroite : capacité à distinguer le "
    "tiers des mois les plus humides (0,5 = équivalent au hasard).\n"
    "Les cases hachurées ne se distinguent pas statistiquement d’une absence "
    "de compétence (p ≥ 0,05).",
    ha="center",
    fontsize=8,
    color="#5e6a6b",
)

fig.savefig(OUT_PATH, bbox_inches="tight")
print(f"\nsaved {OUT_PATH}")
