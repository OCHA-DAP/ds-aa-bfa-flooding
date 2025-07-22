---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-aa-bfa-flooding
    language: python
    name: ds-aa-bfa-flooding
---

# SEAS5 trigger individual regions, additional regions
<!-- markdownlint-disable MD013 -->
Triggering in regions independently (Centre-Nord, Sahel, Boucle de Mouhoun, Sud-Ouest).

Basically a copy+paste of `seas5_trigger_individual` to include the extra regions.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import calendar

import pandas as pd
import numpy as np
import ocha_stratus as stratus
import matplotlib.pyplot as plt

from src.constants import *
from src.utils import rp_calc
```

## Load data

```python
engine = stratus.get_engine("prod")
pcodes = ADM1_AOI_PCODES_EXTRA
```

```python
query = f"""
SELECT *
FROM public.polygon
WHERE pcode IN {tuple(pcodes)}
"""
df_adm = pd.read_sql(query, engine)
```

```python
query = f"""
SELECT *
FROM public.seas5
WHERE pcode in {tuple(pcodes)}
"""
df_seas5 = pd.read_sql(
    query, engine, parse_dates=["valid_date", "issued_date"]
)
```

```python
df_seas5 = df_seas5.merge(df_adm)
```

```python
df_seas5["name"].unique()
```

```python
names_str = ", ".join(df_seas5["name"].unique())
```

## Plot seasonality

```python
for name, group in df_seas5.groupby("name"):
    fig, ax = plt.subplots(dpi=150)

    group.groupby(group["valid_date"].dt.month)["mean"].mean().plot.bar(ax=ax)

    ax.set_xlabel("Mois")
    ax.set_ylabel("Précipitations prévues moyennes (mm / jour)")
    ax.set_title(f"Précipitations sur {name}")

    ax.spines.top.set_visible(False)
    ax.spines.right.set_visible(False)
```

Looks like JAS is suitable for them all, more or less.

```python
season_months = [7, 8, 9]
```

```python
df_seas5_grouped = (
    df_seas5.groupby(["name", "pcode", "valid_date", "issued_date"])["mean"]
    .mean()
    .reset_index()
)
```

```python
df_seas5_grouped
```

```python
df_seas5_season = df_seas5_grouped[
    (df_seas5_grouped["valid_date"] == df_seas5_grouped["issued_date"])
    & (df_seas5_grouped["valid_date"].dt.month.isin(season_months))
    & (df_seas5_grouped["valid_date"].dt.year < 2025)
].copy()
df_seas5_season["year"] = df_seas5_season["valid_date"].dt.year
df_seas5_season["month"] = df_seas5_season["valid_date"].dt.month
df_seas5_season = df_seas5_season.drop(columns=["valid_date", "issued_date"])
```

```python
df_seas5_season = rp_calc.calculate_groups_rp(
    df_seas5_season, by=["pcode", "month"], ascending=False
)
```

```python
df_seas5_season
```

```python
total_years = df_seas5_season["year"].nunique()
```

```python
rp_ind = 3
```

```python
df_seas5_season["name"].unique()
```

```python
short_names = {
    "Boucle du Mouhoun": "B. Mouhoun",
    "Centre-Nord": "C.-Nord",
    "Sud-Ouest": "S.-Ouest",
}
```

```python
df_seas5_season["short_name"] = df_seas5_season["name"].replace(short_names)
```

## Plot thresholds and historical values

Specify the per-region, per-month return period, and use this to get the per-region, per-month thresholds.

Plot which historical years would have activated for each month, in each region.

```python
rp_ind = 3
```

```python
min_year = 2000

fig, axs = plt.subplots(nrows=3, figsize=(10, 10), sharex=True, dpi=200)

trig_colors = ["orange", "dodgerblue", "salmon", "limegreen", "orchid"]
vas = ["bottom", "top"]

df_plot = df_seas5_season.copy()
df_plot["trig"] = False

dicts = []
for i, (month, group) in enumerate(df_plot.groupby("month")):
    group = group.sort_values("mean_rp").copy()
    ax = axs[i]

    for j, (name, name_group) in enumerate(group.groupby("short_name")):
        color = trig_colors[j]
        thresh = np.interp(rp_ind, name_group["mean_rp"], name_group["mean"])
        name_group["trig"] = name_group["mean"] >= thresh

        # set triggered years for later per-region rp calculation
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
        offset = (j - 1.5) * bar_width

        # Set alpha based on threshold comparison
        alphas = name_group_plot["trig"].apply(lambda v: 1 if v else 0.2)

        # Plot individual bars with varying alpha
        for xi, yi, alpha in zip(x, values, alphas):
            ax.bar(xi + offset, yi, width=bar_width, color=color, alpha=alpha)

        ax.axhline(thresh, color=color, linestyle="--", linewidth=1)
        annotation = f"    {name} : " + f"{thresh:.2f} mm".replace(".", ",")
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
            (len(name_group_plot) - 0.5, thresh + y_adj),
            va="center",
            color=color,
            fontsize=8,
        )

    ax.set_xticks(x)
    ax.set_xlim((-1, len(name_group_plot)))
    ax.set_xticklabels(name_group_plot["year"], rotation=90)
    ax.set_title(FRENCH_MONTHS[calendar.month_abbr[month]].capitalize())

    if i == 1:
        ax.set_ylabel(
            "Précipitations quotidiennes moyennes prévues (mm) [SEAS5]"
        )

    ax.spines.top.set_visible(False)
    ax.spines.right.set_visible(False)

    dicts.append({"month": month, "thresh": thresh})

rp_region = (total_years + 1) / df_plot[df_plot["trig"]].groupby("name")[
    "year"
].nunique().mean()
rp_com = (total_years + 1) / df_plot[df_plot["trig"]]["year"].nunique()

axs[-1].set_xlabel("Année")

fig.suptitle(
    f"Précipitations sur {names_str},\nmoyenne sur toute la région, prévues avec délai de 0 mois\n",
    y=0.99,
)

rp_str = (
    "Périodes de retour :\n"
    f"Par mois, par région = {rp_ind:.1f} ans ; "
    f"Par région (moyenne) = {rp_region:.1f} ans ; "
    f"Globale = {rp_com:.1f} ans"
).replace(".", ",")

fig.text(
    0.5,
    0.92,
    rp_str,
    ha="center",
    fontsize=10,
    color="gray",
)
```

## Current values

Just looking at the values from July 2025, since this is excluded from the plots above.

Looks like we would've actually triggered for Sud-Ouest

```python
df_seas5_grouped[
    (df_seas5_grouped["valid_date"] == "2025-07-01")
    & (df_seas5_grouped["issued_date"] == "2025-07-01")
]
```

```python

```
