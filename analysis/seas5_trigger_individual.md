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

# SEAS5 trigger individual regions
<!-- markdownlint-disable MD013 -->
Triggering in regions independently (only Centre-Nord and Sahel, for regional fund).

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
pcodes = ADM1_AOI_PCODES
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

Looks like JAS is suitable for them all.

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
df_seas5_season
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

## Plot thresholds and historical values

Specify the per-region, per-month return period, and use this to get the per-region, per-month thresholds.

Plot which historical years would have activated for each month, in each region.

```python
rp_ind = 3
```

```python
min_year = 2000

fig, axs = plt.subplots(nrows=3, figsize=(10, 10), sharex=True, dpi=200)

trig_colors = ["darkorange", "rebeccapurple"]
vas = ["bottom", "top"]

df_plot = df_seas5_season.copy()
df_plot["trig"] = False

triggered_years = []

dicts = []
for i, (month, group) in enumerate(df_plot.groupby("month")):
    group = group.sort_values("mean_rp").copy()
    ax = axs[i]

    for j, (name, name_group) in enumerate(group.groupby("name")):
        color = trig_colors[j]
        thresh = np.interp(rp_ind, name_group["mean_rp"], name_group["mean"])
        name_group["trig"] = name_group["mean"] >= thresh
        triggered_years.extend(name_group[name_group["trig"]]["year"])

        name_group_plot = name_group[
            name_group["year"] >= min_year
        ].sort_values("year")

        x = np.arange(len(name_group_plot.index))
        bar_width = 0.3
        values = name_group_plot["mean"]
        offset = (j - 0.5) * bar_width

        # Set alpha based on threshold comparison
        alphas = name_group_plot["trig"].apply(lambda v: 0.9 if v else 0.2)

        # Plot individual bars with varying alpha
        for xi, yi, alpha in zip(x, values, alphas):
            ax.bar(xi + offset, yi, width=bar_width, color=color, alpha=alpha)

        ax.axhline(thresh, color=color, linestyle="--", linewidth=1)
        annotation = f"    {name} \n    {thresh:.2f} mm"
        if j == 0:
            annotation = annotation + "\n"
        else:
            annotation = "\n" + annotation
        ax.annotate(
            annotation,
            (len(name_group_plot) - 0.5, thresh),
            va="center",
            color=color,
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

rp_com = (total_years + 1) / len(set(triggered_years))

axs[-1].set_xlabel("Année")

fig.suptitle(
    f"Précipitations sur {names_str}, moyenne sur toute la zone, prévues avec délai de 0 mois\n"
    f"Période de retour par mois, par région = {rp_ind:.1f} ans; Période de retour globale = {rp_com:.1f} ans",
    y=0.95,
)
```

## Current values

Just looking at the values from July 2025, since this is excluded from the plots above.

Looks like neither one triggered.

```python
df_seas5_grouped[
    (df_seas5_grouped["valid_date"] == "2025-07-01")
    & (df_seas5_grouped["issued_date"] == "2025-07-01")
]
```

```python

```
