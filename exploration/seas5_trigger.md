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

# SEAS5 2025 trigger

Combining raster stats for Sahel, Boucle du Mouhoun, Centre-Nord

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
val_col = "mean"
weight_col = "seas5_frac_raw_pixels"
df_seas5["val_weighted"] = df_seas5[val_col] * df_seas5[weight_col]
```

```python
df_seas5_grouped = (
    df_seas5.groupby(["valid_date", "issued_date"])[
        ["val_weighted", weight_col]
    ]
    .sum()
    .reset_index()
)
```

```python
df_seas5_grouped[val_col] = (
    df_seas5_grouped["val_weighted"] / df_seas5_grouped[weight_col]
)
```

```python
df_seas5_grouped = df_seas5_grouped.drop(columns=[weight_col, "val_weighted"])
```

```python
# just double check that weighted mean values look correct
valid_date = "2000-07-01"

fig, ax = plt.subplots()
df_seas5_grouped[df_seas5_grouped["valid_date"] == valid_date].plot(
    x="issued_date", y=val_col, ax=ax
)

df_plot = df_seas5.pivot(
    columns="pcode", index=["valid_date", "issued_date"], values="mean"
).reset_index()

df_plot[df_plot["valid_date"] == valid_date].drop(columns="valid_date").plot(
    x="issued_date", ax=ax
)
```

```python
season_months = [7, 8, 9]
```

```python
df_seas5_season = df_seas5_grouped[
    (df_seas5_grouped["valid_date"] == df_seas5_grouped["issued_date"])
    & (df_seas5_grouped["valid_date"].dt.month.isin(season_months))
].copy()
df_seas5_season["year"] = df_seas5_season["valid_date"].dt.year
df_seas5_season["month"] = df_seas5_season["valid_date"].dt.month
df_seas5_season = df_seas5_season.drop(columns=["valid_date", "issued_date"])
```

```python
df_seas5_season = rp_calc.calculate_groups_rp(
    df_seas5_season, by=["month"], ascending=False
)
```

```python
dicts = []
total_years = df_seas5_season["year"].nunique()
for rp_ind in df_seas5_season["mean_rp"].unique():
    dff = df_seas5_season[df_seas5_season["mean_rp"] >= rp_ind]
    dicts.append(
        {"rp_ind": rp_ind, "rp_com": (total_years + 1) / dff["year"].nunique()}
    )

df_rps = pd.DataFrame(dicts).sort_values("rp_ind", ascending=False)
```

```python
df_rps
```

```python
rp_ind = 3
```

```python
df_seas5_season[df_seas5_season["month"] == 7].sort_values("mean_rank")
```

```python
min_year = 2000

fig, axs = plt.subplots(nrows=3, figsize=(8, 8), sharex=True)

trig_color = "crimson"

dicts = []
for i, (month, group) in enumerate(df_seas5_season.groupby("month")):
    group = group.sort_values("mean_rp").copy()
    thresh = np.interp(rp_ind, group["mean_rp"], group["mean"])

    ax = axs[i]

    df_plot = group[group["year"] >= min_year].sort_values("year")
    colors = ["grey" if v < thresh else trig_color for v in df_plot["mean"]]
    df_plot.plot.bar(
        x="year", y="mean", ax=ax, legend=False, color=colors, alpha=0.6
    )
    ax.set_title(FRENCH_MONTHS[calendar.month_abbr[month]].capitalize())
    ax.axhline(thresh, color=trig_color, linestyle="--", linewidth=1)
    ax.annotate(
        f"   Seuil :\n   {thresh:.2f} mm",
        (len(df_plot) - 1, thresh),
        va="center",
        color=trig_color,
    )
    if i == 1:
        ax.set_ylabel(
            "Précipitations quotidiennes moyennes prévues (mm) [SEAS5]"
        )

    ax.spines.top.set_visible(False)
    ax.spines.right.set_visible(False)

    dicts.append({"month": month, "thresh": thresh})

axs[-1].set_xlabel("Année")
df_threshs = pd.DataFrame(dicts)
```

```python
df_threshs
```

```python

```
