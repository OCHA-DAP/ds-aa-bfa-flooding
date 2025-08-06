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

# River level
<!-- markdownlint-disable MD013 -->
Koriziena (Korizéna, Gorom-Gorom, Oudalan, Sahel) river level from DGRE (seems to be only one with reasonably complete data)

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from io import BytesIO

import ocha_stratus as stratus
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from src.constants import *
from src.utils.rp_calc import calculate_one_group_rp
```

```python
# load from blob
blob_name = f"{PROJECT_PREFIX}/raw/dgre/Donnees/KORIZIENA_Sahel.xls"
data = stratus.load_blob_data(blob_name)
xls = pd.ExcelFile(BytesIO(data))
```

```python
# load locally
filepath = "temp/KORIZIENA_Sahel.xls"
xls = pd.ExcelFile(filepath)
```

```python
# Initialize empty list for storing data
dfs = []

# Loop over all sheets (each named by year)
for sheet_name in xls.sheet_names:
    try:
        year = int(sheet_name)
    except ValueError:
        continue  # Skip sheets not named by year

    # Read the sheet, skip metadata rows, keep only daily values
    df = pd.read_excel(
        xls,
        sheet_name=sheet_name,
        skiprows=7,
        nrows=31,
        usecols="B:M",
        header=None,
    )

    df_long = df.melt(ignore_index=False, var_name="month", value_name="value")
    df_long = df_long.reset_index(names="day")
    df_long["day"] += 1
    df_long["year"] = year

    dfs.append(df_long)

    if year == 2023:
        display(df_long.iloc[:60])
        break
```

```python
df_all = pd.concat(dfs, ignore_index=True)
```

```python
df_all["date"] = pd.to_datetime(
    df_all[["year", "month", "day"]], errors="coerce"
)

# Optionally drop rows with invalid dates (e.g., Feb 30)
df_all = df_all.dropna(subset=["date"])[["date", "value"]]
```

```python
df_all = df_all.sort_values("date")
```

```python
df_all.groupby(df_all["date"].dt.year).count()["value"].plot(
    kind="bar", figsize=(20, 7)
)
```

```python
df_all.groupby(df_all["date"].dt.year)["value"].max().plot()
```

```python
min_year, max_year = 1980, 2021
```

```python
df_complete = df_all[
    df_all["date"].dt.year.isin(range(min_year, max_year + 1))
]
```

```python
df_complete
```

```python
df_yearly = df_complete.loc[
    df_complete.groupby(df_complete["date"].dt.year)["value"].idxmax()
]
```

```python
df_yearly["year"] = df_yearly["date"].dt.year
```

```python
df_yearly
```

```python
df_yearly = calculate_one_group_rp(
    df_yearly, col_name="value", ascending=False
)
```

```python
df_yearly.sort_values("value_rank")
```

```python
df_yearly = df_yearly.sort_values("value_rank", ascending=False)

rps = {}
for rp in [2, 3, 5, 10]:
    thresh = np.interp(rp, df_yearly["value_rp"], df_yearly["value"])
    rps.update({rp: thresh})
```

```python
def get_rv(rp):
    df_interp = df_yearly.sort_values("value_rank", ascending=False)
    return np.interp(rp, df_interp["value_rp"], df_interp["value"])
```

```python
get_rv(10)
```

```python
rps
```

```python
rp_colors = [
    (2, "gold"),
    (3, "darkorange"),
    (4, "crimson"),
    (5, "rebeccapurple"),
]
```

```python
fig, ax = plt.subplots(dpi=150)

df_yearly.sort_values("value_rank").plot(
    x="value_rp", y="value", ax=ax, legend=False, color="dodgerblue"
)

for rp, color in rp_colors:
    rv = get_rv(rp)
    ax.plot(
        [rp, rp], [0, rv], linewidth=1, linestyle="--", color="k", alpha=0.2
    )
    ax.annotate(
        f"{rv:.0f} m$^3$/s",
        (rp, rv),
        color=color,
        fontsize=8,
        ha="left",
        va="top",
    )
    ax.plot([rp], [rv], marker=".", color=color)

top_rp = 10
ax.set_xlim((1, top_rp))
ax.set_ylim((df_yearly["value"].min(), get_rv(top_rp)))

ax.set_ylabel("Débit moyenne journalière,\nmaximum par année (m$^3$/s)")
ax.set_xlabel("Période de retour (ans)")
ax.set_title(
    "Station Koriziena\nPériode de retour (années d'analyse : 1980-2021)"
)

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
```

```python
rp_colors
```

```python
def add_rp_color(rp_q):
    color_out = "grey"
    for rp, color in rp_colors:
        if rp_q >= rp:
            color_out = color
        else:
            return color_out
    return color_out
```

```python
add_rp_color(4.9)
```

```python
fig, ax = plt.subplots(dpi=150, figsize=(12, 7))

df_plot = df_yearly.sort_values("year")
df_plot["color"] = df_plot["value_rp"].apply(add_rp_color)

xmin, xmax = df_plot["year"].min() - 1, df_plot["year"].max() + 1

ax.bar(df_plot["year"], df_plot["value"], color=df_plot["color"], alpha=0.7)

for rp, color in rp_colors:
    rv = get_rv(rp)
    ax.plot(
        [xmin, xmax],
        [rv, rv],
        linewidth=1,
        linestyle="--",
        color=color,
        alpha=1,
    )
    ax.annotate(
        f" {rp}-ans : {rv:.0f} m$^3$/s",
        (xmax, rv),
        color=color,
        fontsize=8,
        ha="left",
        va="center",
    )
    ax.plot([rp], [rv], marker=".", color=color)

ax.annotate(
    f" Seuils\n",
    (xmax, rv),
    fontsize=8,
    ha="left",
    va="bottom",
    fontstyle="italic",
)


ax.set_xticks(df_plot["year"])
ax.set_xticklabels(df_plot["year"], rotation=90)

ax.set_xlim((xmin, xmax))

ax.set_xlabel("Année")
ax.set_ylabel("Débit moyenne journalière, maximum par année (m$^3$/s)")
ax.set_title("Station Koriziena\nAnnées dépassant seuils")

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
```

```python
df_yearly["date_1900"] = df_yearly["date"].apply(
    lambda d: pd.Timestamp(year=1900, month=d.month, day=d.day)
)
```

```python
df_yearly.dtypes
```

```python

```

```python
df_yearly.set_index("year")["value"].plot()
```

```python
df_yearly.sort_values("date_1900")
```

```python
df_yearly.set_index("year")["date_1900"].plot()
```

```python

```
