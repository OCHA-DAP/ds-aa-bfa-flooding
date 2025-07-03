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

# SEAS5

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.datasources import seas5, cerf, emdat
from src.utils import db_utils
```

```python
def calculate_rp(group, col_names, ascending: bool = False):
    for col_name in col_names:
        group[f"rank_{col_name}"] = group[col_name].rank(ascending=False)
        group[f"rp_{col_name}"] = (len(group) + 1) / group[f"rank_{col_name}"]
    return group
```

```python
df_emdat = emdat.load_emdat()
```

```python
df_emdat_year = (
    df_emdat.groupby("Start Year")["Total Affected"].sum().reset_index()
)
full_year_range = range(2000, df_emdat_year["Start Year"].max() + 1)
df_emdat_year = (
    df_emdat_year.set_index("Start Year")
    .reindex(full_year_range)
    .reset_index()
    .fillna(0)
    .astype(int)
    .rename(columns={"Start Year": "year", "Total Affected": "total_affected"})
)
```

```python
df_emdat_year
```

```python
df_emdat_year = calculate_rp(df_emdat_year, col_name="total_affected")
```

```python
df_emdat_year["TP_3yr"] = df_emdat_year["rp_total_affected"] > 3
df_emdat_year["TP_5yr"] = df_emdat_year["rp_total_affected"] > 5
df_emdat_year
```

```python
df_cerf = cerf.load_cerf()
```

```python
df_cerf
```

```python
df_seas5 = seas5.load_seas5()
```

```python
df_seas5
```

```python
adm_level = 0
```

```python
df_seas5_adm0 = (
    df_seas5[df_seas5["adm_level"] == 0]
    .groupby(
        [
            df_seas5["valid_date"].dt.month.rename("valid_month"),
            df_seas5["issued_date"].dt.month.rename("issued_month"),
        ]
    )
    .apply(calculate_rp, col_names=["mean", "max"])
    .reset_index()
    .drop(columns="level_2")
)
```

```python
df_compare = df_emdat_year.copy()
df_compare["cerf"] = df_compare["year"].isin(df_cerf["year"].unique())
```

```python
def display_metrics_df(df, bar_col: str = "total_affected"):
    def highlight_true(val, color: str = "crimson"):
        if isinstance(val, bool) and val is True:
            return f"background-color: {color}"
        return ""

    cols = [x for x in df.columns if x != bar_col] + [bar_col]
    df = df[cols].sort_values(bar_col, ascending=False)
    display(
        df[cols]
        .style.bar(
            subset=bar_col,
            color="coral",
            props="width: 300px;",
        )
        .map(highlight_true)
        .map(highlight_true, color="dodgerblue", subset="cerf")
        .set_table_styles(
            {
                bar_col: [
                    {"selector": "th", "props": [("text-align", "left")]},
                    {"selector": "td", "props": [("text-align", "left")]},
                ]
            }
        )
        .format({bar_col: "{:,}"})
    )
```

```python
def get_thresh_name(
    lt: int = 0, period: str = "mo", val_col: str = "mean", rp: int = 3
):
    return f"lt{lt}_{period}_{val_col}_{rp}rp"
```

```python
months = [6, 7, 8, 9]
n_years_total = df_compare["year"].nunique()

rp_threshs = [3, 5, 8]
lt = 0

for val_col in ["mean", "max"]:
    for rp_thresh in rp_threshs:
        trigger_name = get_thresh_name(
            lt=lt, period="mo", val_col=val_col, rp=rp_thresh
        )
        df_triggers = df_seas5_adm0[
            (df_seas5_adm0[f"rp_{val_col}"] > rp_thresh)
            & (df_seas5_adm0["issued_month"].isin(months))
            & (df_seas5_adm0["leadtime"] == 0)
        ]
        years_triggered = df_triggers["issued_date"].dt.year.unique()
        df_compare[trigger_name] = df_compare["year"].isin(years_triggered)
```

```python
cols = [x for x in df_compare.columns if "8rp" in x] + [
    "total_affected",
    "cerf",
]
display_metrics_df(df_compare.set_index("year")[cols])
```

```python
n_years_total
```

```python
tp_col = "TP_3yr"

dicts = []
for val_col in ["mean", "max"]:
    for rp in rp_threshs:
        trigger_name = get_thresh_name(rp=rp, val_col=val_col)
        dicts.append(
            {
                "val_col": val_col,
                "rp_trig": rp,
                "rp_eff": n_years_total / df_compare[trigger_name].sum(),
                "TPR": df_compare[[tp_col, trigger_name]].all(axis=1).sum()
                / df_compare[tp_col].sum(),
                "PPV": df_compare[[tp_col, trigger_name]].all(axis=1).sum()
                / df_compare[trigger_name].sum(),
            }
        )

df_metrics = pd.DataFrame(dicts)
```

```python
df_metrics
```

```python

```
