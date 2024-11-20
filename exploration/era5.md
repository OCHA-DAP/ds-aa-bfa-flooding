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

# ERA5

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import era5, cerf, codab
from src.constants import *
```

```python
def calculate_rp(group, col_name):
    group[f"rank_{col_name}"] = group[col_name].rank(ascending=False)
    group[f"rp_{col_name}"] = (len(group) + 1) / group[f"rank_{col_name}"]
    return group
```

```python
months_of_interest = [7, 8, 9]
```

```python
adm1 = codab.load_codab_from_blob(admin_level=1)
adm2 = codab.load_codab_from_blob(admin_level=2)
adm3 = codab.load_codab_from_blob(admin_level=3)
```

```python
cerf_df = cerf.load_cerf()
```

```python
cerf_df
```

```python
era5_adm2_df = era5.load_era5_monthly(adm_level=2)
era5_adm1_df = era5.load_era5_monthly(adm_level=1)

```

```python
era5_adm0_df = era5.load_era5_monthly(adm_level=0)
```

```python
era5_df = era5_adm2_df.copy()
```

```python
era5_adm0_jas_df = era5_adm0_df[
    era5_adm0_df["valid_date"].dt.month.isin(months_of_interest)
]

era5_adm0_jas_df = (
    era5_adm0_jas_df.groupby(
        ["ADM0_PCODE", era5_adm0_jas_df["valid_date"].dt.year.rename("year")]
    )
    .sum(numeric_only=True)
    .reset_index()
)

era5_adm0_jas_df

era5_adm0_jas_df = (
    era5_adm0_jas_df.groupby("ADM0_PCODE")
    .apply(calculate_rp, col_name="mean", include_groups=False)
    .reset_index()
    .drop(columns=["level_1"])
)
era5_adm0_jas_df.sort_values("rank_mean")
```

```python
era5_adm1_jas_df = era5_adm1_df[
    era5_adm1_df["valid_date"].dt.month.isin(months_of_interest)
]
era5_adm1_jas_df = (
    era5_adm1_jas_df.groupby(
        ["ADM1_PCODE", era5_adm1_jas_df["valid_date"].dt.year.rename("year")]
    )
    .sum(numeric_only=True)
    .reset_index()
)
era5_adm1_jas_df = (
    era5_adm1_jas_df.groupby("ADM1_PCODE")
    .apply(calculate_rp, col_name="mean", include_groups=False)
    .reset_index()
    .drop(columns=["level_1"])
)
era5_adm1_jas_df
```

```python
rp_thresh = 3

triggered_df = era5_adm1_jas_df[era5_adm1_jas_df["rp_mean"] > rp_thresh]

unique_years = triggered_df["year"].nunique()
print(f"unique years: {unique_years}")
print(f"effective rp: {(44 + 1) / unique_years}")
print()

for pcode in [CENTRE1, CENTRENORD1, SAHEL1, EST1, NORD1, PLATEAUCENTRAL1]:
    display(triggered_df[triggered_df["ADM1_PCODE"] == pcode])
```

```python
era5_df["valid_date"].dt.year.nunique()
```

```python
era5_df = era5_df.merge(
    adm2[["ADM1_PCODE", "ADM2_PCODE", "ADM1_FR", "ADM2_FR"]]
)
```

```python
era5_adm1_df
```

```python
era5_adm1_rp_df = (
    era5_adm1_df.groupby(
        [era5_adm1_df["valid_date"].dt.month.rename("month"), "ADM1_PCODE"]
    )
    .apply(calculate_rp, col_name="mean", include_groups=False)
    .reset_index()
    .drop(columns=["level_2"])
)
```

```python
era5_adm1_rp_df = era5_adm1_rp_df[
    era5_adm1_rp_df["month"].isin(months_of_interest)
]
```

```python
rp_thresh = 3

triggered_df = (
    era5_adm1_rp_df[era5_adm1_rp_df["rp_mean"] > rp_thresh]
    .merge(adm1[["ADM1_PCODE", "ADM1_FR"]])
    .sort_values(["ADM1_FR", "month", "rank_mean"])
)

unique_years = triggered_df["valid_date"].dt.year.nunique()
print(f"unique years: {unique_years}")
print(f"effective rp: {(44 + 1) / unique_years}")
print()

for pcode in [CENTRE1, CENTRENORD1, SAHEL1, EST1, NORD1, PLATEAUCENTRAL1]:
    dff = triggered_df[triggered_df["ADM1_PCODE"] == pcode]
    unique_years_f = dff["valid_date"].dt.year.nunique()
    print(f"unique years: {unique_years_f}")
    print(f"effective rp: {(44 + 1) / unique_years_f}")
    display(triggered_df[triggered_df["ADM1_PCODE"] == pcode])
```

```python
era5_adm1_rp_df[
    (era5_adm1_rp_df["ADM1_PCODE"] == CENTRE1)
    & (era5_adm1_rp_df["rp_mean"] >= 20)
].sort_values(["month", "rank_mean"])
```

## CERF 2009

Check relative historical rainfall for 2009 CERF allocation.

```python
cerf_dff = cerf_df[cerf_df["year"] == 2009].copy()

adm3_pcode = cerf_dff.iloc[0]["ADM_PCODE"]

adm2_pcode = adm3[adm3["ADM3_PCODE"] == adm3_pcode].iloc[0]["ADM2_PCODE"]

era5_dff = era5_df[
    (era5_df["ADM2_PCODE"] == adm2_pcode)
    & (era5_df["valid_date"].dt.month == cerf_dff.iloc[0]["month"])
    & (era5_df["valid_date"].dt.year >= 0)
].copy()

era5_dff["rank"] = era5_dff["mean"].rank(ascending=False).astype(int)
era5_dff["rp"] = (len(era5_dff) + 1) / era5_dff["rank"]

era5_dff.sort_values("rank")
```

## CERF 2010

```python
cerf_dff = cerf_df[cerf_df["year"] == 2010].copy()

era5_dff = era5_df[
    (era5_df["ADM1_PCODE"].isin(cerf_dff["ADM_PCODE"].unique()))
    & (era5_df["valid_date"].dt.month == cerf_dff.iloc[0]["month"])
    & (era5_df["valid_date"].dt.year >= 0)
].copy()

cols = ["sum", "count"]
era5_dff = era5_dff.groupby("valid_date")[cols].sum().reset_index()
era5_dff["effective_mean"] = era5_dff["sum"] / era5_dff["count"]

era5_dff["rank"] = era5_dff["effective_mean"].rank()
era5_dff["rp"] = (len(era5_dff) + 1) / (len(era5_dff) - era5_dff["rank"] + 1)

era5_dff.sort_values("rp", ascending=False)
```

```python
era5_dff = era5_df[
    (era5_df["ADM1_PCODE"].isin(cerf_dff["ADM_PCODE"].unique()))
    & (era5_df["valid_date"].dt.month == cerf_dff.iloc[0]["month"])
    & (era5_df["valid_date"].dt.year >= 0)
].copy()

cols = ["max"]
era5_dff = era5_dff.groupby("valid_date")[cols].max().reset_index()

era5_dff["rank"] = era5_dff["max"].rank()
era5_dff["rp"] = (len(era5_dff) + 1) / (len(era5_dff) - era5_dff["rank"] + 1)

era5_dff.sort_values("rp", ascending=False)
```

```python

```
