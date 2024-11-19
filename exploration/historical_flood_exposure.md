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

# Historical flood exposure

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
from tqdm.auto import tqdm

from src.datasources import codab, floodscan, worldpop
from src.utils import blob_utils
```

```python
pop = worldpop.load_worldpop_from_blob()
```

```python
pop = pop.rename({"x": "lon", "y": "lat"})
```

```python
pop.plot()
```

```python
int(pop.sum())
```

```python
dicts = []
for pcode, row in tqdm(adm.set_index("ADM2_PCODE").iterrows(), total=len(adm)):
    dicts.append(
        {
            "total_pop": int(pop.rio.clip([row.geometry]).sum()),
            "ADM2_PCODE": pcode,
        }
    )
```

```python
df_pop = pd.DataFrame(dicts)
```

```python
df_pop
```

```python
codab.download_codab_to_blob()
```

```python
adm = codab.load_codab_from_blob()
```

```python
adm.plot()
```

```python
df = floodscan.load_tabular_flood_exposure()
```

```python
df
```

```python
da = floodscan.open_historical_floodscan()
```

```python
da
```

```python
da_clip = da.rio.clip(adm.geometry, all_touched=True)
```

```python
da_clip_filtered = da_clip.where(da_clip >= 0.05)
```

```python
da_year = da_clip_filtered.groupby("time.year").max()
```

```python
da_year.isel(year=0).plot()
```

```python
exposure = da_year.interp_like(pop, method="nearest") * pop
```

```python
da_year
```

```python
exposure.isel(year=0).plot()
```

```python
exposure = exposure.rio.set_spatial_dims(y_dim="lat", x_dim="lon")
```

```python
exposure = exposure.persist()
```

```python
exposure = exposure.rio.set_spatial_dims(y_dim="lat", x_dim="lon")
```

```python
exposure
```

```python
dfs = []
for pcode, row in tqdm(adm.set_index("ADM2_PCODE").iterrows(), total=len(adm)):
    da_adm = exposure.rio.clip([row.geometry])
    df_in = (
        da_adm.sum(dim=["lat", "lon"])
        .to_dataframe("max_exposure")["max_exposure"]
        .astype(int)
        .reset_index()
    )
    df_in["ADM2_PCODE"] = pcode
    dfs.append(df_in)
```

```python
df_year = pd.concat(dfs, ignore_index=True)
```

```python
df_year
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/bfa_yearly_max_flood_exposure.parquet"
)
blob.upload_parquet_to_blob(blob_name, df_year)
```

```python
df_year = df_year.merge(df_pop)
```

```python
df_year["max_frac_exposure"] = df_year["max_exposure"] / df_year["total_pop"]
```

```python
df_year_mean = (
    df_year.groupby("ADM2_PCODE")
    .mean()[["max_exposure", "max_frac_exposure"]]
    .reset_index()
)
```

```python
def thousands_formatter(x, pos):
    return "{:,.0f}".format(x)
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(10, 5))
adm.merge(df_year_mean).plot(
    column="max_exposure", legend=True, ax=ax, cmap="Purples"
)
adm.boundary.plot(linewidth=0.1, color="k", ax=ax)
cbar = ax.get_figure().get_axes()[1]
cbar.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
ax.axis("off")
ax.set_title("Average population exposed to flooding each year\nby province")
```

```python
df_plot = df_year_mean.merge(adm[["ADM2_PCODE", "ADM2_FR", "ADM1_FR"]])[
    ["ADM1_FR", "ADM2_FR", "max_exposure", "max_frac_exposure"]
]
df_plot["max_exposure"] = df_plot["max_exposure"].astype(int)
df_plot.sort_values("max_exposure", ascending=False).iloc[:20].rename(
    columns={
        "max_exposure": "Avg. pop. exposed",
        "max_frac_exposure": "Avg. frac. pop. exp.",
        "ADM1_FR": "Région",
        "ADM2_FR": "Province",
    }
).style.background_gradient(
    cmap="Purples",
).format(
    "{:,.0f}", subset=["Avg. pop. exposed"]
).format(
    "{:.2f}", subset=["Avg. frac. pop. exp."]
)
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(10, 5))
adm.merge(df_year_mean).plot(
    column="max_frac_exposure", legend=True, ax=ax, cmap="Purples"
)
ax.axis("off")
adm.boundary.plot(linewidth=0.1, color="k", ax=ax)
cbar = ax.get_figure().get_axes()[1]
# cbar.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
ax.set_title(
    "Average fraction of population exposed to flooding each year\nby province"
)
```

```python
df_plot.sort_values("max_frac_exposure", ascending=False).iloc[:20].rename(
    columns={
        "max_exposure": "Avg. pop. exposed",
        "max_frac_exposure": "Avg. frac. pop. exp.",
        "ADM1_FR": "Région",
        "ADM2_FR": "Province",
    }
).style.background_gradient(
    cmap="Purples",
).format(
    "{:,.0f}", subset=["Avg. pop. exposed"]
).format(
    "{:.2f}", subset=["Avg. frac. pop. exp."]
)
```

```python
exposure_mean = exposure.mean(dim="year")
```

```python
exposure_mean
```

```python
da_year
```

```python
da_mean = da_year.mean(dim="year").compute()
```

```python
fig, ax = plt.subplots(dpi=300)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
ax.axis("off")
da_mean.plot(ax=ax, cmap="Blues")
```

```python
fig, ax = plt.subplots(dpi=300)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
ax.axis("off")
pop.plot(ax=ax, cmap="Greys", vmax=800)
```

```python
fig, ax = plt.subplots(dpi=300)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
ax.axis("off")
exposure_mean.plot(ax=ax, cmap="Purples", vmax=100)
```
