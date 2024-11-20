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

# CERF allocations

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.utils import blob_utils
from src.datasources import cerf
from src.constants import *
```

```python
df = pd.DataFrame(
    columns=["year", "month", "ADM_PCODE", "admin_level"],
    data=[
        [2009, 9, OUAGADOUGOU3, 3],
        *[
            [2010, 7, x, 1]
            for x in [EST1, SAHEL1, NORD1, CENTRENORD1, PLATEAUCENTRAL1]
        ],
    ],
)
df
```

```python
blob_utils.upload_csv_to_blob(cerf.get_blob_name(), df)
```

```python

```
