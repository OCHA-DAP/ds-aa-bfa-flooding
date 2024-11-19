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
from src.datasources import seas5
from src.utils import db_utils
```

```python
print(db_utils.get_engine("prod"))
```

```python
df = seas5.load_seas5()
```

```python
df
```

```python
df["leadtime"].unique()
```

```python

```
