# data-service

Copernicus quant system's data service | Copernicus量化系统数据服务

Development notes:

Cloudflare R2 Structure:

```
copernicus
  meta - stores metadata, including stock list, store state
  stock - stock parquet data
  index - index parquet data
```

```
uv sync
uv run uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```
