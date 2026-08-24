# data-service

Copernicus quant system's data service | 哥白尼量化系统数据服务

Development notes:

Cloudflare R2 Structure:

```
copernicus
  meta - stores metadata, including stock list, store state
  stock - stock parquet data
  index - index parquet data
```
