# 一键试用里的外部查询引擎：DuckDB 1.5.6 的 Python 包，预装读 Iceberg 表要用的三个扩展（httpfs、avro、iceberg），
# 运行时不再联网下载。查询入口是 lake-query（同目录的 lake-query.py）。
# External query engine of the quickstart: the DuckDB 1.5.6 Python package, with the three extensions needed to read
# Iceberg tables (httpfs, avro, iceberg) preinstalled, so nothing is downloaded at run time. Queries go through
# lake-query (lake-query.py in this directory).
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
RUN pip install --no-cache-dir duckdb==1.5.6 \
 && useradd --create-home --uid 10001 duck
COPY --chmod=755 lake-query.py /usr/local/bin/lake-query
USER duck
RUN python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('httpfs', 'avro', 'iceberg')]"
