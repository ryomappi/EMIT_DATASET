FROM python:3.10-slim

# 必要なツールを一括でインストール
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      gdal-bin \
      libgdal-dev && \
    rm -rf /var/lib/apt/lists/*

# GDAL のヘッダーのパスを設定
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# 必要なPythonパッケージをインストール
RUN pip install --no-cache-dir \
    earthaccess \
    pandas \
    geopandas \
    xarray \
    netCDF4 \
    spectral \
    GDAL==3.6.2 \
    scikit-image \
    rasterio \
    rioxarray \
    imagecodecs

# コンテナの作業ディレクトリを設定
WORKDIR /workspace

CMD []
