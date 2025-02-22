# EMIT DATASET

EMITのL2ARFLとL2BCH4PLMの2種類のデータを用いて、EMITのデータセットを作成します。

## Python環境構築

### mambaを用いて環境を構築する場合

These Python Environments will work for all of the guides, how-to's, and tutorials within this repository, and the [VITALS repository](https://github.com/nasa/VITALS).

1. Using your preferred command line interface (command prompt, terminal, cmder, etc.) navigate to your local copy of the repository, then type the following to create a compatible Python environment.

    For Windows:

    ```cmd
    mamba create -n lpdaac_vitals -c conda-forge --yes python=3.10 fiona=1.8.22 gdal hvplot geoviews rioxarray rasterio jupyter geopandas earthaccess jupyter_bokeh h5py h5netcdf spectral scikit-image jupyterlab seaborn dask ray-default
    ```

    For MacOSX*:

    ```cmd
    mamba create -n lpdaac_vitals -c conda-forge --yes python=3.10 gdal=3.7.2 hvplot geoviews rioxarray rasterio geopandas fiona=1.9.4 jupyter earthaccess jupyter_bokeh h5py h5netcdf spectral scikit-image seaborn jupyterlab dask
    ```

    >***MacOSX users will need to install "ray[default]" separately using pip after creating and activating the environment.**

2. Next, activate the Python Environment that you just created.

    ```cmd
    mamba activate lpdaac_vitals
    ```

    **After activating the environment if using MacOSX, install the "ray[default]" package using pip:**

    ```cmd
    pip install ray[default]
    ```

### Dockerを用いて環境を構築する場合

まだ未検証

## データセット作成手順

1. geojsonファイルをダウンロード

以下を実行して、geojsonファイルをダウンロードします。

```sh
python src/download_geojson.py --max_downloads [max_downloads]
```

`max_downloads`にはダウンロードするgeojsonファイルの数を指定します。デフォルトは0です。

2. geojsonからデータセットを作成

以下を実行して、geojsonファイルからデータセットを作成します。

```sh
python src/make_dataset.py
```

バックグラウンドで実行する場合は以下を実行します。

```sh
./make_dataset_bg.sh
```
