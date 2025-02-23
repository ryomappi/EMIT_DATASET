"""
datasetを作成するためのスクリプト

1. L2A, L2Bのデータをgeojsonを用いて検索し, それぞれのURLを取得し, URLをcsvファイルに書き込む
2. URLを用いてストリーミングでデータを処理し, .npyファイルに書き込む
"""

import argparse
import earthaccess
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import sys
from shapely.geometry.polygon import orient
from typing import Tuple

sys.path.append("modules")
from emit_tools import emit_xarray
from tutorial_utils import results_to_geopandas, convert_bounds


def get_asset_url(row, asset, key="Type", value="GET DATA"):
    """
    Retrieve a url from the list of dictionaries for a row in the _related_urls column.
    Asset examples: CH4PLM, CH4PLMMETA, RFL, MASK, RFLUNCERT
    """
    # Add _ to asset so string matching works
    asset = f"_{asset}_"
    # Retrieve URL matching parameters
    for _dict in row[f"_related_urls{asset}"]:
        if _dict.get(key) == value and asset in _dict["URL"].split("/")[-1]:
            return _dict["URL"]


def search_by_geojson(geojson_path, date_range):
    """
    geojson ファイルを用いて, 同じタイムスタンプを持つEMITL2ARFL及びEMITL2BCH4PLMのURLを取得する

    * concept_id について
    - EMITL2ARFL: "C2408750690-LPCLOUD"
    - EMITL2BCH4PLM: "C2748088093-LPCLOUD"
    """
    # geojson ファイルを読み込み、関心領域のポリゴンを取得
    roi_gdf = gpd.read_file(geojson_path)
    roi = orient(roi_gdf.geometry[0], sign=1.0)
    roi = list(roi.exterior.coords)

    # EMITL2ARFL, EMITL2BCH4PLMのURLを取得
    EMITL2ARFL_concept_id = "C2408750690-LPCLOUD"
    EMITL2BCH4PLM_concept_id = "C2748088093-LPCLOUD"
    EMITL2ARFL_results = earthaccess.search_data(
        concept_id=EMITL2ARFL_concept_id, temporal=date_range, polygon=roi, count=200
    )
    EMITL2BCH4PLM_results = earthaccess.search_data(
        concept_id=EMITL2BCH4PLM_concept_id, temporal=date_range, polygon=roi, count=200
    )

    # cloud_cover の情報を追加して geopandas に変換
    EMITL2ARFL_results_gdf = results_to_geopandas(
        EMITL2ARFL_results, fields=["_cloud_cover"]
    )
    EMITL2BCH4PLM_results_gdf = results_to_geopandas(
        EMITL2BCH4PLM_results, fields=["_cloud_cover"]
    )

    # cloud_cover が60未満のデータのみを取得 (雲が多すぎるとデータが使えないため)
    # EMITL2ARFL_results_gdf = EMITL2ARFL_results_gdf[EMITL2ARFL_results_gdf["_cloud_cover"] < 60]
    # EMITL2ARFL_results_gdf = EMITL2ARFL_results_gdf.reset_index(drop=True, inplace=True)
    # EMITL2BCH4PLM_results_gdf = EMITL2BCH4PLM_results_gdf[EMITL2BCH4PLM_results_gdf["_cloud_cover"] < 60]
    # EMITL2BCH4PLM_results_gdf = EMITL2BCH4PLM_results_gdf.reset_index(drop=True, inplace=True)

    # cloud_cover で昇順ソート
    EMITL2ARFL_results_gdf = EMITL2ARFL_results_gdf.sort_values(
        by="_cloud_cover"
    ).reset_index(drop=True)
    EMITL2BCH4PLM_results_gdf = EMITL2BCH4PLM_results_gdf.sort_values(
        by="_cloud_cover"
    ).reset_index(drop=True)

    # 同じタイムスタンプを持つ EMITL2ARFL, EMITL2BCH4PLM のペアの URL を取得
    merged_results_df = pd.merge(
        EMITL2ARFL_results_gdf,
        EMITL2BCH4PLM_results_gdf,
        left_on="_beginning_date_time",
        right_on="_single_date_time",
        suffixes=("_L2A_RFL_", "_L2B_CH4PLM_"),
    )
    url_pairs = []
    for index, row in merged_results_df.iterrows():
        EMITL2ARFL_url = get_asset_url(row, "L2A_RFL")
        EMITL2BCH4PLM_url = get_asset_url(row, "L2B_CH4PLM")
        if EMITL2ARFL_url and EMITL2BCH4PLM_url:
            timestamp = row["_beginning_date_time"]
            url_pairs.append((timestamp, EMITL2ARFL_url, EMITL2BCH4PLM_url))
        else:
            print(
                f"ペア取得に失敗しました ({row['native-id_L2A_RFL_']} と {row['native-id_L2BCH4PLM_']}) "
            )
    if not url_pairs:
        print(
            "同じタイムスタンプを持つ L2ARFL と L2BCH4PLM のペアが見つかりませんでした."
        )
    else:
        geojson_id = geojson_path.stem
        print(f"{geojson_id}.json\t: {len(url_pairs)} 件のペアが見つかりました.")
    return url_pairs


def ortho_file_pair(geojson_id, l2a_fp, l2b_fp, l2a_outdir, l2b_outdir):
    # .npy ファイルの出力先パス
    l2a_dst = l2a_outdir / f"{geojson_id}.npy"
    l2b_dst = l2b_outdir / f"{geojson_id}.npy"

    # 既に出力ファイルが存在する場合はスキップ
    if l2a_dst.exists() and l2b_dst.exists():
        print(
            f"ファイル {l2a_dst} および {l2b_dst} は既に存在しています。スキップします。"
        )
        return

    print(f"以下のファイルを処理します: \nL2A: {l2a_fp}\nL2B: {l2b_fp}")

    try:
        # L2Aデータのオルソ処理
        l2a_geo = emit_xarray(l2a_fp, ortho=True)
        l2a_geo.reflectance.data[l2a_geo.reflectance.data == -9999] = 0  # 欠損値を0に

        # L2Bデータのオルソ処理
        with rasterio.open(l2b_fp) as src:
            # オルソ補正パラメータの計算
            transform, width, height = calculate_default_transform(
                src.crs, src.crs, src.width, src.height, *src.bounds
            )
            # 出力用配列の作成
            l2b_geo = np.empty((src.count, height, width), dtype=src.dtypes[0])
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=l2b_geo[i - 1],
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=src.crs,
                    resampling=Resampling.nearest,
                )
            l2b_geo = l2b_geo.squeeze()
            # L2Bのバウンディングボックスを取得
            bbox = src.bounds
            print(f"bbox: {bbox}")

        # L2Aデータを L2B のバウンディングボックスでくり抜く
        l2a_cropped = l2a_geo.reflectance.sel(
            longitude=slice(bbox.left, bbox.right),
            latitude=slice(bbox.top, bbox.bottom),
        )

        # データを保存
        np.save(l2a_dst, l2a_cropped.data)
        np.save(l2b_dst, l2b_geo)
        print(f"保存完了:   L2A -> {l2a_dst},  L2B -> {l2b_dst}")
    except Exception as e:
        print(
            f"{geojson_id}.json のペアの処理でエラーが発生しました。エラー内容: {e}. このペアはスキップします。"
        )
        # 途中で生成されたファイルがあれば削除
        if l2a_dst.exists():
            l2a_dst.unlink()
        if l2b_dst.exists():
            l2b_dst.unlink()
        return


def main():
    parser = argparse.ArgumentParser(description="Make dataset from geojson files.")
    parser.add_argument(
        "--date_range",
        type=Tuple[str, str],
        default=("2023-01-01", "2024-12-31"),
        help="Date range for search (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    # .env ファイルから Earthdata Login 情報を取得してログイン
    load_dotenv()
    auth = earthaccess.login(strategy="environment", persist=True)
    if not auth:
        print("Earthdata Login に失敗しました.")
        sys.exit(1)

    # HTTPS セッションを取得
    fs = earthaccess.get_fsspec_https_session()

    # data/dataset/geojsons にある geojson ファイルを使用してEMITL2ARFL, EMITL2BCH4PLM の URL を取得し1組ずつ csv に書き込む
    geojson_dir = Path("data/dataset/geojsons")
    geojson_paths = sorted(
        geojson_dir.glob("*.json"), key=lambda geojson_path: int(geojson_path.stem)
    )
    if not geojson_paths:
        print(f"No GeoJSON files found in {geojson_dir}")
        sys.exit(1)

    # dataset.csv に書き込む
    dataset_csv_path = Path("data/dataset/dataset.csv")
    if not dataset_csv_path.exists():
        with open(dataset_csv_path, "w") as f:
            f.write("geojson_id,timestamp,EMITL2ARFL_url,EMITL2BCH4PLM_url\n")
    for geojson_path in geojson_paths:
        url_pairs = search_by_geojson(geojson_path, args.date_range)
        url_pair = url_pairs[0]  # 一番目のペアのみを使用
        with open(dataset_csv_path, "a") as f:
            f.write(f"{geojson_path.stem},{url_pair[0]},{url_pair[1]},{url_pair[2]}\n")

        # HTTPS ストリームを開いてデータを取得
        EMITL2ARFL_fp = fs.open(url_pair[1])
        EMITL2BCH4PLM_fp = fs.open(url_pair[2])

        # .npy ファイルの出力先ディレクトリを作成
        EMITL2ARFL_outdir = Path("data/dataset/EMITL2ARFL")
        EMITL2BCH4PLM_outdir = Path("data/dataset/EMITL2BCH4PLM")
        EMITL2ARFL_outdir.mkdir(parents=True, exist_ok=True)
        EMITL2BCH4PLM_outdir.mkdir(parents=True, exist_ok=True)

        # データを処理して .npy ファイルに書き込む
        ortho_file_pair(
            geojson_path.stem,
            EMITL2ARFL_fp,
            EMITL2BCH4PLM_fp,
            EMITL2ARFL_outdir,
            EMITL2BCH4PLM_outdir,
        )


if __name__ == "__main__":
    main()
