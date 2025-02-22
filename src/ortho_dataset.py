import argparse
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import sys
from pathlib import Path
import concurrent.futures

sys.path.append("python/modules/")
from emit_tools import emit_xarray

MAX_WORKERS = 8


def ortho_file_pair(geojson_id, l2a_file, l2b_file, l2a_outdir, l2b_outdir):
    # 出力ファイル名を生成
    l2a_dst = l2a_outdir / f"{geojson_id}.npy"
    l2b_dst = l2b_outdir / f"{geojson_id}.npy"

    if l2a_dst.exists() and l2b_dst.exists():
        print(
            f"\nファイル {l2a_dst} および {l2b_dst} は既に存在しています。スキップします。"
        )
        return

    print(f"\nProcessing file pair:\n  L2A: {l2a_file}\n  L2B: {l2b_file}")

    try:
        # L2Aデータのオルソ処理
        l2a_geo = emit_xarray(str(l2a_file), ortho=True)
        l2a_geo.reflectance.data[l2a_geo.reflectance.data == -9999] = 0  # 欠損値を0に

        # L2Bデータのオルソ処理
        with rasterio.open(str(l2b_file)) as src:
            # オルソ補正パラメータの計算
            transform, width, height = calculate_default_transform(
                src.crs, src.crs, src.width, src.height, *src.bounds
            )
            # 出力用配列の作成
            l2b_ortho = np.empty((src.count, height, width), dtype=src.dtypes[0])
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=l2b_ortho[i - 1],
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=src.crs,
                    resampling=Resampling.nearest,
                )
            l2b_ortho = l2b_ortho.squeeze()
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
        np.save(l2b_dst, l2b_ortho)
        print(f"保存完了: \n  L2A -> {l2a_dst}\n  L2B -> {l2b_dst}")
    except Exception as e:
        print(
            f"{geojson_id} の処理でエラーが発生しました。エラー内容: {e}. このペアはスキップします。"
        )
        # 途中で生成されたファイルがあれば削除
        if l2a_dst.exists():
            l2a_dst.unlink()
        if l2b_dst.exists():
            l2b_dst.unlink()
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json", type=str, default="data/dataset/geojsons", help="GeoJSON data dir"
    )
    # L2A, L2B データのディレクトリを指定（ディレクトリ内のファイルペアを使用）
    parser.add_argument(
        "--l2a_dir",
        type=str,
        default="data/dataset/l2a",
        help="L2A data directory (e.g. containing .nc files)",
    )
    parser.add_argument(
        "--l2b_dir",
        type=str,
        default="data/dataset/l2b",
        help="L2B data directory (e.g. containing .tif files)",
    )
    parser.add_argument(
        "--dataset",
        "-d",
        type=str,
        default="data/dataset",
        help="Output directory",
    )
    args = parser.parse_args()

    l2a_dir = Path(args.l2a_dir)
    l2b_dir = Path(args.l2b_dir)
    outdir = Path(args.dataset)
    l2a_outdir = outdir / "train"
    l2b_outdir = outdir / "gt"
    l2a_outdir.mkdir(parents=True, exist_ok=True)
    l2b_outdir.mkdir(parents=True, exist_ok=True)

    # L2A, L2Bのファイルを対応づける
    file_pairs = {}  # {geojson_id: (l2a_file, l2b_file)}
    # geojson_idを全て取得して、file_pairsのキーとする
    for geojson_path in Path(args.json).glob("*.json"):
        geojson_id = geojson_path.stem
        file_pairs[geojson_id] = (None, None)
    # L2A, L2Bのファイルを対応づける
    for l2a_file in l2a_dir.glob("*.nc"):
        geojson_id = l2a_file.stem.split("_", 1)[0]
        if geojson_id in file_pairs:
            file_pairs[geojson_id] = (l2a_file, None)
    for l2b_file in l2b_dir.glob("*.tif"):
        geojson_id = l2b_file.stem.split("_", 1)[0]
        if geojson_id in file_pairs:
            l2a_file, _ = file_pairs[geojson_id]
            file_pairs[geojson_id] = (l2a_file, l2b_file)

    # 有効なペアのみを抽出
    valid_pairs = {
        geojson_id: (l2a_file, l2b_file)
        for geojson_id, (l2a_file, l2b_file) in file_pairs.items()
        if l2a_file is not None and l2b_file is not None
    }

    if not valid_pairs:
        print("有効なファイルペアが見つかりませんでした")
        sys.exit(1)

    # プロセスプールを使って並列処理
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        # L2A, L2Bのファイルペアごとに処理
        for geojson_id, (l2a_file, l2b_file) in valid_pairs.items():
            futures.append(
                executor.submit(
                    ortho_file_pair,
                    geojson_id,
                    l2a_file,
                    l2b_file,
                    l2a_outdir,
                    l2b_outdir,
                )
            )
        # 全タスクの完了を待機
        for future in concurrent.futures.as_completed(futures):
            future.result()
    print("\n全ての処理が完了しました。")


if __name__ == "__main__":
    main()
