import argparse
from dotenv import load_dotenv
import earthaccess
import earthaccess
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import sys
from pathlib import Path

sys.path.append("../modules/")
from emit_tools import emit_xarray


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--l2a",
        type=str,
        default="data/registration_test/EMIT_L2A_RFL_001_20241020T170504_2429411_003.nc",
        help="L2A data path",
    )
    parser.add_argument(
        "--l2b",
        type=str,
        default="data/registration_test/EMIT_L2B_CH4ENH_001_20241020T170504_2429411_003.tif",
        help="L2B data path",
    )
    parser.add_argument("--dst", type=str, help="Output path")
    args = parser.parse_args()

    l2a_path, l2b_path = args.l2a, args.l2b

    # earthdataにログイン
    load_dotenv()
    earthaccess.login(strategy="environment", persist=True)

    # L2Aデータのオルソ化
    l2a_geo = emit_xarray(l2a_path, ortho=True)
    l2a_geo.reflectance.data[l2a_geo.reflectance.data == -9999] = 0  # 欠損値処理
    # l2a_geo['reflectance'].data[:,:,l2a_geo['good_wavelengths'].data==0] = np.nan  # water absorption bandsを除外

    # L2Bデータのオルソ化
    with rasterio.open(l2b_path) as src:
        # オルソ補正のための変換パラメータを計算する
        transform, width, height = calculate_default_transform(
            src.crs, src.crs, src.width, src.height, *src.bounds
        )

        # オルソ補正されたデータを格納するためのメタデータを更新
        kwargs = src.meta.copy()
        kwargs.update(
            {"crs": src.crs, "transform": transform, "width": width, "height": height}
        )

        # オルソ補正してデータを保存
        l2b_ortho = np.empty((src.count, height, width), dtype=src.dtypes[0])
        for i in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, i),
                destination=l2b_ortho[i - 1],
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=src.crs,
                resampling=Resampling.nearest,  # 最近傍補間
            )
        l2b_ortho = l2b_ortho.squeeze()
        # l2b_orthoの欠損値処理
        # l2b_ortho[l2b_ortho == -9999] = np.nan
        l2b_ortho[l2b_ortho == -9999] = 0

        # L2Bバウンディングボックスを取得
        bbox = src.bounds
        print(bbox)

    # L2AデータをL2Bのバウンディングボックスでくり抜く
    l2a_cropped = l2a_geo.reflectance.sel(
        longitude=slice(bbox.left, bbox.right), latitude=slice(bbox.top, bbox.bottom)
    )

    # 再投影されたL2AデータとL2Bデータを保存
    outdir = Path(args.dst)
    l2a_filename = Path(args.l2a).name
    l2a_name = l2a_filename.split("_", 1)[0] + "_" + l2a_filename.split("_", 1)[1]
    l2a_dst = outdir / l2a_name / ".npy"
    l2b_filename = Path(args.l2b).name
    l2b_name = l2b_filename.split("_", 1)[0] + "_" + l2b_filename.split("_", 1)[1]
    l2b_dst = outdir / l2b_name / ".npy"

    np.save(l2a_dst, l2a_cropped.data)
    np.save(l2b_dst, l2b_ortho)


if __name__ == "__main__":
    main()
