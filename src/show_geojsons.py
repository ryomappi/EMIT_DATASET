import json
import folium
import argparse
from pathlib import Path
from shapely.geometry import shape, box


def create_bbox_feature(file, bbox):
    """
    bbox (minx, miny, maxx, maxy) からポリゴンを生成し、
    GeoJSON の Feature として返す。
    """
    # shapely の box を利用してポリゴンを作成
    poly = box(*bbox)
    feature = {
        "type": "Feature",
        "geometry": poly.__geo_interface__,
        "properties": {"name": file.name, "bbox": bbox},
    }
    return feature


def main():
    parser = argparse.ArgumentParser(
        description="GeoJSONファイルのbboxを地図上に表示する"
    )
    parser.add_argument(
        "--geojson_dir",
        type=str,
        default="data/dataset/geojsons",
        help="GeoJSONファイルが保存されているディレクトリ",
    )
    args = parser.parse_args()
    geojson_dir = Path(args.geojson_dir)
    if not geojson_dir.exists():
        print(f"ディレクトリが存在しません: {geojson_dir}")
        return

    # 地図の作成
    m = folium.Map(location=[0, 0], zoom_start=2)

    features = []  # 各ファイルのbboxを格納するリスト

    for file in geojson_dir.glob("*.json"):
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            features_list = data.get("features", [])
            if not features_list:
                continue

            # 各ファイル内のフィーチャの bbox を個別に計算し、全体の bbox を求める
            file_bounds = []
            for feature in features_list:
                geom = feature.get("geometry")
                if not geom:
                    continue
                shp = shape(geom)
                file_bounds.append(shp.bounds)
            if file_bounds:
                minx = min(bounds[0] for bounds in file_bounds)
                miny = min(bounds[1] for bounds in file_bounds)
                maxx = max(bounds[2] for bounds in file_bounds)
                maxy = max(bounds[3] for bounds in file_bounds)
                bbox = (minx, miny, maxx, maxy)
                feat = create_bbox_feature(file, bbox)
                features.append(feat)
        except Exception as e:
            print(f"{file} の読み込みに失敗: {e}")

    # 作成した bbox を FeatureCollection としてまとめる
    feature_collection = {"type": "FeatureCollection", "features": features}

    # style_function により bbox の表示スタイルを定義
    def style_function(feature):
        return {
            "color": "blue",
            "fillColor": "blue",
            "fillOpacity": 0.2,
            "weight": 2,
        }

    folium.GeoJson(
        feature_collection,
        name="GeoJSON Bounding Boxes",
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(
            fields=["name", "bbox"], aliases=["ファイル名", "BBox"]
        ),
    ).add_to(m)

    # 全てのbbox を囲む範囲を計算して、地図の表示範囲を自動調整する
    if features:
        minx = min(feat["properties"]["bbox"][0] for feat in features)
        miny = min(feat["properties"]["bbox"][1] for feat in features)
        maxx = max(feat["properties"]["bbox"][2] for feat in features)
        maxy = max(feat["properties"]["bbox"][3] for feat in features)
        m.fit_bounds([[miny, minx], [maxy, maxx]])
    else:
        print("有効な bounding box が見つかりませんでした。")

    # 凡例（カスタムHTML）の追加
    # legend_html = """
    #  <div style="
    #  position: fixed;
    #  bottom: 50px; left: 50px; width: 180px; height: 50px;
    #  background-color: white;
    #  border:2px solid grey; z-index:9999; font-size:14px;
    #  padding: 10px;
    #  ">
    #  <b>凡例</b><br>
    #  <i style="background: blue; opacity:0.2; width: 18px; height: 18px; float: left; margin-right: 8px;"></i>
    #  GeoJSON Bounding Box
    #  </div>
    #  """
    # m.get_root().html.add_child(folium.Element(legend_html))

    # 地図をHTMLファイルとして保存
    output_map = Path("geojsons_bbox_map.html")
    m.save(str(output_map))
    print(f"地図を {output_map} として出力しました。")


if __name__ == "__main__":
    main()
