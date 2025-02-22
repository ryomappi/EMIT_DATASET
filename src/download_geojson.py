import argparse
import sys
import csv
import json
import earthaccess
import requests
from pathlib import Path
from dotenv import load_dotenv


def load_existing_daac_names(output_dir):
    """
    outdir 内の連番 geojson (n.json) から、連番と DAAC Scene Names の対応情報を辞書形式で返す。
    また、すべての DAAC Scene Names をセットで返す。
    """
    records = {}
    existing_names = set()
    for f in output_dir.glob("*.json"):
        if f.stem.isdigit():
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                features = data.get("features", [])
                if features:
                    props = features[0].get("properties", {})
                    names = props.get("DAAC Scene Names", [])
                    records[f.stem] = names
                    for n in names:
                        existing_names.add(n)
            except Exception as e:
                print(f"{f} の読み込みに失敗: {e}")
                continue
    return records, existing_names


def save_records_csv(records, csv_path):
    """
    連番と DAAC Scene Names の対応情報を CSV として出力。
    CSV の列は "Sequence", "DAAC Scene Names" とする。
    """
    with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Sequence", "DAAC Scene Names"])
        for seq, names in sorted(records.items(), key=lambda x: int(x[0])):
            writer.writerow([seq, ";".join(names)])
    print(f"CSVファイルを書き出しました: {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Download GeoJSON files from Earthdata Search"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/dataset/geojsons",
        help="Directory to save geojson files",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="2023-01-01",
        help="Start date for search (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default="2024-12-31",
        help="End date for search (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--max_downloads",
        type=int,
        default=0,
        help="Maximum number of GeoJSON files to download (0 means no limit)",
    )
    args = parser.parse_args()

    load_dotenv()
    earthaccess.login(strategy="environment", persist=True)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 既存のダウンロード済み geojson から、連番と DAAC Scene Names を取得
    records, existing_names = load_existing_daac_names(output_dir)
    # 出力先は outdir の1つ上の階層
    csv_path = output_dir.parent / "geojson_daac_scene_names.csv"
    save_records_csv(records, csv_path)

    # geojson用の検索。short_name は実際のサービスポリシーに合わせてください。
    results = earthaccess.search_data(
        short_name="EMITL2BCH4PLM", temporal=(args.start_date, args.end_date)
    )

    if not results:
        print("検索結果が見つかりませんでした。")
        sys.exit(1)

    # 既に出力ディレクトリにある連番ファイルから最大番号を調べる
    existing_files = list(output_dir.glob("*.json"))
    max_num = 0
    for f in existing_files:
        if f.stem.isdigit():
            num = int(f.stem)
            if num > max_num:
                max_num = num
    seq_num = max_num + 1

    download_count = 0
    for granule in results:
        if args.max_downloads > 0 and download_count >= args.max_downloads:
            break

        links = granule.data_links()
        # 拡張子が .json のリンクを抽出
        geojson_links = [link for link in links if link.endswith(".json")]
        if not geojson_links:
            print("granule に geojson のリンクが見つかりませんでした。")
            continue

        # 一旦候補の URL から内容を取得して、DAAC Scene Names を抽出
        url = geojson_links[0]
        try:
            r = requests.get(url, stream=True)
            r.raise_for_status()
            data = json.loads(r.content)
            features = data.get("features", [])
            if not features:
                print(f"{url} の内容から features が見つかりませんでした。")
                continue
            props = features[0].get("properties", {})
            new_names = props.get("DAAC Scene Names", [])
        except Exception as e:
            print(f"{url} の取得に失敗しました: {e}")
            continue

        # 既存の geojson の DAAC Scene Names と被っていないかチェック（少なくとも一つも共通がなければ OK）
        if any(n in existing_names for n in new_names):
            print(
                f"{url} の DAAC Scene Names は既存と被っているため、ダウンロードをスキップします。"
            )
            continue

        # 被っていなければ連番名で保存
        dest = output_dir / f"{seq_num}.json"
        if dest.exists():
            print(f"{dest} は既に存在します。スキップします。")
            seq_num += 1
            continue

        print(f"{url} を {dest} にダウンロード中...")
        try:
            dest.write_bytes(r.content)
            print(f"ダウンロード完了: {dest}")
            download_count += 1
            # 更新: 新たにダウンロードしたファイルの DAAC Scene Names を既存セット・レコードに追加
            records[str(seq_num)] = new_names
            for n in new_names:
                existing_names.add(n)
            seq_num += 1
        except Exception as e:
            print(f"{url} のダウンロードに失敗しました: {e}")
            continue

    print(
        "すべてのGeoJSONのダウンロードが完了しました。ダウンロード件数:", download_count
    )
    # 最終的な対応関係をCSVとして再出力
    save_records_csv(records, csv_path)


if __name__ == "__main__":
    main()
