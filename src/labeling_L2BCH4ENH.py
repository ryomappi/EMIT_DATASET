import argparse
import numpy as np
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--l2b", type=str, help="L2BCH4PLM numpy data path")
    args = parser.parse_args()

    l2b_path = args.l2b

    # L2BPLMデータ読み込み
    l2b_data = np.load(l2b_path)

    # 0のとこは0、それ以外は1にする
    l2b_data[l2b_data == 0] = 0
    l2b_data[l2b_data != 0] = 1

    # 保存
    output_path = Path(f"{l2b_path}.npy")
