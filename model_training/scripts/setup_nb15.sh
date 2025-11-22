#!/usr/bin/env bash


ROOT_DIR=$(realpath ..)
CSV_PATH=$(realpath ../data/NB15_csv)
PQ_PATH=$(realpath ../data/NB15_preprocessed/raw)
SUBSET_PATH=$(realpath ../data/NB15_pp_sample/raw)
ZIP_FILE=nb15_tmp.zip

FILE_ID=1mBJzj_oeY5TFSYseRbmSkgVExzq3VwKb
SPLIT=false


if ! command -v curl &> /dev/null; then
    echo 'curl not found.'
    echo "Please install curl with:"
    echo "sudo apt install curl"
    exit 1
fi

if ! command -v uv &> /dev/null; then
    echo "uv not found."
    echo "Please install uv with:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

check_package() {
    if uv pip show "$1" --directory "$ROOT_DIR" &> /dev/null; then
        echo "$1 installed"
    else
        echo "$1 is missing"
        exit 1
    fi
}

required=("duckdb")

for pkg in "${required[@]}"; do
    check_package "$pkg"
done

echo "Downloading UNSW-NB15 to ${CSV_PATH}..."

mkdir -p "$CSV_PATH"
uvx gdown "https://drive.google.com/uc?id=${FILE_ID}" -O "${CSV_PATH}/${ZIP_FILE}"

if [ $? -eq 0 ]; then
    unzip -q "${CSV_PATH}/${ZIP_FILE}" -d "${CSV_PATH}"
    rm "${CSV_PATH}/${ZIP_FILE}"
fi

read -p "Split to train/test/val sets? (y/n): "  do_split

mkdir -p "$PQ_PATH"
if [[ "$do_split" =~ ^[Yy]$ ]]; then
    uv run ../src/preprocessing/preprocess_nb15.py \
        --raw_path "$CSV_PATH" --save_path "$PQ_PATH" --split
else
    uv run ../src/preprocessing/preprocess_nb15.py \
        --raw_path "$CSV_PATH" --save_path "$PQ_PATH"
fi

read -p "Save a subset of data to $SUBSET_PATH? (y/n): " do_subset

mkdir -p "$SUBSET_PATH"
if [[ "$do_subset" =~ ^[Yy]$ ]]; then
    uv run ../src/preprocessing/preprocess_nb15.py \
        --raw_path "$CSV_PATH" --save_path "$SUBSET_PATH" --sf 10
fi

