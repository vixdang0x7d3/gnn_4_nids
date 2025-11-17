#!/usr/bin/env bash

if ! command -v python3 &> /dev/null; then
    echo "uv not found."
    echo "Please install uv with:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
fi

check_package() {
    if uv pip show "$1" &> /dev/null; then
        echo "$1 installed"
    else
        echo "$1 is missing"
        exit 1
    fi
}

required=("duckdb", "kaggle")

for pkg in "${required[@]}"; do
    check_package "$pkg"
done

echo "Downloading UNSW-NB15 dataset..."
uv run kaggle datasets download mrwellsdavid/unsw-nb15





