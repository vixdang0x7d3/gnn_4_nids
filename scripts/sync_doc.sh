#!/usr/bin/env bash

set -eou pipefail 

if [ $# -ne 2 ]; then
		echo "Usage: $0 <source> destination"
		exit 1
fi

SRC="${SRC:-$1}"
DST="${DST:-$2}"

# Validate source exists
if [ ! -d "$SRC" ]; then
		echo "Error: Source directory '$SRC' does not exist"
		exit 1
fi

mkdir -p "$DST"

if [ "${DRY_RUN:-}" = "1" ]; then 
		rsync -av --delete --exclude "*.tmp" --dry-run "$SRC/"  "$DST/"
else
		rsync -av --delete --exclude "*.tmp" "$SRC/"  "$DST/"
fi

if [ -d "$DST" ]; then
		echo "Converting line  endings in docs directory..."
		find "$DST" -type f \( -name "*.md" \) \
				-exec dos2unix {} \; 2>/dev/null || {
				echo "Warning: dos2unix not found or failed on some files"
		}
fi


