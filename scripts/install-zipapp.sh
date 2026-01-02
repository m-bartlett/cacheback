#!/usr/bin/env bash
install_dir="${1:?install directory required}"
if ! [ -d "$install_dir" ]; then
    echo "Error: $install_dir is not a directory"
    exit 1
fi

output_path="$install_dir/cacheback"
pkg_dir="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp --directory)"
trap "rm -r $work_dir" EXIT
pip install $pkg_dir --no-cache-dir --target="$work_dir"
python -m zipapp \
    --output "$output_path" \
    --python='/usr/bin/env python' \
    --main 'cacheback.__main__:main' \
    "$work_dir"

[ -x "$output_path" ] && "$output_path" --version