#!/bin/sh
# Read-only canonical schema gate. Generated descriptors stay in a temporary directory.
set -eu

cd "$(dirname "$0")"
test "$(protoc --version)" = "libprotoc 3.20.3"

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
trap 'exit 130' HUP INT TERM

for version in 1_0 1_1; do
  if test "$version" = 1_0; then minor=1.0; else minor=1.1; fi
  for root in common tool hook provider event_sink resource host_action telemetry_sink; do
    protoc -I "v$version" --include_imports \
      --descriptor_set_out="$scratch/$root-$minor.pb" "$root.proto"
    cmp "descriptors/$root-$minor.pb" "$scratch/$root-$minor.pb"
  done
done

python3 -B verify_810.py
python3 -B activation_model.py
node verify_echo.mjs
