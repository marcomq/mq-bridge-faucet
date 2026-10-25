#!/usr/bin/env bash
# Downloads the faucet CLI release the plugin is built against and checks its sha256.
# Usage: bench/fetch_faucet.sh [version]   (default: faucet-cli version in Cargo.lock)
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
ver=${1:-$(grep -A1 '^name = "faucet-cli"$' "$repo/Cargo.lock" | sed -n 's/^version = "\(.*\)"/\1/p')}
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) target=aarch64-apple-darwin ;;
  Darwin-x86_64) target=x86_64-apple-darwin ;;
  Linux-aarch64) target=aarch64-unknown-linux-gnu ;;
  Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
  *) echo "unsupported platform" >&2; exit 1 ;;
esac
dir="$repo/target/bench/faucet-cli-$ver"
asset="faucet-cli-$target.tar.xz"
url="https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v$ver/$asset"
mkdir -p "$dir"
cd "$dir"
curl -fsSL -o "$asset" "$url"
curl -fsSL -o "$asset.sha256" "$url.sha256"
want=$(awk '{print $1}' "$asset.sha256")
got=$(shasum -a 256 "$asset" | awk '{print $1}')
[ "$want" = "$got" ] || { echo "checksum mismatch for $asset" >&2; exit 1; }
tar -xJf "$asset"
echo "$dir/faucet-cli-$target/faucet"
