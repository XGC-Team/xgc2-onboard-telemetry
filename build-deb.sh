#!/usr/bin/env bash
# Build the product-only, architecture-independent Debian package.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_dir=/output/packages
if (( $# )); then
  [[ $# == 2 && "$1" == --output-dir ]] || { echo 'Usage: build-deb.sh [--output-dir DIR]' >&2; exit 2; }
  output_dir="$2"
fi
version="$(cat "$root/VERSION")"
[[ -n "$version" ]] && dpkg --validate-version "$version"
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
stage="$(mktemp -d)"
trap 'rm -rf -- "$stage"' EXIT
"$root/install.sh" "$stage/usr"
install -d "$stage/DEBIAN"
cat > "$stage/DEBIAN/control" <<CONTROL
Package: xgc2-onboard-telemetry
Version: $version
Section: net
Priority: optional
Architecture: all
Maintainer: XGC2 Engineering <141200314+lxk36@users.noreply.github.com>
Depends: python3 (>= 3.8), ros-noetic-rospy, ros-noetic-roslib
Description: Configured ROS1 to Zenoh onboard telemetry sender
 Forwards selected ROS1 messages as their original serialized bytes to
 explicitly configured Zenoh keys, with per-topic latest-value rate limiting.
CONTROL
dpkg-deb --root-owner-group --build "$stage" "$output_dir/xgc2-onboard-telemetry_${version}_all.deb"
