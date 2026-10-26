#!/usr/bin/env bash
# Run in a disposable Noetic container with the Zenoh SDK already available.
set -euo pipefail
[[ $(id -u) == 0 ]] || { echo 'Run only inside an isolated root-owned test container' >&2; exit 2; }
if dpkg-query -W -f='${Status}' xgc2-onboard-telemetry 2>/dev/null | grep -q 'install ok installed'; then
  echo 'Refusing to replace an existing installation' >&2; exit 1
fi
[[ ! -e /usr/bin/onboard-telemetry && ! -e /usr/lib/onboard-telemetry ]] || { echo 'Installation path already exists' >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf -- "$tmp"' EXIT
"$root/build-deb.sh" --output-dir "$tmp/packages"
mapfile -t packages < <(find "$tmp/packages" -maxdepth 1 -type f -name '*.deb')
[[ ${#packages[@]} == 1 ]]
deb="${packages[0]}"
[[ $(dpkg-deb -f "$deb" Package) == xgc2-onboard-telemetry ]]
[[ $(dpkg-deb -f "$deb" Architecture) == all ]]
[[ $(dpkg-deb -f "$deb" Depends) == 'python3 (>= 3.8), ros-noetic-rospy, ros-noetic-roslib' ]]
! dpkg-deb -c "$deb" | grep -E 'site-packages|dist-packages|libzenoh|/zenoh/'
printf 'preserve\n' > "$tmp/unrelated"
dpkg --install "$deb"
[[ -x /usr/bin/onboard-telemetry ]]
export PYTHONDONTWRITEBYTECODE=1
/usr/bin/onboard-telemetry --help >/dev/null
export TELEMETRY_BIN=/usr/bin/onboard-telemetry
export PYTHONPATH="/usr/lib/onboard-telemetry${PYTHONPATH:+:$PYTHONPATH}"
python3 "$root/tests/integration_ros_zenoh.py"
dpkg --remove xgc2-onboard-telemetry
[[ ! -e /usr/bin/onboard-telemetry && ! -e /usr/lib/onboard-telemetry/onboard_telemetry/sender.py ]]
[[ $(cat "$tmp/unrelated") == preserve ]]
python3 -c 'import zenoh, rospy'
echo 'PASS: Debian package build, installation, real data flow and removal'
