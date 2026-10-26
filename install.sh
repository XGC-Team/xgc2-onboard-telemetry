#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
prefix="${1:-/usr/local}"
[[ "$prefix" = /* ]] || { echo 'Usage: install.sh [absolute-prefix]' >&2; exit 2; }
install -d "$prefix/lib/onboard-telemetry/onboard_telemetry" "$prefix/bin" "$prefix/share/xgc2/process-definitions"
install -m 0644 "$root"/onboard_telemetry/*.py "$prefix/lib/onboard-telemetry/onboard_telemetry/"
install -m 0644 "$root/process-definitions/xgc2-onboard-telemetry.json" "$prefix/share/xgc2/process-definitions/"
cat > "$prefix/bin/onboard-telemetry" <<'ENTRY'
#!/usr/bin/env bash
set -euo pipefail
prefix="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$prefix/lib/onboard-telemetry${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m onboard_telemetry "$@"
ENTRY
chmod 0755 "$prefix/bin/onboard-telemetry"
