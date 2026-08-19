#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
  cat <<EOF
Usage: $(basename "$0") --admin [--username NAME]

Options:
  --admin           Create or update the admin user (prompts for a password)
  --username NAME   Admin username (default: admin)
  -h, --help        Show this help message
EOF
}

ADMIN=false
USERNAME="admin"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --admin)
      ADMIN=true
      shift
      ;;
    --username)
      [[ $# -ge 2 ]] || { echo "--username requires a value" >&2; exit 1; }
      USERNAME="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$ADMIN" != true ]]; then
  usage
  exit 1
fi

read -rsp "Password for '$USERNAME': " PASSWORD
echo
read -rsp "Confirm password: " PASSWORD_CONFIRM
echo

if [[ -z "$PASSWORD" ]]; then
  echo "Password cannot be empty" >&2
  exit 1
fi

if [[ "$PASSWORD" != "$PASSWORD_CONFIRM" ]]; then
  echo "Passwords do not match" >&2
  exit 1
fi

echo "Starting db and web containers..."
docker compose up -d db web >/dev/null

# Password is piped over stdin (never passed as an argv) so it never shows up
# in `docker compose exec` process listings.
printf '%s' "$PASSWORD" | docker compose exec -T web python -m app.manage create-admin \
  --username "$USERNAME" --password-stdin

echo "Admin user '$USERNAME' is ready. Log in at http://localhost:8765/login"
