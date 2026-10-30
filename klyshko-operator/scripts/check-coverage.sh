#!/usr/bin/env bash
#
# Copyright (c) 2026 - for information on the respective copyright owner
# see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.
#
# SPDX-License-Identifier: Apache-2.0
#
# Fails if the total statement coverage of a Go coverage profile (generated code excluded) is below a floor.
#
# Usage: scripts/check-coverage.sh <coverage-profile> <floor-percent>
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <coverage-profile> <floor-percent>" >&2
  exit 2
fi
profile=$1
floor=$2

if [ ! -s "$profile" ]; then
  echo "coverage profile '$profile' missing or empty" >&2
  exit 2
fi
if ! [[ "$floor" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "floor '$floor' is not a number" >&2
  exit 2
fi

filtered=$(mktemp)
trap 'rm -f "$filtered"' EXIT
grep -v 'zz_generated' "$profile" > "$filtered"

total=$(go tool cover -func="$filtered" | awk '/^total:/ { gsub("%", "", $NF); print $NF }')
if [ -z "$total" ]; then
  echo "could not determine total coverage" >&2
  exit 2
fi

echo "total coverage (excluding generated code): ${total}% (floor: ${floor}%)"
if awk -v t="$total" -v f="$floor" 'BEGIN { exit !(t + 0 < f + 0) }'; then
  echo "coverage ${total}% is below the floor of ${floor}%" >&2
  exit 1
fi
