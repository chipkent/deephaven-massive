#!/usr/bin/env bash
set -eu
case_dir="$(dirname "$0")"
cp -R "$case_dir/files/." .
cp "$case_dir/../../../../../docs/configuration.md" configuration.md
