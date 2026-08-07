#!/bin/bash
# Run clang-tidy only on files changed in a PR, for faster CI feedback.
# A changed header is checked via a .cpp that includes it: the first one found,
# or all of them when CLANG_TIDY_ALL_INCLUDERS=1 (set by the "Refactor" label).
#
# Env:
#   CLANG_TIDY_ALL_INCLUDERS=1  check all includers of a changed header
#   CLANG_TIDY_DIFF_BASE=<ref>  override the diff base (local dev)
#   CLANG_TIDY_DEBUG=1          print the selected files
set -o pipefail

build_dir="${1:-clang-debug-tidy}"
jobs="${2:-$(( $(nproc) / 4 ))}"
[ "$jobs" -lt 1 ] && jobs=1
all_includers="${CLANG_TIDY_ALL_INCLUDERS:-0}"
debug="${CLANG_TIDY_DEBUG:-0}"

[ -f "$build_dir/compile_commands.json" ] || {
  echo "FATAL: no compile_commands.json in $build_dir (need -DCMAKE_EXPORT_COMPILE_COMMANDS=ON)"; exit 1; }

# Diff base: previous commit on master, else the merge-base with master.
if [ -n "$CLANG_TIDY_DIFF_BASE" ]; then
  base="$CLANG_TIDY_DIFF_BASE"
elif [ "$BRANCH_NAME" = "master" ]; then
  base=$(git rev-parse HEAD^1)
else
  git fetch --no-tags --quiet origin master
  base=$(git merge-base FETCH_HEAD HEAD)
fi

changed=$( { git diff --diff-filter=d --name-only "$base"; \
             git ls-files --others --exclude-standard; } \
           | grep -E '^src/.*\.[chi]pp$' | grep -v '^src/test' | sort -u )
[ -z "$changed" ] && { echo "No tidy-relevant files changed."; exit 0; }

cpps=$(echo "$changed" | grep '\.cpp$')
hdrs=$(echo "$changed" | grep -E '\.[hi]pp$')

# For each changed header, find .cpp files that mention it and add the first
# one (or all of them in Refactor mode).
for h in $hdrs; do
  inc=$(grep -rlF "$(basename "$h")" src --include=*.cpp | grep -v '^src/test' | sort)
  [ -z "$inc" ] && continue
  [ "$all_includers" = 1 ] || inc=$(echo "$inc" | head -n 1)
  cpps=$(printf '%s\n%s' "$cpps" "$inc")
done
cpps=$(printf '%s\n' "$cpps" | grep -v '^$' | sort -u)
[ -z "$cpps" ] && { echo "No translation units to analyze."; exit 0; }

# Restrict header diagnostics to the changed headers.
hf=$(for h in $hdrs; do basename "$h"; done | paste -sd'|' -)

[ "$debug" = 1 ] && { echo "[debug] base=$base  tus=$(echo "$cpps" | grep -c .)"; echo "$cpps" | sed 's/^/  /'; }

printf '%s\n' "$cpps" | xargs -r -P "$jobs" -n 1 \
  clang-tidy -p "$build_dir" --quiet --warnings-as-errors='*' \
  ${hf:+--header-filter="($hf)\$"}