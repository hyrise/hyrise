#!/bin/bash
# Run clang-tidy only on files changed compared to master branch.
# A changed header is checked via a .cpp that includes it: the first one found,
# or all of them when CLANG_TIDY_ALL_INCLUDING_TUS=1. Test files are not checked.
#
# Env:
#   CLANG_TIDY_ALL_INCLUDING_TUS=1  check all includers of a changed header
#   CLANG_TIDY_DIFF_BASE=<ref>      tidy files changed since <ref> (default: merge-base with master)
#   CLANG_TIDY_DEBUG=1              print the selected files to tidy
set -o pipefail

build_dir="${1:-clang-debug-tidy}"
jobs="${2:-$(nproc)}"
all_includers="${CLANG_TIDY_ALL_INCLUDING_TUS:-0}"
debug="${CLANG_TIDY_DEBUG:-0}"

[ -f "$build_dir/compile_commands.json" ] || {
  echo "FATAL: no compile_commands.json in $build_dir (need -DCMAKE_EXPORT_COMPILE_COMMANDS=ON)"; exit 1; }

# version.hpp and the jemalloc headers are generated during the build, but this build dir is only configured. Build
# just those two targets so that the files clang-tidy needs to parse exist.
[ -f "$build_dir/build.ninja" ] && ninja -C "$build_dir" libjemalloc-build version.hpp >/dev/null 2>&1

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

# A header is not a translation unit and has no entry in compile_commands.json, so clang-tidy cannot be run on it
# directly. It is analyzed through a .cpp that includes it; we add the first includer we find (or all of them).
for h in $hdrs; do
  inc=$(grep -rlF "$(basename "$h")" src --include=*.cpp | grep -v '^src/test' | sort)
  [ -z "$inc" ] && continue
  [ "$all_includers" = 1 ] || inc=$(echo "$inc" | head -n 1)
  cpps=$(printf '%s\n%s' "$cpps" "$inc")
done
# Drop duplicates (a .cpp can include several changed headers) and the empty line that the printf above adds when no
# .cpp file itself was changed.
cpps=$(printf '%s\n' "$cpps" | grep -v '^$' | sort -u)
[ -z "$cpps" ] && { echo "No translation units to analyze."; exit 0; }

# Regex of the changed headers for --header-filter below, e.g. "sort.hpp|table.hpp".
hf=$(for h in $hdrs; do basename "$h"; done | paste -sd'|' -)

# Print the diff base and the translation units we selected.
[ "$debug" = 1 ] && { echo "[debug] base=$base  tus=$(echo "$cpps" | grep -c .)"; echo "$cpps" | sed 's/^/  /'; }

# Run clang-tidy for each file, $jobs at a time. --header-filter restricts header diagnostics to the changed headers
# (omitted when none changed), so we do not fail on pre-existing issues in untouched headers.
printf '%s\n' "$cpps" | xargs -r -P "$jobs" -n 1 \
  clang-tidy -p "$build_dir" --quiet --warnings-as-errors='*' \
  ${hf:+--header-filter="($hf)\$"}
