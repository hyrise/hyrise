#!/bin/bash
# Run clang-tidy only on the files a PR changed, for fast CI feedback.
#
# Selection:
#   * every changed .cpp is analyzed;
#   * For a changed header (.hpp/.ipp), we analyze ONE representative
#     direct includer. A header is not its own translation unit, so it can only be
#     analyzed through a .cpp that includes it. 
#   * CLANG_TIDY_ALL_INCLUDERS=1 (the
#     "Refactor" label) analyzes all direct includers instead.
#
# Env:
#   CLANG_TIDY_ALL_INCLUDERS=1  expand headers to all includers (Refactor label)
#   CLANG_TIDY_DIFF_BASE=<ref>  override the diff base (local dev)
#   CLANG_TIDY_DEBUG=1          verbose selection diagnostics

set -o pipefail

build_dir="${1:-clang-debug-tidy}"
jobs="${2:-$(( $(nproc) / 4 ))}"
[ "$jobs" -lt 1 ] && jobs=1
all_includers="${CLANG_TIDY_ALL_INCLUDERS:-0}"
debug="${CLANG_TIDY_DEBUG:-0}"

command -v clang-tidy >/dev/null || { echo "FATAL: clang-tidy not on PATH"; exit 1; }
[ -f "$build_dir/compile_commands.json" ] || {
  echo "FATAL: no compile_commands.json in $build_dir"
  echo "       (configure it with -DCMAKE_EXPORT_COMPILE_COMMANDS=ON)"; exit 1; }

# version.hpp is generated at build time; build just that target so the one TU
# including it can be parsed without a full build.
[ -f "$build_dir/build.ninja" ] && ninja -C "$build_dir" version.hpp >/dev/null 2>&1

# --- diff base -------------------------------------------------------------
#   master builds: diff against the previous state of master (what the latest
#                  merge introduced); branch/PR builds: merge-base with master.
if [ -n "$CLANG_TIDY_DIFF_BASE" ]; then
  base="$CLANG_TIDY_DIFF_BASE"
elif [ "$BRANCH_NAME" = "master" ]; then
  if [ -n "$GIT_PREVIOUS_SUCCESSFUL_COMMIT" ] \
     && git cat-file -e "$GIT_PREVIOUS_SUCCESSFUL_COMMIT" 2>/dev/null; then
    base="$GIT_PREVIOUS_SUCCESSFUL_COMMIT"
  else
    base=$(git rev-parse HEAD^1)
  fi
else
  git fetch --no-tags --quiet origin master
  base=$(git merge-base FETCH_HEAD HEAD)
fi
[ "$debug" = 1 ] && echo "[debug] diff base: $base  (all_includers=$all_includers, jobs=$jobs)"

# Changed, tidy-relevant files:
changed=$( { git diff --diff-filter=d --name-only "$base"; \
             git ls-files --others --exclude-standard; } \
           | grep -E '^src/.*\.[chi]pp$' | grep -v '^src/test' | sort -u )
[ -z "$changed" ] && { echo "No tidy-relevant files changed."; exit 0; }

cpps=$(echo "$changed" | grep '\.cpp$')
hdrs=$(echo "$changed" | grep -E '\.[hi]pp$')

# Get a header's rooted include spelling.
# NOTE: sed delimiter is '#', not '|' -- '|' would clash with the alternation.
rooted_spelling() { printf '%s' "$1" | sed -E 's#^src/(lib|bin|benchmarklib|plugins)/##'; }

# Resolve a header's direct .cpp includers:
includers_of() {
  local h="$1" rooted dir base_name
  rooted=$(rooted_spelling "$h")
  dir=$(dirname "$h"); base_name=$(basename "$h")
  {
    grep -rlF "#include \"$rooted\"" \
         src/lib src/bin src/benchmarklib src/plugins --include=*.cpp 2>/dev/null
    grep -lF "#include \"$base_name\"" "$dir"/*.cpp 2>/dev/null
  } | grep -v '^src/test' | sort -u
}

# Expand headers to includers
for h in $hdrs; do
  inc=$(includers_of "$h")
  if [ -z "$inc" ]; then
    [ "$debug" = 1 ] && echo "[debug] header $h -> no .cpp includer (skipped)"
    continue
  fi
  if [ "$all_includers" = 1 ]; then
    [ "$debug" = 1 ] && echo "[debug] header $h -> $(echo "$inc" | grep -c .) includer(s) [all]"
    cpps=$(printf '%s\n%s' "$cpps" "$inc")
  else
    one=$(echo "$inc" | head -n 1)
    [ "$debug" = 1 ] && echo "[debug] header $h -> representative TU: $one"
    cpps=$(printf '%s\n%s' "$cpps" "$one")
  fi
done
cpps=$(printf '%s\n' "$cpps" | grep -v '^$' | sort -u)
[ -z "$cpps" ] && { echo "No .cpp translation units to analyze."; exit 0; }

# Header-filter: attribute findings to the changed headers only:
hf=$(for h in $hdrs; do rooted_spelling "$h"; echo; done \
     | grep -v '^$' \
     | sed 's/[.^$*+?()|{}[]/\\&/g' \
     | paste -sd'|' -)

n_tu=$(printf '%s\n' "$cpps" | grep -c .)
if [ "$debug" = 1 ]; then
  echo "[debug] header-filter: [${hf}]"
  echo "[debug] translation units to analyze: $n_tu"
  printf '%s\n' "$cpps" | sed 's/^/  run: /'
fi

# Run and report findings.
printf '%s\n' "$cpps" | xargs -r -P "$jobs" -n 1 \
  clang-tidy -p "$build_dir" --quiet --warnings-as-errors='*' \
  ${hf:+--header-filter="($hf)\$"}