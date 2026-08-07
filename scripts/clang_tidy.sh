#!/bin/bash
set -o pipefail
build_dir="${1:-clang-debug-tidy}"

# Set CLANG_TIDY_DEBUG=1 for verbose diagnostics.
debug="${CLANG_TIDY_DEBUG:-0}"

# Determine diff base:
#  - master builds: diff against the previous state of master, so we tidy
#    exactly what the latest merge introduced.
#  - branch/PR builds: diff against the merge-base with origin/master.
if [ -n "$CLANG_TIDY_DIFF_BASE" ]; then
  base="$CLANG_TIDY_DIFF_BASE"
elif [ "$BRANCH_NAME" = "master" ]; then
  # Prefer the last successfully built commit (covers multi-commit pushes);
  # fall back to first parent.
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
[ "$debug" = 1 ] && echo "[debug] diff base: $base"

changed=$( { git diff --diff-filter=d --name-only "$base"; \
             git ls-files --others --exclude-standard; } \
           | grep -E '^src/.*\.[chi]pp$' | grep -v '^src/test' | sort -u )
[ -z "$changed" ] && { echo "No tidy-relevant files changed."; exit 0; }

cpps=$(echo "$changed" | grep '\.cpp$')
hdrs=$(echo "$changed" | grep -E '\.[hi]pp$')

if [ "$debug" = 1 ]; then
  echo "[debug] changed tidy-relevant files:"
  echo "$changed" | sed 's/^/  /'
  echo "[debug] directly changed .cpp: $(echo "$cpps" | grep -c .)"
  echo "[debug] changed headers/.ipp:  $(echo "$hdrs" | grep -c .)"
fi

# Header/ipp → direct includers. Hyrise includes are rooted at src/lib etc.,
for h in $hdrs; do
  inc=$(echo "$h" | sed -E 's|^src/(lib\|bin\|benchmarklib\|plugins)/||')
  includers=$(grep -rlE "#include \"$inc\"" src/lib src/bin src/benchmarklib src/plugins | grep '\.cpp$')
  [ "$debug" = 1 ] && echo "[debug] header $h -> $(echo "$includers" | grep -c .) direct .cpp includer(s)"
  cpps=$(printf "%s\n%s" "$cpps" "$includers")
done
cpps=$(echo "$cpps" | grep -v '^$' | sort -u)
[ -z "$cpps" ] && { echo "Changed headers have no direct .cpp includers."; exit 0; }

# Restrict header diagnostics to the *changed* headers only.
hf=$(echo "$hdrs" | sed -E 's|^src/(lib\|bin\|benchmarklib\|plugins)/||' | paste -sd'|' -)

n_tu=$(echo "$cpps" | grep -c .)
if [ "$debug" = 1 ]; then
  echo "[debug] header-filter: [${hf}]"
  if [ -f "$build_dir/compile_commands.json" ] && command -v jq >/dev/null; then
    echo "[debug] compile_commands.json entries: $(jq length "$build_dir/compile_commands.json")"
  else
    echo "[debug] compile_commands.json: missing or jq unavailable at $build_dir"
  fi
  echo "[debug] translation units to analyze: $n_tu"
  echo "$cpps" | sed 's/^/  run: /'
fi

# --- Run tidy ---------------------------------------------------------------
if [ "$debug" = 1 ]; then
  # Capture per-file output so we can summarize what actually fired.
  log_dir=$(mktemp -d)
  echo "$cpps" | xargs -r -P "$(nproc)" -n 1 -I{} \
    bash -c 'f="$1"; ld="$2"; bd="$3"; hf="$4";
             out="$ld/$(echo "$f" | tr / _).log";
             if [ -n "$hf" ]; then
               clang-tidy -p "$bd" --quiet --header-filter="($hf)\$" "$f" >"$out" 2>&1
             else
               clang-tidy -p "$bd" --quiet "$f" >"$out" 2>&1
             fi
             echo "  $f: exit=$? lines=$(wc -l <"$out")"' _ {} "$log_dir" "$build_dir" "$hf"
  rc=$?

  echo "=== per-check breakdown (deduplicated by check name) ==="
  cat "$log_dir"/*.log 2>/dev/null \
    | grep -oE '\[[a-z0-9]+(-[a-z0-9]+)*(,[a-z0-9-]+)*\]$' \
    | sort | uniq -c | sort -rn
  echo "=== unique (file:line:col: severity) findings ==="
  cat "$log_dir"/*.log 2>/dev/null | grep -E ': (warning|error):' | sort -u | wc -l
  echo "[debug] per-file logs in: $log_dir"
  exit "$rc"
else
  echo "$cpps" | xargs -r -P "$(nproc)" -n 1 \
    clang-tidy -p "$build_dir" --quiet --warnings-as-errors='*' \
    ${hf:+--header-filter="($hf)\$"}
fi