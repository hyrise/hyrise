# Running Hash Aggregation Benchmarks

Quick guide for running the playground benchmarks on HPC or local machines.

## Configuration

### 1. Edit Parameters in playground.cpp

Open [playground.cpp](playground.cpp) and modify the configuration:

#### Scale Factors (line ~854)
```cpp
const auto scale_factors = std::vector<float>{1.0f, 2.0f, 4.0f, 8.0f, 16.0f, 32.0f};
```
**What it does:** Runs benchmarks for each TPC-H scale factor in the list.
- SF 1 ≈ 6M rows (~1 GB)
- SF 10 ≈ 60M rows (~10 GB)
- SF 32 ≈ 192M rows (~32 GB)

#### PlaygroundConfig (lines ~36-44)
```cpp
struct PlaygroundConfig {
  uint32_t num_workers = 10;        // Worker threads for parallel variants
  uint32_t num_iterations = 7;      // Iterations per algorithm (for averaging)
  bool run_single_baseline = true;  // Single-threaded baseline (always enable!)
  bool run_single_optimized = true; // Single-threaded optimized
  bool run_multi_naive = true;      // Multi-threaded naive
  bool run_multi_optimized = true;  // Multi-threaded optimized
};
```

**Key settings:**
- `num_workers`: Set to your CPU core count (e.g., 10, 20, 32)
- `num_iterations`: More iterations = better averaging, but longer runtime
- `run_single_baseline`: **MUST be true** if any other variant is enabled (for validation)

#### Which Benchmark to Run (line ~864)
```cpp
run_hash_micro_benchmark(scale_factor);        // Hash aggregation
// run_sort_micro_benchmark(scale_factor);     // Sort aggregation (TODO)
```
Uncomment the line for the benchmark you want to run.

## Building

### IMPORTANT: Use Release Build for Accurate Timings

Debug builds are ~10x slower than release builds. Always use release mode for benchmarking.

```bash
# From repository root
mkdir cmake-build-release
cd cmake-build-release

# Configure with Release mode
cmake -DCMAKE_BUILD_TYPE=Release ..

# Build (parallel compilation)
make -j$(nproc) hyrisePlayground
```

**Note:** `$(nproc)` uses all available CPU cores. On macOS, use `$(sysctl -n hw.ncpu)` instead.

## Running

### Local Execution
```bash
cd cmake-build-release
./hyrisePlayground
```

### HPC/SLURM Execution

**Option 1: Interactive Job**
```bash
srun --nodes=1 --ntasks=1 --cpus-per-task=32 --mem=64G --time=2:00:00 --pty bash
cd cmake-build-release
./hyrisePlayground
```

**Option 2: Batch Script** (`run_benchmark.sh`)
```bash
#!/bin/bash
#SBATCH --job-name=hyrise_bench
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH --output=benchmark_%j.log

cd $HOME/hyrise/cmake-build-release
./hyrisePlayground
```

Submit with: `sbatch run_benchmark.sh`

## Output

The benchmark prints:
1. **Per-algorithm timings** (avg, min, max across iterations)
2. **Validation status** `[VALIDATED]` on first iteration
3. **Per-scale-factor total time** after each scale factor completes
4. **Total benchmark time** at the end

### Example Output
```
######################################
# SCALE FACTOR: 1
# Workers: 10
######################################

=== Running HASH Microbenchmark ===
- Generating TPC-H LineItem (SF 1)...
  [Mode: Single Threaded]
  Running Hash Single Baseline (5 iterations)... [VALIDATED] Done.
    Avg: 3984.0 ms (trimmed) | Min: 3950 ms | Max: 4020 ms
  ...

>>> Scale Factor 1 completed in 15234 ms (15.23 s)

######################################
# SCALE FACTOR: 2
# Workers: 10
######################################
...

=============================================
TOTAL BENCHMARK TIME: 87654 ms (87.65 s)
=============================================
```

## Troubleshooting

### Build Errors
- **Missing dependencies:** Run `./install_dependencies.sh` from repo root
- **Wrong build type:** Check `cmake-build-release` directory exists

### Runtime Errors
- **Validation fails:** Ensure `run_single_baseline = true` in config
- **Out of memory:** Reduce scale factors or request more memory in SLURM
- **Slow performance:** Verify you built with `-DCMAKE_BUILD_TYPE=Release`

### Performance Tips
- **Worker count:** Set `num_workers` to match allocated CPU cores
- **Iterations:** 5 iterations is usually sufficient; 3 minimum for trimmed mean
- **Scale factors:** Start small (SF 1-4) to verify correctness before large runs

## Quick Checklist

- [x] Edited scale factors in `playground.cpp` (line ~854)
- [x] Set `num_workers` to match CPU cores (line ~38)
- [ ] Enabled desired algorithm variants (lines ~40-43)
- [ ] Built with `CMAKE_BUILD_TYPE=Release`
- [ ] Allocated sufficient memory for largest scale factor
- [ ] `run_single_baseline = true` for validation

## See Also
- [README_DPMH.md](README_DPMH.md) - Detailed implementation documentation
- [CLAUDE.md](../../CLAUDE.md) - General Hyrise build instructions
