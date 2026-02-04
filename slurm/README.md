# Hyrise HPC Cluster Benchmarking

This directory contains SLURM batch scripts for running Hyrise benchmarks on HPC clusters using containerized environments.

## Prerequisites

1. **HPC Cluster Access**
   - SLURM workload manager installed
   - Pyxis/Enroot container support (for Docker images)
   - X86 architecture nodes available

2. **SLURM Account**
   - You need a valid account/project allocation
   - Example: `sci-rabl-data-processing`

3. **Repository Setup**
   - Clone this repository to your home directory on the cluster
   - Ensure you have sufficient disk space (~10 GB for build artifacts)

## Quick Start

### 1. Initial Setup (One-time)

```bash
# Clone repository (if not already done)
cd ~
git clone <repo-url> hyrise
cd hyrise

# Create results directory
mkdir -p results

# Configure your SLURM account
export SLURM_ACCOUNT=sci-rabl-data-processing  # Replace with your account
```

### 2. Configure Benchmark Settings

Edit [src/bin/playground.cpp](../src/bin/playground.cpp) to set:

- **Line 51:** `num_workers` - Set to 32 for full node utilization
- **Line 52:** `num_iterations` - Set to 7 (or more) for statistical significance
- **Lines 53-57:** Enable/disable benchmark variants:
  - `run_single_baseline` - Single-threaded baseline
  - `run_single_optimized` - Single-threaded optimized
  - `run_multi_naive` - Multi-threaded naive implementation
  - `run_multi_optimized` - Multi-threaded optimized implementation
- **Line 1095:** `scale_factors` - Currently: `{1.0f, 2.0f, 4.0f, 8.0f, 16.0f, 32.0f}`
- **Lines 1115-1116:** Choose algorithm (hash or sort):
  ```cpp
  // run_hash_micro_benchmark(scale_factor);  // Comment out for sort
  run_sort_micro_benchmark(scale_factor);     // Uncomment for sort
  ```

### 3. Update SLURM Account

**Option A: Environment Variable (Recommended)**
```bash
export SLURM_ACCOUNT=sci-rabl-data-processing
```

**Option B: Edit Script Directly**

Edit [run_benchmark.slurm](run_benchmark.slurm) line 3:
```bash
#SBATCH --account=sci-rabl-data-processing
```

### 4. Submit Job

```bash
sbatch slurm/run_benchmark.slurm
```

You'll see output like:
```
Submitted batch job 12345
```

### 5. Monitor Job

```bash
# Check job status
squeue -u $USER

# Watch live output (replace 12345 with your job ID)
tail -f results/benchmark_12345.log

# View full log after completion
cat results/benchmark_12345.log
```

### 6. Cancel Job (if needed)

```bash
scancel 12345  # Replace with your job ID
```

## Job Configuration

The [run_benchmark.slurm](run_benchmark.slurm) script is configured with:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `--cpus-per-task` | 32 | Number of CPU cores |
| `--mem` | 128G | RAM allocation |
| `--time` | 6:00:00 | Maximum runtime (6 hours) |
| `--exclusive` | - | Lock entire node for stable measurements |
| `--constraint` | ARCH:X86 | X86 architecture required |
| `--partition` | cpu-batch | CPU partition (not GPU) |

### Customizing Resource Allocation

To modify resources, edit the `#SBATCH` directives in [run_benchmark.slurm](run_benchmark.slurm):

**For smaller test runs:**
```bash
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=1:00:00
```

**For larger benchmarks:**
```bash
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=12:00:00
```

**Important:** If you change `--cpus-per-task`, also update `num_workers` in [playground.cpp](../src/bin/playground.cpp) to match!

## How It Works

### Build Process (First Run Only)

1. **Container Setup**: Pulls Ubuntu 25.04 Docker image
2. **Dependency Installation**: Runs [install_dependencies.sh](../install_dependencies.sh)
   - Installs system packages (clang, cmake, boost, tbb, etc.)
   - Clones git submodules
   - Installs Python requirements
3. **Compilation**: Builds `hyrisePlayground` in Release mode
   - Uses Clang 20 compiler with C++20
   - Parallel compilation with all allocated cores
   - Build artifacts saved to `cmake-build-release/`

### Benchmark Execution (Every Run)

1. **Environment Configuration**:
   - Sets CPU affinity for stable measurements
   - Configures OpenMP threading: `OMP_NUM_THREADS=32`
2. **Benchmark Execution**:
   - Runs `hyrisePlayground` binary
   - Executes configured scale factors and iterations
   - Outputs results to stdout (captured in log file)

### Subsequent Runs

If `cmake-build-release/hyrisePlayground` already exists, the build step is skipped and only the benchmark runs. This saves ~15-20 minutes per job.

**To force a rebuild:**
```bash
rm -rf cmake-build-release/
sbatch slurm/run_benchmark.slurm
```

## Expected Runtime

| Phase | Duration | Notes |
|-------|----------|-------|
| **First job (with build)** |  |  |
| - Dependency installation | ~10-15 min | One-time per container |
| - Compilation | ~10-15 min | Parallel with 32 cores |
| - Benchmark execution | ~3-5 hours | Full scale factor set (1.0-32.0), 7 iterations |
| **Total first run** | ~5-6 hours | Within 6h time limit |
| **Subsequent jobs** |  |  |
| - Benchmark execution | ~3-5 hours | Build skipped |
| **Total subsequent runs** | ~3-5 hours |  |

## Memory Requirements

Scale factor determines TPC-H LineItem table size:

| Scale Factor | Rows | Approximate RAM | Notes |
|--------------|------|-----------------|-------|
| 1.0 | ~6M | 1-2 GB | Quick testing |
| 2.0 | ~12M | 2-4 GB |  |
| 4.0 | ~24M | 4-8 GB |  |
| 8.0 | ~48M | 8-16 GB |  |
| 16.0 | ~96M | 16-32 GB |  |
| 32.0 | ~192M | 32-64 GB | Maximum recommended |
| **Full set** | **1.0-32.0** | **~80-100 GB** | 128 GB allocation provides safe margin |

## Container Execution

This script uses **pyxis** to run jobs inside Docker containers. Key benefits:

1. **No sudo required on host**: Container runs as root internally
2. **Reproducible environment**: Same Ubuntu 25.04 base for all team members
3. **Dependency isolation**: No conflicts with system packages
4. **Easy setup**: No manual package installation

The repository is mounted at `/workspace` inside the container, so all build artifacts and results persist to your home directory.

## Troubleshooting

### Build Issues

**Problem:** `sudo: command not found`

**Solution:** Ubuntu 25.04 base image may not include sudo. The script automatically installs it. If this fails, switch to ubuntu:24.04:
```bash
# Edit run_benchmark.slurm line 31
CONTAINER_IMAGE="ubuntu:24.04"
```

---

**Problem:** `CMake not found` or `Boost not found`

**Solution:** Dependency installation failed. Check the error log:
```bash
cat results/benchmark_JOBID.err
```

If install_dependencies.sh failed, you can manually install dependencies:
```bash
# In the SLURM script, add before ./install_dependencies.sh:
apt-get update && apt-get install -y cmake libboost-all-dev libtbb-dev
```

---

**Problem:** `Git submodule update failed`

**Solution:** Network timeout or authentication issue. Retry the job or check cluster internet connectivity.

### Runtime Issues

**Problem:** `Out of memory` error during benchmark

**Solution:**
1. Reduce scale factors in [playground.cpp](../src/bin/playground.cpp) line 1095:
   ```cpp
   const auto scale_factors = std::vector<float>{1.0f, 2.0f, 4.0f};
   ```
2. Or increase memory allocation in SLURM script:
   ```bash
   #SBATCH --mem=192G
   ```

---

**Problem:** `FAILED VALIDATION!` in output

**Solution:** Ensure `run_single_baseline = true` in [playground.cpp](../src/bin/playground.cpp) PlaygroundConfig. The baseline is required for validation.

---

**Problem:** Job pending with `Resources unavailable`

**Solution:** Cluster is busy. Options:
1. Wait for resources to become available
2. Reduce `--cpus-per-task` for faster scheduling
3. Remove `--exclusive` flag (not recommended - affects measurement stability)

### Container Issues

**Problem:** `pyxis: command not found`

**Solution:** Your cluster may not have pyxis installed. Try Singularity instead:
```bash
# Replace srun --container-image line with:
singularity exec docker://ubuntu:25.04 bash -c "..."
```

Contact your HPC admin for container runtime availability.

---

**Problem:** `Permission denied` when writing files

**Solution:** You have root inside container but not on host. Ensure all output goes to the mounted `/workspace` directory, which maps to your repository.

## Advanced Usage

### Running Quick Tests

Before submitting a 6-hour job, test with smaller configuration:

1. Edit SLURM script:
   ```bash
   #SBATCH --cpus-per-task=4
   #SBATCH --mem=16G
   #SBATCH --time=30:00
   ```

2. Edit [playground.cpp](../src/bin/playground.cpp):
   ```cpp
   // Line 1095
   const auto scale_factors = std::vector<float>{1.0f};

   // Line 52
   uint32_t num_iterations = 3;
   ```

3. Submit and verify it works:
   ```bash
   rm -rf cmake-build-release/  # Force rebuild
   sbatch slurm/run_benchmark.slurm
   ```

4. Once verified, restore original settings and submit production job.

### Running Multiple Configurations

To compare different settings (e.g., hash vs sort, different worker counts):

```bash
# Option 1: Edit playground.cpp, commit, and submit
git add src/bin/playground.cpp
git commit -m "Configure for hash aggregation test"
sbatch slurm/run_benchmark.slurm

# Option 2: Create multiple SLURM scripts
cp slurm/run_benchmark.slurm slurm/run_hash_8workers.slurm
# Edit the copy with different configurations
sbatch slurm/run_hash_8workers.slurm
```

### Analyzing Results

The benchmark outputs execution times in milliseconds. Example output format:
```
Execution time: 3984 ms (single-threaded baseline)
Execution time: 338 ms (multi-threaded optimized)
Speedup: 11.8x
```

To extract timing data:
```bash
grep "Execution time" results/benchmark_12345.log
```

To find validation status:
```bash
grep "VALIDATED\|FAILED" results/benchmark_12345.log
```

## Support

For issues with:
- **Hyrise benchmark code**: See [src/bin/README_DPMH.md](../src/bin/README_DPMH.md)
- **Build system**: See [CLAUDE.md](../CLAUDE.md)
- **HPC cluster**: Contact your cluster administrator
- **SLURM configuration**: Check official SLURM documentation or your cluster's user guide

## File Structure

```
slurm/
├── README.md              # This file
└── run_benchmark.slurm    # Main SLURM batch script

results/                   # Created by user, gitignored
├── benchmark_JOBID.log    # Standard output
└── benchmark_JOBID.err    # Error log

cmake-build-release/       # Created by build, gitignored
└── hyrisePlayground       # Compiled binary
```
