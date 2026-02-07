#!/bin/bash
#SBATCH --job-name=hyrise-benchmark
#SBATCH --account=UPDATE_THIS_ACCOUNT
#SBATCH --mail-type=ALL
#SBATCH --mail-user=slack:<UPDATE_THIS_ACCOUNT>
#SBATCH --partition=cpu-batch
#SBATCH --constraint=ARCH:X86
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=7:00:00
#SBATCH --exclusive
#SBATCH --output=slurm-benchmark-%j.log

# ============================================
# Hyrise Playground Benchmark - SLURM Script
# ============================================
# Generic script for running Hyrise benchmarks on HPC cluster
# Uses containerized environment with pyxis for dependency isolation
#
# Usage:
#   1. Set SLURM_ACCOUNT: export SLURM_ACCOUNT=your-account
#   2. Or edit line 3 above: #SBATCH --account=your-account
#   3. Configure playground.cpp for benchmark settings
#   4. Submit: sbatch slurm/run_benchmark.slurm
#
# Prerequisites:
#   - HPC cluster with pyxis/enroot container support
#   - Repository cloned to cluster
#   - results/ directory created (mkdir -p results)
# ============================================

set -e  # Exit on error

# ============================================
# CONFIGURATION
# ============================================
REPO_DIR="${PWD}"
BUILD_DIR="cmake-build-release"
CONTAINER_IMAGE="ubuntu:25.04"
NUM_WORKERS="${SLURM_CPUS_PER_TASK}"

# ============================================
# JOB INFORMATION
# ============================================
echo "=========================================="
echo "Hyrise Playground Benchmark"
echo "=========================================="
echo "Job ID:       ${SLURM_JOB_ID}"
echo "Node:         ${SLURM_NODELIST}"
echo "CPUs:         ${SLURM_CPUS_PER_TASK}"
echo "Memory:       ${SLURM_MEM_PER_NODE}M"
echo "Time Limit:   ${SLURM_TIMELIMIT}"
echo "Account:      ${SLURM_JOB_ACCOUNT}"
echo "Partition:    ${SLURM_JOB_PARTITION}"
echo "Start Time:   $(date '+%Y-%m-%d %H:%M:%S')"
echo "Repository:   ${REPO_DIR}"
echo "Workers:      ${NUM_WORKERS}"
echo "=========================================="
echo ""

# ============================================
# CONTAINER EXECUTION
# ============================================
srun --container-image="${CONTAINER_IMAGE}" \
     --container-mounts="${REPO_DIR}:/workspace" \
     --container-workdir=/workspace \
     bash -c "
set -e  # Exit on error inside container

echo '=========================================='
echo 'Container Setup'
echo '=========================================='
echo 'Container Image: ${CONTAINER_IMAGE}'
echo 'Container User:  $(whoami) (UID: $(id -u))'
echo 'Working Dir:     $(pwd)'
echo ''

# ============================================
# STEP 1: Install Dependencies
# ============================================
if [ ! -f ${BUILD_DIR}/hyrisePlayground ]; then
    echo '=========================================='
    echo 'Step 1: Installing Dependencies'
    echo '=========================================='

    # Ensure sudo is available (Ubuntu 25.04 base may not include it)
    if ! command -v sudo &> /dev/null; then
        echo 'Installing sudo...'
        apt-get update -qq
        apt-get install -y sudo
        echo 'sudo installed successfully'
    fi

    # Set environment for non-interactive installation
    export HYRISE_HEADLESS_SETUP=true
    export DEBIAN_FRONTEND=noninteractive

    # Run dependency installation script
    echo 'Running install_dependencies.sh...'
    if ! ./install_dependencies.sh; then
        echo 'ERROR: Dependency installation failed!'
        exit 1
    fi

    echo 'Dependencies installed successfully'
    echo ''
fi

# ============================================
# STEP 2: Build Hyrise Playground
# ============================================
if [ ! -f ${BUILD_DIR}/hyrisePlayground ]; then
    echo '=========================================='
    echo 'Step 2: Building Hyrise Playground'
    echo '=========================================='
    echo 'Build Type:      Release'
    echo 'Compiler:        clang++-20'
    echo 'Parallel Jobs:   ${NUM_WORKERS}'
    echo 'Build Directory: ${BUILD_DIR}'
    echo ''

    # Create build directory
    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}

    # Configure with CMake
    echo 'Configuring CMake...'
    if ! cmake -DCMAKE_BUILD_TYPE=Release \
               -DCMAKE_C_COMPILER=clang-20 \
               -DCMAKE_CXX_COMPILER=clang++-20 \
               ..; then
        echo 'ERROR: CMake configuration failed!'
        exit 1
    fi

    # Build hyrisePlayground target
    echo 'Building hyrisePlayground...'
    if ! make -j${NUM_WORKERS} hyrisePlayground; then
        echo 'ERROR: Build failed!'
        exit 1
    fi

    echo 'Build completed successfully'
    echo ''

    cd /workspace
else
    echo '=========================================='
    echo 'Step 2: Using Pre-built Binary'
    echo '=========================================='
    echo 'Binary found at: ${BUILD_DIR}/hyrisePlayground'
    echo 'Skipping build step'
    echo ''
fi

# ============================================
# STEP 3: Run Benchmark
# ============================================
echo '=========================================='
echo 'Step 3: Running Benchmark'
echo '=========================================='
echo 'Benchmark Config (from playground.cpp):'
echo '  - Workers: Set in PlaygroundConfig.num_workers'
echo '  - Iterations: Set in PlaygroundConfig.num_iterations'
echo '  - Algorithm: Hash or Sort (see main() function)'
echo '  - Scale Factors: See scale_factors vector'
echo ''
echo 'Setting CPU affinity for stable measurements...'

# Set CPU affinity environment
export OMP_NUM_THREADS=${NUM_WORKERS}
export OMP_PROC_BIND=close
export OMP_PLACES=cores
export HYRISE_HEADLESS_SETUP=true

# Display CPU topology info
echo 'CPU Configuration:'
echo '  OMP_NUM_THREADS: '${OMP_NUM_THREADS}
echo '  OMP_PROC_BIND:   '${OMP_PROC_BIND}
echo '  OMP_PLACES:      '${OMP_PLACES}
echo ''

echo '=========================================='
echo 'Benchmark Starting...'
echo '=========================================='
echo ''

# Run the benchmark
cd ${BUILD_DIR}
if ! ./hyrisePlayground; then
    echo ''
    echo 'ERROR: Benchmark execution failed!'
    exit 1
fi

echo ''
echo '=========================================='
echo 'Benchmark Complete!'
echo '=========================================='
echo 'End Time: \$(date \"+%Y-%m-%d %H:%M:%S\")'
echo 'Results logged to: results/benchmark_${SLURM_JOB_ID}.log'
echo '=========================================='
"

# ============================================
# JOB COMPLETION
# ============================================
EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Job Summary"
echo "=========================================="
echo "Exit Code:    ${EXIT_CODE}"
echo "End Time:     $(date '+%Y-%m-%d %H:%M:%S')"
if [ ${EXIT_CODE} -eq 0 ]; then
    echo "Status:       SUCCESS"
else
    echo "Status:       FAILED"
fi
echo "=========================================="

exit ${EXIT_CODE}
