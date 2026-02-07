#!/bin/bash
#SBATCH -A <YOUR_ACCOUNT>
#SBATCH --mail-type=ALL
#SBATCH --mail-user=slack:<YOUR_USERNAME>
#SBATCH --partition=cpu-batch
#SBATCH -C ARCH:X86
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=1:00:00
#SBATCH --exclusive
#SBATCH --job-name=hyrise-benchmark
#SBATCH --output=slurm-benchmark-%j.out

set -e

# Hyrise Playground Benchmark
# IMPORTANT: Update REPO_PATH to match your cluster home directory path
# Submit from anywhere: sbatch slurm/run_benchmark.slurm

REPO_PATH=/sc/home/<YOUR_USERNAME>/hyrise
BUILD_DIR=cmake-build-release
CONTAINER_NAME=hyrise-bench-$USER
NUM_CORES=$SLURM_CPUS_PER_TASK

echo "Starting benchmark job"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "CPU Model: $(cat /proc/cpuinfo | grep 'model name' | head -1 | cut -d: -f2 | xargs)"

# Step 1: Set up container with dependencies and build (reuses container across jobs)
echo "Setting up container..."
srun --container-image="ubuntu:25.04" \
     --container-name=$CONTAINER_NAME \
     --container-writable \
     --container-remap-root \
     --container-mounts=$REPO_PATH:/workspace \
     bash -c "
set -e
if [ ! -f /workspace/$BUILD_DIR/hyrisePlayground ]; then
  echo 'Building hyrisePlayground...'
  apt-get update -qq && apt-get install -y -qq sudo make
  cd /workspace
  HYRISE_HEADLESS_SETUP=true DEBIAN_FRONTEND=noninteractive ./install_dependencies.sh
  mkdir -p $BUILD_DIR && cd $BUILD_DIR
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-20 -DCMAKE_CXX_COMPILER=clang++-20 -DENABLE_NUMA_SUPPORT=OFF ..
  make -j$NUM_CORES hyrisePlayground
else
  echo 'Binary found, skipping build'
fi
"

# Step 2: Run benchmark (reuses cached container)
echo "Running benchmark..."
srun --container-name=$CONTAINER_NAME \
     --container-remap-root \
     --container-mounts=$REPO_PATH:/workspace \
     --container-workdir=/workspace/$BUILD_DIR \
     ./hyrisePlayground

echo "Job completed!"
