#include "storage/buffer/migration_policy.hpp"
#include <random>

namespace hyrise {

MigrationPolicy::MigrationPolicy(double dram_read_ratio, double dram_write_ratio, double numa_read_ratio,
                                 double numa_write_ratio)
    : _dram_read_ratio(dram_read_ratio),
      _dram_write_ratio(dram_write_ratio),
      _numa_read_ratio(numa_read_ratio),
      _numa_write_ratio(numa_write_ratio) {}

bool MigrationPolicy::bypass_dram_during_read() {
  const auto rand = random();
  return rand > 0 ? rand > _dram_read_ratio : true;
}

bool MigrationPolicy::bypass_dram_during_write() {
  const auto rand = random();
  return rand > 0 ? rand > _dram_write_ratio : true;
}

bool MigrationPolicy::bypass_numa_during_read() {
  const auto rand = random();
  return rand > 0 ? rand > _numa_read_ratio : true;
}

bool MigrationPolicy::bypass_numa_during_write() {
  const auto rand = random();
  return rand > 0 ? rand > _numa_write_ratio : true;
}

double MigrationPolicy::random() {
  static thread_local std::mt19937 generator;
  std::uniform_int_distribution<int> distribution(0, 100000);
  return (distribution(generator) + 0.0) / 100000;
}

}  // namespace hyrise