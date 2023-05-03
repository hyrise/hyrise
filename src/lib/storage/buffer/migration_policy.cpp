#include "storage/buffer/migration_policy.hpp"
#include <random>

namespace hyrise {

MigrationPolicy::MigrationPolicy(const double dram_read_ratio, const double dram_write_ratio,
                                 const double numa_read_ratio, const double numa_write_ratio,
                                 const std::optional<uint32_t> seed)
    : _dram_read_ratio(dram_read_ratio),
      _dram_write_ratio(dram_write_ratio),
      _numa_read_ratio(numa_read_ratio),
      _numa_write_ratio(numa_write_ratio),
      _seed(seed) {}

bool MigrationPolicy::bypass_dram_during_read() const {
  const auto rand = random();
  return rand > 0 ? rand > _dram_read_ratio : true;
}

bool MigrationPolicy::bypass_dram_during_write() const {
  const auto rand = random();
  return rand > 0 ? rand > _dram_write_ratio : true;
}

bool MigrationPolicy::bypass_numa_during_read() const {
  const auto rand = random();
  return rand > 0 ? rand > _numa_read_ratio : true;
}

bool MigrationPolicy::bypass_numa_during_write() const {
  const auto rand = random();
  return rand > 0 ? rand > _numa_write_ratio : true;
}

double MigrationPolicy::random() const {
  // std::random_device might not be the best way to initialize the generator, but it's just good enough for us
  static thread_local std::mt19937 generator{_seed ? *_seed : std::random_device{}()};
  std::uniform_int_distribution<int> distribution(0, 100000);
  return (distribution(generator) + 0.0) / 100000;
}

double MigrationPolicy::get_dram_read_ratio() const {
  return _dram_read_ratio;
}

double MigrationPolicy::get_dram_write_ratio() const {
  return _dram_write_ratio;
}

double MigrationPolicy::get_numa_read_ratio() const {
  return _numa_read_ratio;
}

double MigrationPolicy::get_numa_write_ratio() const {
  return _numa_write_ratio;
}

}  // namespace hyrise