#pragma once

namespace hyrise {

/**
 * MigrationPolicy is used to determine whether a page should be migrated to DRAM/NUMA or not using a random number generator.
 * The idea is taken from Spitfire: https://github.com/zxjcarrot/spitfire/blob/bab7c28bdf1d671e61931c251be2591401ece365/include/buf/buf_mgr.h#L504
*/
struct MigrationPolicy {
 public:
  explicit MigrationPolicy(double dram_read_ratio, double dram_write_ratio, double numa_read_ratio,
                           double numa_write_ratio);

  bool bypass_dram_during_read();

  bool bypass_dram_during_write();

  bool bypass_numa_during_read();

  bool bypass_numa_during_write();

 private:
  double random();

  double _dram_read_ratio;
  double _dram_write_ratio;
  double _numa_read_ratio;
  double _numa_write_ratio;
};

// LazyMigrationPolicy is good for large-working sets that do not fit in-memory
class LazyMigrationPolicy : public MigrationPolicy {
 public:
  LazyMigrationPolicy() : MigrationPolicy(0.01, 0.01, 0.2, 1) {}
};

// EagerMigrationPolicy is good for small-working sets that fit in-memory as we are trying to fit as much into DRAM as possible
class EagerMigrationPolicy : public MigrationPolicy {
 public:
  EagerMigrationPolicy() : MigrationPolicy(1, 1, 1, 1) {}
};

}  // namespace hyrise
