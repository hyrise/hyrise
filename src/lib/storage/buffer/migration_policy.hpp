#pragma once

namespace hyrise {

/**
 * MigrationPolicy is used to determine whether a page should be migrated to DRAM/NUMA or not using a random number generator.
 * The idea is taken from Spitfire: https://github.com/zxjcarrot/spitfire/blob/bab7c28bdf1d671e61931c251be2591401ece365/include/buf/buf_mgr.h#L504
 * 
 * Use the respective bypass function to determine whether a page should be migrated or not.
*/
struct MigrationPolicy {
 public:
  explicit MigrationPolicy(const double dram_read_ratio, const double dram_write_ratio, const double numa_read_ratio,
                           const double numa_write_ratio);

  // Returns true if the migration should bypass DRAM during reads
  bool bypass_dram_during_read() const;

  // Returns true if the migration should bypass DRAM during writes
  bool bypass_dram_during_write() const;

  // Returns true if the migration should bypass NUMA during reads
  bool bypass_numa_during_read() const;

  //  Returns true if the migration should bypass NUMA during writes
  bool bypass_numa_during_write() const;

  //  Returns the DRAM read ratio that is used for bypass_dram_during_read()
  double get_dram_read_ratio() const;

  //  Returns the DRAM write ratio for bypass_dram_during_write()
  double get_dram_write_ratio() const;

  //  Returns the NUMA read ratio for bypass_numa_during_read()
  double get_numa_read_ratio() const;

  //  Returns the NUMA write ratio for bypass_numa_during_write()
  double get_numa_write_ratio() const;

 private:
  double _dram_read_ratio;
  double _dram_write_ratio;
  double _numa_read_ratio;
  double _numa_write_ratio;

  double random() const;
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
