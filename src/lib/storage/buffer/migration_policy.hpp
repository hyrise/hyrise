#pragma once
#include <optional>

namespace hyrise {

/**
 * MigrationPolicy is used to determine whether a page should be migrated to DRAM/NUMA or not using a random number generator.
 * The idea is taken from Spitfire: https://github.com/zxjcarrot/spitfire/blob/bab7c28bdf1d671e61931c251be2591401ece365/include/buf/buf_mgr.h#L504
 * 
 * Use the respective bypass function to determine whether a page should be migrated or not.
 * 
 * TODO: Incorporate the page size into the migration policy
*/
struct MigrationPolicy {
 public:
  constexpr MigrationPolicy(const double dram_read_ratio, const double dram_write_ratio, const double numa_read_ratio,
                            const double numa_write_ratio, const int64_t seed = -1)
      : _dram_read_ratio(dram_read_ratio),
        _dram_write_ratio(dram_write_ratio),
        _numa_read_ratio(numa_read_ratio),
        _numa_write_ratio(numa_write_ratio),
        _seed(seed) {}

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

  double _dram_read_ratio;
  double _dram_write_ratio;
  double _numa_read_ratio;
  double _numa_write_ratio;

  double random() const;

  int64_t _seed;

  bool operator<=>(const MigrationPolicy&) const = default;
};

// LazyMigrationPolicy is good for large-working sets that do not fit in-memory
constexpr auto LazyMigrationPolicy = MigrationPolicy(0.01, 0.01, 0.2, 0.1);

// EagerMigrationPolicy is good for small-working sets that fit in memory as we are trying to work as much with DRAM as possible
constexpr auto EagerMigrationPolicy = MigrationPolicy(1, 1, 1, 1);

// Only enable DRAM
constexpr auto NumaOnlyMigrationPolicy = MigrationPolicy(0, 0, 1, 1);

// Only enanle NUMA
constexpr auto DramOnlyMigrationPolicy = MigrationPolicy(1, 1, 0, 0);

}  // namespace hyrise
