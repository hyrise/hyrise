#pragma once

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
  explicit MigrationPolicy(const double dram_read_ratio, const double dram_write_ratio, const double numa_read_ratio,
                           const double numa_write_ratio, const std::optional<uint32_t> seed = std::nullopt);

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

  std::optional<uint32_t> _seed;
};

// LazyMigrationPolicy is good for large-working sets that do not fit in-memory
class LazyMigrationPolicy : public MigrationPolicy {
 public:
  LazyMigrationPolicy(const std::optional<uint32_t> seed = std::nullopt) : MigrationPolicy(0.01, 0.01, 0.2, 1, seed) {}
};

// EagerMigrationPolicy is good for small-working sets that fit in memory as we are trying to work as much with DRAM as possible
class EagerMigrationPolicy : public MigrationPolicy {
 public:
  EagerMigrationPolicy(const std::optional<uint32_t> seed = std::nullopt) : MigrationPolicy(1, 1, 1, 1, seed) {}
};

}  // namespace hyrise
