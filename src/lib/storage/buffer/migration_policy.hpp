#pragma

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

  bool bypass_numa_during_write();

  bool bypass_numa_during_write();

 private:
  double random();
  double _dram_read_ratio;
  double _dram_write_ratio;
  double _numa_read_ratio;
  double _numa_write_ratio;
};

// Default policies are taken from the Spitfire paper
const auto LazyMigrationPolicy = MigrationPolicy(0.01, 0.01, 0.2, 1);
const auto EagerMigrationPolicy = MigrationPolicy(1, 1, 1, 1);

}  // namespace hyrise
