#pragma once

#include "aggregate_hashsort_utils.hpp"
#include "static_hash_map.hpp"

#define VERBOSE_ALGO VERBOSE && 0

namespace opossum {

namespace aggregate_hashsort {

/**
 * Radix partition the data from `run_source` by hash values via `radix_fan_out` into `partitions`
 *
 * @param remaining_row_count   Number of groups to partition
 */
template <typename Run>
void partition(const AggregateHashSortEnvironment& environment, size_t partition_group_count,
               RunReader<Run>& run_reader, const RadixFanOut& radix_fan_out, std::vector<Partition<Run>>& partitions,
               const size_t level) {
  DebugAssert(radix_fan_out.partition_count == partitions.size(), "Partition count mismatch");
  DebugAssert(partition_group_count > 0, "partition() shouldn't have been called");

  // Pre-allocate target runs
  auto run_per_partition = std::vector<Run>{};
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    const auto [group_capacity, group_data_capacity] = next_run_size(run_reader, partition_group_count, radix_fan_out);
    auto run = create_run<Run>(environment, group_capacity, group_data_capacity);
    run_per_partition.emplace_back(std::move(run));
  }

#if VERBOSE_ALGO
  std::cout << indent(level) << "partition() processing " << partition_group_count << " elements"
            << "\n";
  Timer t;
#endif

  while (!run_reader.end_of_reader() && partition_group_count > 0) {
    const auto& source_run = run_reader.current_run();

    while (!run_reader.end_of_run() && partition_group_count > 0) {
      const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.hashes[run_reader.run_offset]);

      auto& target_run = run_per_partition[partition_idx];

      target_run.append(source_run, run_reader.run_offset, environment.config.buffer_flush_threshold);

      --partition_group_count;
      run_reader.next_group_in_run();
    }

    for (auto& run : run_per_partition) {
      run.flush_buffers(source_run);
    }

    if (run_reader.end_of_run()) {
      run_reader.next_run();
    }
  }

  // Add non-empty Runs to the output partitions
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    auto& run = run_per_partition[partition_idx];
    if (run.size == 0) {
      continue;
    }

    run.shrink_to_size();

    partitions[partition_idx].runs.emplace_back(std::move(run));
  }

#if VERBOSE_ALGO
  std::cout << indent(level) << "partition(): took " << t.lap_formatted() << "\n";
#endif

  DebugAssert(partition_group_count == 0, "Invalid `remaining_row_count` parameter passed");
}

/**
 * @return  {Whether to continue hashing, Number of input groups processed}
 */
template <typename Run>
std::pair<bool, size_t> hashing(const AggregateHashSortEnvironment& environment, const size_t hash_table_size,
                                RunReader<Run>& run_reader, const RadixFanOut& radix_fan_out,
                                std::vector<Partition<Run>>& partitions, const size_t level) {
  auto run_per_partition = std::vector<Run>{};
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    const auto estimated_group_count =
        static_cast<size_t>(std::ceil(hash_table_size * environment.config.hash_table_max_load_factor));
    const auto [group_capacity, group_data_capacity] = next_run_size(run_reader, estimated_group_count, radix_fan_out);
    auto run = create_run<Run>(environment, group_capacity, std::max(group_data_capacity, group_data_capacity));
    run.is_aggregated = true;
    run_per_partition.emplace_back(std::move(run));
  }

  auto group_key_counter_per_partition = std::vector<size_t>(radix_fan_out.partition_count);

#if VERBOSE_ALGO
  Timer t;
#endif

  using HashTableKey = typename Run::HashTableKey;

  struct Hasher {
    mutable size_t hash_counter{};

    size_t operator()(const HashTableKey& key) const {
#if VERBOSE_ALGO
      ++hash_counter;
#endif
      return key.hash;
    }
  };

  const auto hasher = Hasher{};

#ifdef USE_UNORDERED_MAP
  auto compare_fn = [&](const auto& lhs, const auto& rhs) {
#if VERBOSE_ALGO
    ++compare_counter;
#endif
    return typename GroupRun::HashTableCompare{run_source.layout}(lhs, rhs);
  };

  auto hash_table =
      std::unordered_map<typename GroupRun::HashTableKey, size_t, decltype(hash_fn), decltype(compare_fn)>{
          hash_table_size, hash_fn, compare_fn};
#endif

#ifdef USE_DENSE_HASH_MAP
  using HashTableCompare = typename Run::HashTableCompare;

  const auto compare = HashTableCompare{environment};

  auto hash_table = google::dense_hash_map<HashTableKey, size_t, Hasher, HashTableCompare>{
      static_cast<size_t>(hash_table_size), hasher, compare};
#if VERBOSE_ALGO
  Timer timer_set_empty_key;
#endif
  hash_table.set_empty_key(HashTableKey::EMPTY_KEY);
  hash_table.min_load_factor(0.0f);
#endif

#ifdef USE_STATIC_HASH_MAP
  using HashTableCompare = typename Run::HashTableCompare;

  const auto compare = HashTableCompare{environment};

  auto hash_table = StaticHashMap<HashTableKey, size_t, Hasher, HashTableCompare>{
      ceil_power_of_two(static_cast<size_t>(hash_table_size)), hasher, compare};
#if VERBOSE_ALGO
   Timer timer_set_empty_key;
#endif
#endif

#if VERBOSE_ALGO
  std::cout << indent(level) << "hashing(): requested hash_table_size: " << hash_table_size
              << "; actual hash table size: " << hash_table.bucket_count() << "\n";
#endif

#ifdef USE_DENSE_HASH_MAP
  hash_table.max_load_factor(1.0f);
#endif
#ifdef USE_UNORDERED_MAP
  hash_table.max_load_factor(1.0f);
#endif

  auto hash_table_full = false;
  auto group_counter = size_t{0};

  while (!run_reader.end_of_reader() && !hash_table_full) {
    const auto& source_run = run_reader.current_run();

    while (!run_reader.end_of_run() && !hash_table_full) {
      const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.hashes[run_reader.run_offset]);

      auto& target_run = run_per_partition[partition_idx];

      const auto key = source_run.make_key(run_reader.run_offset);
#if VERBOSE_ALGO >= 2
      std::cout << indent(level) << "hashing() Key: " << key << ": " << std::flush;
#endif

      auto hash_table_iter = hash_table.find(key);

      if (hash_table_iter == hash_table.end()) {
        auto& group_key_counter = group_key_counter_per_partition[partition_idx];
#if VERBOSE_ALGO >= 2
        std::cout << indent(level) << "hashing() Append " << group_key_counter << std::endl;
#endif
        hash_table.insert({key, group_key_counter});
        target_run.append(source_run, run_reader.run_offset, environment.config.buffer_flush_threshold);
        ++group_key_counter;
        ++group_counter;

        if (hash_table.load_factor() >= environment.config.hash_table_max_load_factor) {
          hash_table_full = true;
        }
      } else {
#if VERBOSE_ALGO >= 2
        std::cout << indent(level) << "hashing() Aggregate " << hash_table_iter->second << std::endl;
#endif
        target_run.aggregate(hash_table_iter->second, source_run, run_reader.run_offset,
                             environment.config.buffer_flush_threshold);
        ++group_counter;
      }

      run_reader.next_group_in_run();
    }

    for (auto& run : run_per_partition) {
      run.flush_buffers(source_run);
    }

    if (run_reader.end_of_run()) {
      run_reader.next_run();
    }
  }

  // Add non-empty Runs to the output
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    auto& run = run_per_partition[partition_idx];
    if (run.size == 0) {
      continue;
    }

    run.shrink_to_size();

    partitions[partition_idx].runs.emplace_back(std::move(run));
  }

  const auto continue_hashing =
      static_cast<float>(group_counter) / hash_table.size() >= environment.config.continue_hashing_density_threshold;
#if VERBOSE_ALGO
  std::cout << indent(level) << "hashing(): processed " << group_counter << " elements into " << hash_table.size()
            << " groups in " << t.lap_formatted() << "; hash_counter: " << hash_table.hash_function().hash_counter
            << "; compare_counter: " << hash_table.key_eq().counter << "; load_factor: " << hash_table.load_factor()
            << "; continue_hashing: " << continue_hashing << "\n";
#endif

  return {continue_hashing, group_counter};
}

template <typename Run>
std::vector<Partition<Run>> adaptive_hashing_and_partition(const AggregateHashSortEnvironment& environment,
                                                           std::vector<Run> runs, const RadixFanOut& radix_fan_out,
                                                           const size_t level) {
  auto run_reader = RunReader{std::move(runs)};

#if VERBOSE_ALGO
  Timer t;
#endif

  // Start with a single partition and expand to `radix_fan_out.partition_count` partitions if `hashing()` detects a too
  // low density of groups and determines the switch to HashSortMode::RadixFanOut
  auto partitions = std::vector<Partition<Run>>{radix_fan_out.partition_count};
  auto mode = HashSortMode::Hashing;

  /**
   * Initial hashing pass without simultaneous partitioning
   */
  {
#if VERBOSE_ALGO
    std::cout << indent(level) << "adaptive_hashing_and_partition() Rows: " << run_reader.remaining_group_count << "\n";
#endif
    const auto hash_table_size = configure_hash_table(environment, run_reader.remaining_group_count);
    const auto [continue_hashing, hashing_row_count] =
        hashing(environment, hash_table_size, run_reader, RadixFanOut{1, 0, 0}, partitions, level);
    if (!continue_hashing) {
      mode = HashSortMode::Partition;
    }

    // The initial hashing() pass didn't process all data, so in order to progress with the main loop below we need to
    // partition its output
    if (run_reader.remaining_group_count > 0) {
      const auto initial_fan_out_row_count = partitions.front().size();
      auto partitioning_run_reader = RunReader{std::move(partitions.front().runs)};
      partitions = std::vector<Partition<Run>>{radix_fan_out.partition_count};
      partition(environment, initial_fan_out_row_count, partitioning_run_reader, radix_fan_out, partitions, level);
    }
  }

#if VERBOSE_ALGO
  std::cout << indent(level) << "adaptive_hashing_and_partition() Main Loop\n";
#endif

  /**
   * Main loop of alternating hashing (with simultaneous partitioning) and partitioning steps
   */
  while (!run_reader.end_of_reader()) {
#if VERBOSE_ALGO
    std::cout << indent(level) << "adaptive_hashing_and_partition() Rows: " << run_reader.remaining_group_count
              << ", Bytes: " << run_reader.remaining_group_data_size << "\n";
#endif

    if (mode == HashSortMode::Hashing) {
      const auto hash_table_size = configure_hash_table(environment, run_reader.remaining_group_count);
      const auto [continue_hashing, hashing_row_count] =
          hashing(environment, hash_table_size, run_reader, radix_fan_out, partitions, level);
      if (!continue_hashing) {
        mode = HashSortMode::Partition;
      }

    } else {
      const auto partition_row_count =
          std::min(environment.config.max_partitioning_counter, run_reader.remaining_group_count);
      partition(environment, partition_row_count, run_reader, radix_fan_out, partitions, level);
      mode = HashSortMode::Hashing;
    }
  }

#if VERBOSE_ALGO
  std::cout << indent(level) << "adaptive_hashing_and_partition() took " << t.lap_formatted() << "\n";
#endif

  return partitions;
}

}  // namespace aggregate_hashsort

}  // namespace opossum