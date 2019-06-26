#pragma once

#include "aggregate_hashsort_utils.hpp"

namespace opossum {

namespace aggregate_hashsort {

/**
 * Radix partition the data from `run_source` by hash values via `radix_fan_out` into `partitions`
 *
 * @param remaining_row_count   Number of groups to partition
 */
template <typename Run>
void partition(const AggregateHashSortSetup& setup, size_t remaining_row_count,
               const std::shared_ptr<AbstractRunSource<Run>>& run_source,
               const RadixFanOut& radix_fan_out,
               std::vector<Partition<Run>>& partitions,
               const size_t level) {
  DebugAssert(radix_fan_out.partition_count == partitions.size(), "Partition count mismatch");
  DebugAssert(remaining_row_count > 0, "partition() shouldn't have been called");

  run_source->prefetch(setup);

  // Pre-allocate target runs
  auto run_per_partition = std::vector<Run>{};
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    const auto [group_capacity, group_data_capacity] = next_run_size(*run_source,remaining_row_count, radix_fan_out);
    auto run = create_run<Run>(setup, group_capacity, group_data_capacity);
    run_per_partition.emplace_back(std::move(run));
  }

#if VERBOSE
  std::cout << indent(level) << "partition() processing " << remaining_row_count << " elements"
            << "\n";
  Timer t;
#endif

  while (!run_source->end_of_source() && remaining_row_count > 0) {
    if (run_source->end_of_run()) {
#if VERBOSE
      Timer t2;
#endif
      run_source->next_run(setup);
#if VERBOSE
      std::cout << indent(level) << "partition(): next_run() of " << run_source->runs.back().size << " rows took "
                << t2.lap_formatted() << "\n";
#endif
    }

    const auto& source_run = run_source->current_run();

    while (!run_source->end_of_run() && remaining_row_count > 0) {
      const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.hashes[run_source->run_offset]);

      auto& target_run = run_per_partition[partition_idx];

      target_run.append(source_run, run_source->run_offset, setup.config.buffer_flush_threshold);

      --remaining_row_count;
      run_source->next_group_in_run();
    }

    for (auto& run : run_per_partition) {
      run.flush_buffers(source_run);
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

#if VERBOSE
  std::cout << indent(level) << "partition(): took " << t.lap_formatted() << "\n";
#endif

  DebugAssert(remaining_row_count == 0, "Invalid `remaining_row_count` parameter passed");
}

/**
 * @return  {Whether to continue hashing, Number of input groups processed}
 */
template <typename Run>
std::pair<bool, size_t> hashing(const AggregateHashSortSetup& setup, const size_t hash_table_size,
                                const std::shared_ptr<AbstractRunSource<Run>>& run_source,
                                const RadixFanOut& radix_fan_out,
                                std::vector<Partition<Run>>& partitions,
                                const size_t level) {
  run_source->prefetch(setup);

  auto run_per_partition = std::vector<Run>{};
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    const auto estimated_group_count = static_cast<size_t>(std::ceil(hash_table_size * setup.config.hash_table_max_load_factor));
    const auto[group_capacity, group_data_capacity] = next_run_size(*run_source, estimated_group_count, radix_fan_out);
    auto run = create_run<Run>(setup, group_capacity, std::max(group_data_capacity, group_data_capacity));
    run.is_aggregated = true;
    run_per_partition.emplace_back(std::move(run));
  }

  auto group_key_counter_per_partition = std::vector<size_t>(radix_fan_out.partition_count);

#if VERBOSE
  Timer t;
#endif

  using HashTableKey = typename Run::HashTableKey;

  struct Hasher {
    mutable size_t hash_counter{};

    size_t operator()(const HashTableKey& key) const {
#if VERBOSE
      ++hash_counter;
#endif
      return key.hash;
    }
  };

  const auto hasher = Hasher{};

#ifdef USE_UNORDERED_MAP
  auto compare_fn = [&](const auto& lhs, const auto& rhs) {
#if VERBOSE
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

  const auto compare = HashTableCompare{setup};

  auto hash_table = google::dense_hash_map<HashTableKey, size_t, Hasher, HashTableCompare>{
      static_cast<size_t>(hash_table_size), hasher, compare};
#if VERBOSE
  Timer timer_set_empty_key;
#endif
  hash_table.set_empty_key(HashTableKey::EMPTY_KEY);
  hash_table.min_load_factor(0.0f);
#endif

#if VERBOSE
  std::cout << indent(level) << "hashing(): requested hash_table_size: " << hash_table_size <<
  "; actual hash table size: " << hash_table.bucket_count() << "\n";
#endif

  hash_table.max_load_factor(1.0f);

  auto hash_table_full = false;
  auto group_counter = size_t{0};

  while(!run_source->end_of_source() && !hash_table_full) {
    if (run_source->end_of_run()) {
      run_source->next_run(setup);
    }

    const auto &source_run = run_source->current_run();

    while (!run_source->end_of_run() && !hash_table_full) {
      const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.hashes[run_source->run_offset]);

      auto &target_run = run_per_partition[partition_idx];

      const auto key = source_run.make_key(run_source->run_offset);
#if VERBOSE >= 2
      std::cout << indent(level) << "hashing() Key: " << key << ": " << std::flush;
#endif

      auto hash_table_iter = hash_table.find(key);

      if (hash_table_iter == hash_table.end()) {

        auto &group_key_counter = group_key_counter_per_partition[partition_idx];
#if VERBOSE >= 2
        std::cout << indent(level) << "hashing() Append " << group_key_counter << std::endl;
#endif
        hash_table.insert({key, group_key_counter});
        target_run.append(source_run, run_source->run_offset, setup.config.buffer_flush_threshold);
        ++group_key_counter;
        ++group_counter;

        if (hash_table.load_factor() >= setup.config.hash_table_max_load_factor) {
          hash_table_full = true;
        }
      } else {
#if VERBOSE >= 2
       std::cout << indent(level) << "hashing() Aggregate " <<hash_table_iter->second << std::endl;
#endif
        target_run.aggregate(hash_table_iter->second, source_run, run_source->run_offset, setup.config.buffer_flush_threshold);
        ++group_counter;
      }

      run_source->next_group_in_run();
    }

    for (auto& run : run_per_partition) {
      run.flush_buffers(source_run);
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

  const auto continue_hashing = static_cast<float>(group_counter) / hash_table.size() >= setup.config.continue_hashing_density_threshold;
#if VERBOSE
  std::cout << indent(level) << "hashing(): processed " << group_counter << " elements into " << hash_table.size() <<
  " groups in " << t.lap_formatted() << "; hash_counter: "<<hash_table.hash_function().hash_counter << "; compare_counter: " << hash_table.key_eq().counter <<
  "; load_factor: " << hash_table.load_factor() << "; continue_hashing: " << continue_hashing << "\n";
#endif
  
  return {continue_hashing, group_counter};
}

template <typename Run>
std::vector<Partition<Run>> adaptive_hashing_and_partition(const AggregateHashSortSetup& setup,
                                                           const std::shared_ptr<AbstractRunSource<Run>>& run_source,
                                                           const RadixFanOut& radix_fan_out, const size_t level) {

#if VERBOSE
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
#if VERBOSE
    std::cout << indent(level) << "adaptive_hashing_and_partition() Rows: " << run_source->total_remaining_group_count << ", Fetched: " << run_source->remaining_fetched_group_count << " rows, " << run_source->remaining_fetched_group_data_size << " elements" << "\n";
#endif
    const auto hash_table_size = configure_hash_table(setup, run_source->total_remaining_group_count);
    const auto[continue_hashing, hashing_row_count] =
    hashing(setup, hash_table_size, run_source, RadixFanOut{1, 0, 0}, partitions, level);
    if (!continue_hashing) {
      mode = HashSortMode::Partition;
    }

    // The initial hashing() pass didn't process all data, so in order to progress with the main loop below we need to
    // partition its output
    if (!run_source->end_of_source()) {
      const auto initial_fan_out_row_count = partitions.front().size();
      auto initial_fan_out_source =
      std::static_pointer_cast<AbstractRunSource<Run>>(std::make_shared<PartitionRunSource<Run>>
      (std::move(partitions.front().runs)));
      partitions = std::vector<Partition<Run>>{radix_fan_out.partition_count};
      partition(setup, initial_fan_out_row_count, initial_fan_out_source, radix_fan_out, partitions, level);
    }
  }

#if VERBOSE
  std::cout << indent(level) << "adaptive_hashing_and_partition() Main Loop\n";
#endif

  /**
   * Main loop of alternating hashing (with simultaneous partitioning) and partitioning steps
   */
  while (!run_source->end_of_source()) {
#if VERBOSE
    std::cout << indent(level) << "adaptive_hashing_and_partition() Rows: " << run_source->total_remaining_group_count << ", Fetched: " << run_source->remaining_fetched_group_count << " rows, " << run_source->remaining_fetched_group_data_size << " elements" << "\n";
#endif

    DebugAssert(run_source->total_remaining_group_count >= run_source->remaining_fetched_group_count, "Bug detected");


    if (mode == HashSortMode::Hashing) {
      const auto hash_table_size = configure_hash_table(setup, run_source->total_remaining_group_count);
      const auto [continue_hashing, hashing_row_count] =
      hashing(setup, hash_table_size, run_source, radix_fan_out, partitions, level);
      if (!continue_hashing) {
        mode = HashSortMode::Partition;
      }

    } else {
      const auto partition_row_count = std::min(setup.config.max_partitioning_counter, run_source->total_remaining_group_count);
      partition(setup, partition_row_count, run_source, radix_fan_out, partitions, level);
      mode = HashSortMode::Hashing;
    }
  }

#if VERBOSE
  std::cout << indent(level) << "adaptive_hashing_and_partition() took " << t.lap_formatted() << "\n";
#endif

  return partitions;
}

template <typename Run>
std::vector<Run> aggregate(const AggregateHashSortSetup& setup,
                           const std::shared_ptr<AbstractRunSource<Run>>& run_source, const size_t level) {
  if (run_source->end_of_source()) {
    return {};
  }

#if VERBOSE
  std::cout << indent(level) << "aggregate() at level " << level << " - ";

  if (const auto* table_run_source = dynamic_cast<TableRunSource<Run>*>(run_source.get())) {
    std::cout << "Table: " << table_run_source->table->row_count() << " rows";
  } else if (const auto* partition_run_source = dynamic_cast<PartitionRunSource<Run>*>(run_source.get())) {
    auto rows = size_t{0};
    for (const auto& run : partition_run_source->runs) {
      rows += run.size;
    }

    std::cout << partition_run_source->runs.size() << " runs; " << rows << " rows";
  } else {
    Fail("");
  }

  std::cout << "\n";
#endif

  auto output_runs = std::vector<Run>{};

  if (run_source->runs.size() == 1 && run_source->runs.front().is_aggregated) {
#if VERBOSE
    std::cout << indent(level) << " Single run in partition is aggregated, our job here is done"
              << "\n";
#endif
    output_runs.emplace_back(std::move(run_source->runs.front()));
    return output_runs;
  }

  const auto radix_fan_out = RadixFanOut::for_level(level);

  auto partitions = adaptive_hashing_and_partition(setup, run_source, radix_fan_out, level);

  for (auto& partition : partitions) {
    if (partition.size() == 0) {
      continue;
    }

    const auto partition_run_source = std::static_pointer_cast<AbstractRunSource<Run>>(std::make_shared<PartitionRunSource<Run>>(std::move(partition.runs)));

    auto aggregated_partition = aggregate(setup, partition_run_source, level + 1);

    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()),
                       std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

}  // namespace aggregate_hashsort

}  // namespace opossum