#include "aggregate_hashsort_steps.hpp"

namespace opossum {

namespace aggregate_hashsort {

/**
 * Radix partition the data from `run_source` by hash values via `radix_fan_out` into `partitions`
 *
 * @param remaining_row_count   Number of groups to partition
 */
template <typename Run>
void partition(const AggregateHashSortDefinition& definition, size_t remaining_row_count,
               const std::shared_ptr<AbstractRunSource<Run>>& run_source,
               const RadixFanOut& radix_fan_out,
               std::vector<Partition<Run>>& partitions,
               const size_t level) {
  if (run_source->end_of_run()) {
    run_source->next_run();
  }

  // Start with one Run per partition
  auto runs_per_partition = std::vector<std::vector<Run>>(radix_fan_out.partition_count);
  for (auto& runs : runs_per_partition) {
    const auto[group_capacity, group_data_capacity] = RunAllocationStrategy{run_source, radix_fan_out}.next_run_size();
    group_data_capacity = std::max(group_data_capacity, run_source->current_run()->hashes[run_source->run_offset]);
    runs.emplace_back(create_run(definition, group_capacity, std::max(group_data_capacity, group_data_capacity)));
  }

  DebugAssert(radix_fan_out.partition_count == partitions.size(), "Partition count mismatch");
  DebugAssert(remaining_row_count > 0, "partition() shouldn't have been called");

#if VERBOSE
  std::cout << indent(level) << "partition() processing " << remaining_row_count << " elements"
            << "\n";
  Timer t;
#endif

  auto done = false;

  while (!run_source->end_of_source()) {
    if (run_source->end_of_run()) {
#if VERBOSE
      Timer t2;
#endif
      run_source->next_run();
#if VERBOSE
      std::cout << indent(level) << "partition(): next_run() of " << run_source->runs.back().size() << " rows took "
                << t2.lap_formatted() << "\n";
#endif
    }

    auto run_allocation_strategy = RunAllocationStrategy{run_source, radix_fan_out};

    const auto& source_run = run_source->current_run();

    while (!run_source->end_of_run()) {
      const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.hashes[run_source->run_offset]);

      auto& partition_runs = runs_per_partition[partition_idx];

      if (!partition_runs.back().can_append(source_run, run_source->run_offset)) {
        partition_runs.back().flush_buffers(source_run);

        const auto[group_capacity, group_data_capacity] = RunAllocationStrategy{run_source, radix_fan_out}.next_run_size();
        group_data_capacity = std::max(group_data_capacity, run_source->current_run()->hashes[run_source->run_offset]);
        partition_runs.emplace_back(create_run(definition, group_capacity, std::max(group_data_capacity, group_data_capacity)));
      }

      partition_runs.back().append(source_run, source_run->run_offset, level, partition_idx);

      --remaining_row_count;

      if (remaining_row_count == 0) {
        done = true;
      } else {
        run_source->next_group();
      }
    }

    for (auto& runs : runs_per_partition) {
      runs.back().flush_buffers(source_run);
    }

    if (done) {
      break;
    }
  }


  // Add non-empty Runs to the output
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    auto& runs = runs_per_partition[partition_idx];
    if (runs.back().empty()) {
      runs.pop_back();
    }

    partitions[partition_idx].runs.insert(partitions[partition_idx].runs.end, std::make_move_iterator(runs.begin()), std::make_move_iterator(runs.end()));
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
std::pair<bool, size_t> hashing(const AggregateHashSortDefinition& definition, const size_t hash_table_size,
                                const float hash_table_max_load_factor,
                                const std::shared_ptr<AbstractRunSource<Run>>& run_source,
                                const RadixFanOut& radix_fan_out,
                                std::vector<Partition<Run>>& partitions,
                                const size_t level) {
  // hashing() processes one input run, max. If there isn't one available we fetch one here, but never later.
  if (run_source->end_of_run()) {
    run_source->next_run();
  }
  
  // Initialize one Run per partition
  auto run_per_partition = std::vector<Run>(radix_fan_out.partition_count);
  for (auto& run : run_per_partition) {
    const auto[group_capacity, group_data_capacity] = RunAllocationStrategy{run_source, radix_fan_out}.next_run_size();
    group_data_capacity = std::max(group_data_capacity, run_source->current_run()->hashes[run_source->run_offset]);    
    run = create_run(definition, group_capacity, std::max(group_data_capacity, group_data_capacity));
    run.is_aggregated = true;
  }

  auto group_key_counter_per_partition = std::vector<size_t>(radix_fan_out.partition_count);

#if VERBOSE
  Timer t;
//  auto hash_counter = size_t{0};
//  auto compare_counter = size_t{0};
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

  const auto compare = HashTableCompare{run_source->layout};

  auto hash_table = google::dense_hash_map<HashTableKey, size_t, Hasher, HashTableCompare>{
      static_cast<size_t>(hash_table_size), hasher, compare};
#if VERBOSE
  Timer timer_set_empty_key;
#endif
  hash_table.set_empty_key(HashTableKey::EMPTY_KEY);
#if VERBOSE
  std::cout << indent(level) << "hashing(): set_empty_key() on hash table of size " << hash_table.bucket_count() << " took " << timer_set_empty_key.lap_formatted() << "\n";
#endif
  hash_table.min_load_factor(0.0f);
#endif

#if VERBOSE
  std::cout << indent(level) << "hashing(): requested hash_table_size: " << hash_table_size <<
  "; actual hash table size: " << hash_table.bucket_count() << "\n";
#endif

  hash_table.max_load_factor(1.0f);

  auto group_counter = size_t{0};
  
  const auto& source_run = run_source->current_run();
  
  while (!run_source->end_of_run()) {
    const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.groups.hashes[run_source->run_offset]);

    auto& target_run = run_per_partition[partition_idx];

    const auto key = source_run.groups.make_key(run_source->run_offset);

    auto hash_table_iter = hash_table.find(key);

    if (hash_table_iter == hash_table.end()) {
      if (!target_run.can_append(source_run, run_source->run_offset)) {
        break;
      }
      
      auto& group_key_counter = group_key_counter_per_partition[partition_idx];
      hash_table.insert({key, group_key_counter});
      target_run.append(source_run, run_source->run_offset, level, partition_idx);
      ++group_key_counter;
      ++group_counter;
      
      if (hash_table.load_factor() >= hash_table_max_load_factor) {
        break;
      }
    } else {
      target_run.aggregate(hash_table_iter->second, source_run, run_source->run_offset);
      ++group_counter;
    }    
    
    run_source->next_group();
  }
  
  // Add non-empty Runs to the output
  for (auto partition_idx = size_t{0}; partition_idx < partitions.size(); ++partition_idx) {
    auto& run = run_per_partition[partition_idx];

    run.flush_buffers(source_run);

    if (run.size > 0) {
      partitions[partition_idx].runs.emplace_back(std::move(run));
    }
  }

  const auto continue_hashing = static_cast<float>(group_counter) / hash_table.size() < 3;
#if VERBOSE
  std::cout << indent(level) << "hashing(): processed " << counter << " elements into " << hash_table.size() <<
  " groups in " << t.lap_formatted() << "; hash_counter: "<<hash_table.hash_function().hash_counter << "; compare_counter: " << hash_table.key_eq().compare_counter <<
  "; load_factor: " << hash_table.load_factor() << "; continue_hashing: " << continue_hashing << "\n";
#endif
  
  return {continue_hashing, group_counter};
}

template <typename Run>
std::vector<Partition<Run>> adaptive_hashing_and_partition(const AggregateHashSortDefinition& definition,
                                                           const std::shared_ptr<AbstractRunSource<Run>>& run_source,
                                                           const RadixFanOut& radix_fan_out, const size_t level) {

#if VERBOSE
  std::cout << indent(level) << "adaptive_hashing_and_partition() {"
            << "\n";
  Timer t;
#endif

  auto remaining_row_count = run_source->size();

  // Start with a single partition and expand to `radix_fan_out.partition_count` partitions if `hashing()` detects a too
  // low density of groups and determines the switch to HashSortMode::RadixFanOut
  auto partitions = std::vector<Partition<Run>>{radix_fan_out.partition_count};
  auto mode = HashSortMode::Hashing;

  /**
   * Initial hashing pass without simultaneous partitioning
   */
  {
#if VERBOSE
    std::cout << indent(level) << "adaptive_hashing_and_partition() remaining_rows: " << remaining_row_count << "\n";
#endif
    const auto hash_table_size = configure_hash_table(definition, remaining_row_count);
    const auto[continue_hashing, hashing_row_count] =
    hashing(definition, hash_table_size, definition.config.hash_table_max_load_factor, run_source, RadixFanOut{1, 0, 0}, partitions, level);
    if (!continue_hashing) {
      mode = HashSortMode::Partition;
    }

    remaining_row_count -= hashing_row_count;

    // The initial hashing() pass didn't process all data, so in order to progress with the main loop below we need to
    // partition its output
    if (!run_source->end_of_source()) {
#if VERBOSE
      std::cout << indent(level) << "adaptive_hashing_and_partition() partitioning initial hash table\n";
#endif
      const auto initial_fan_out_row_count = partitions.front().size();
      auto initial_fan_out_source =
      std::static_pointer_cast<AbstractRunSource<Run>>(std::make_shared<PartitionRunSource<Run>>
      (std::move(partitions.front().runs)));
      partitions = std::vector<Partition<Run>>{radix_fan_out.partition_count};
      partition(initial_fan_out_row_count, initial_fan_out_source, radix_fan_out, partitions, level);
    }
  }

  /**
   * Main loop of alternating hashing (with simultaneous partitioning) and partitioning steps
   */
  while (!run_source->end_of_source()) {
#if VERBOSE
    std::cout << indent(level) << "adaptive_hashing_and_partition() remaining_rows: " << remaining_row_count << "\n";
#endif

    if (mode == HashSortMode::Hashing) {
      const auto hash_table_size = configure_hash_table(config, remaining_row_count);
      const auto [continue_hashing, hashing_row_count] =
      hashing(definition, hash_table_size, config.hash_table_max_load_factor, run_source, radix_fan_out, partitions, level);
      if (!continue_hashing) {
        mode = HashSortMode::Partition;
      }
      remaining_row_count -= hashing_row_count;

    } else {
      const auto partition_row_count = std::min(config.max_partitioning_counter, remaining_row_count);
      partition(definition, partition_row_count, run_source, radix_fan_out, partitions, level);
      mode = HashSortMode::Hashing;
      remaining_row_count -= partition_row_count;
    }
  }

#if VERBOSE
  std::cout << indent(level) << "} // adaptive_hashing_and_partition() took " << t.lap_formatted() << "\n";
#endif

  return partitions;
}

template <typename Run>
std::vector<Run> aggregate(const AggregateHashSortDefinition& definition,
                           const std::shared_ptr<AbstractRunSource<Run>>& run_source, const size_t level) {
  if (run_source->end_of_source()) {
    return {};
  }

#if VERBOSE
  std::cout << indent(level) << "aggregate() at level " << level << " - ";

  if (const auto* table_run_source = dynamic_cast<TableRunSource<GroupRun>*>(run_source.get())) {
    std::cout << "groups from Table with " << table_run_source->table->row_count() << " rows";
  } else if (const auto* partition_run_source = dynamic_cast<PartitionRunSource<GroupRun>*>(run_source.get())) {
    auto rows = size_t{0};
    for (const auto& run : partition_run_source->runs) {
      rows += run.size();
    }

    std::cout << "groups from Partition with " << partition_run_source->runs.size() << " runs and " << rows << " rows";
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

  auto partitions = adaptive_hashing_and_partition(definition, run_source, radix_fan_out, level);

  for (auto& partition : partitions) {
    if (partition.size() == 0) {
      continue;
    }

    const auto partition_run_source = std::static_pointer_cast<AbstractRunSource<Run>>(std::make_shared<PartitionRunSource<Run>>(std::move(partition.runs)));

    auto aggregated_partition = aggregate(config, partition_run_source, level + 1);

    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()),
                       std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

}  // namespace aggregate_hashsort

}  // namespace opossum