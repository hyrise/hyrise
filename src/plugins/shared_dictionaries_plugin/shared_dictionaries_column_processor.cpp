#include "shared_dictionaries_column_processor.hpp"

namespace opossum {

template <typename T>
SharedDictionariesColumnProcessor<T>::SharedDictionariesColumnProcessor(
    const std::shared_ptr<Table>& init_table, const std::string& init_table_name, const ColumnID init_column_id,
    const std::string& init_column_name, const double init_jaccard_index_threshold,
    const std::shared_ptr<SharedDictionariesPlugin::SharedDictionariesStats>& init_stats)
    : table(init_table),
      table_name(init_table_name),
      column_id(init_column_id),
      column_name(init_column_name),
      jaccard_index_threshold(init_jaccard_index_threshold),
      stats(init_stats) {}

template <typename T>
void SharedDictionariesColumnProcessor<T>::process() {
  const auto allocator = PolymorphicAllocator<T>{};
  auto merge_plans = std::vector<std::shared_ptr<MergePlan>>{};
  _initialize_merge_plans(merge_plans);

  std::optional<SegmentChunkPair<T>> previous_segment_chunk_pair_opt = std::nullopt;

  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);
    stats->total_previous_bytes += segment->memory_usage(MemoryUsageCalculationMode::Full);

    const auto current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (current_dictionary_segment && !current_dictionary_segment->uses_dictionary_sharing()) {
      const auto current_dictionary = current_dictionary_segment->dictionary();
      const auto current_segment_chunk_pair = std::make_pair(current_dictionary_segment, chunk);

      const auto [best_merge_plan_index, best_shared_dictionary] =
          _compare_with_existing_merge_plans(current_dictionary, merge_plans, allocator);

      auto merged_current_dictionary = false;
      if (best_merge_plan_index >= 0 && best_shared_dictionary) {
        // Merge with existing shared dictionary
        const auto merge_plan = merge_plans[best_merge_plan_index];
        merge_plan->shared_dictionary = best_shared_dictionary;
        _add_segment_chunk_pair(*merge_plan, current_segment_chunk_pair, false);
        merged_current_dictionary = true;
      } else if (previous_segment_chunk_pair_opt) {
        // Check with previous segment
        const auto shared_dictionary_with_previous =
            _compare_with_previous_dictionary(current_dictionary, *previous_segment_chunk_pair_opt, allocator);
        if (shared_dictionary_with_previous) {
          // Merge with previous dictionary
          const auto new_merge_plan = std::make_shared<MergePlan>(shared_dictionary_with_previous);
          _add_segment_chunk_pair(*new_merge_plan, current_segment_chunk_pair, false);
          _add_segment_chunk_pair(*new_merge_plan, *previous_segment_chunk_pair_opt, false);
          merge_plans.emplace_back(new_merge_plan);
          merged_current_dictionary = true;
        }
      }

      // Save unmerged current dictionary for possible later merge
      previous_segment_chunk_pair_opt =
          merged_current_dictionary ? std::nullopt : std::make_optional(current_segment_chunk_pair);
    }
  }
  _process_merge_plans(merge_plans, allocator);
}

template <typename T>
void SharedDictionariesColumnProcessor<T>::_initialize_merge_plans(
    std::vector<std::shared_ptr<MergePlan>>& merge_plans) {
  auto shared_dictionaries_map = std::map<std::shared_ptr<const pmr_vector<T>>, std::shared_ptr<MergePlan>>{};
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);

    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (dictionary_segment && dictionary_segment->uses_dictionary_sharing()) {
      stats->num_existing_merged_dictionaries++;
      const auto shared_dictionary = dictionary_segment->dictionary();
      const auto [iter, inserted] =
          shared_dictionaries_map.try_emplace(shared_dictionary, std::make_shared<MergePlan>(shared_dictionary));
      const auto segment_chunk_pair = std::make_pair(dictionary_segment, chunk);
      const auto merge_plan = iter->second;
      _add_segment_chunk_pair(*merge_plan, segment_chunk_pair, true);
    }
  }

  stats->num_existing_shared_dictionaries = shared_dictionaries_map.size();

  for (const auto [key, merge_plan] : shared_dictionaries_map) {
    merge_plans.emplace_back(merge_plan);
  }
}

template <typename T>
std::pair<int32_t, std::shared_ptr<const pmr_vector<T>>>
SharedDictionariesColumnProcessor<T>::_compare_with_existing_merge_plans(
    const std::shared_ptr<const pmr_vector<T>> current_dictionary,
    const std::vector<std::shared_ptr<MergePlan>>& merge_plans, const PolymorphicAllocator<T>& allocator) {
  auto best_merge_plan_index = -1;
  auto best_jaccard_index = -1;
  std::shared_ptr<const pmr_vector<T>> best_shared_dictionary = nullptr;
  const auto merge_plans_size = merge_plans.size();
  for (auto merge_plan_index = 0ul; merge_plan_index < merge_plans_size; ++merge_plan_index) {
    const auto merge_plan = merge_plans[merge_plan_index];
    const auto shared_dictionary = merge_plan->shared_dictionary;
    auto union_result = std::make_shared<pmr_vector<T>>(allocator);
    union_result->reserve(std::max(current_dictionary->size(), shared_dictionary->size()));
    std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), shared_dictionary->cbegin(),
                   shared_dictionary->cend(), std::back_inserter(*union_result));
    const auto total_size = current_dictionary->size() + shared_dictionary->size();
    const auto union_size = union_result->size();
    const auto jaccard_index = _calc_jaccard_index(union_size, total_size - union_size);
    if (jaccard_index > best_jaccard_index) {
      if (_should_merge(jaccard_index, current_dictionary->size(), union_size,
                        merge_plan->segment_chunk_pairs_to_merge)) {
        best_merge_plan_index = static_cast<int32_t>(merge_plan_index);
        best_jaccard_index = jaccard_index;
        union_result->shrink_to_fit();
        best_shared_dictionary = union_result;
      }
    }
  }

  return std::make_pair(best_merge_plan_index, best_shared_dictionary);
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> SharedDictionariesColumnProcessor<T>::_compare_with_previous_dictionary(
    const std::shared_ptr<const pmr_vector<T>> current_dictionary,
    const SegmentChunkPair<T> previous_segment_chunk_pair, const PolymorphicAllocator<T>& allocator) {
  const auto previous_dictionary = previous_segment_chunk_pair.first->dictionary();
  auto union_result = std::make_shared<pmr_vector<T>>(allocator);
  union_result->reserve(std::max(current_dictionary->size(), previous_dictionary->size()));
  std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), previous_dictionary->cbegin(),
                 previous_dictionary->cend(), std::back_inserter(*union_result));
  const auto total_size = current_dictionary->size() + previous_dictionary->size();
  const auto union_size = union_result->size();
  const auto jaccard_index = _calc_jaccard_index(union_size, total_size - union_size);
  if (_should_merge(jaccard_index, current_dictionary->size(), union_size,
                    std::vector<SegmentChunkPair<T>>{previous_segment_chunk_pair})) {
    union_result->shrink_to_fit();
    return union_result;
  } else {
    return nullptr;
  }
}

template <typename T>
void SharedDictionariesColumnProcessor<T>::_process_merge_plans(
    const std::vector<std::shared_ptr<MergePlan>>& merge_plans, const PolymorphicAllocator<T>& allocator) {
  const auto merge_plans_size = merge_plans.size();
  for (auto merge_plan_index = 0ul; merge_plan_index < merge_plans_size; ++merge_plan_index) {
    const auto merge_plan = merge_plans[merge_plan_index];
    const auto shared_dictionary = merge_plan->shared_dictionary;
    const auto segment_chunk_pairs_to_merge = merge_plan->segment_chunk_pairs_to_merge;
    Assert(segment_chunk_pairs_to_merge.size() >= 2, "At least 2 segments should be merged.");
    if (merge_plan->contains_non_merged_segment) {
      stats->num_shared_dictionaries++;
      auto shared_dictionary_memory_usage = _calc_dictionary_memory_usage(shared_dictionary);
      const auto new_dictionary_memory_usage = shared_dictionary_memory_usage;

      auto previous_dictionary_memory_usage = 0ul;
      if (merge_plan->contains_already_merged_segment) {
        previous_dictionary_memory_usage += shared_dictionary_memory_usage;
      }

      previous_dictionary_memory_usage += merge_plan->non_merged_dictionary_bytes;
      stats->modified_previous_bytes += merge_plan->non_merged_total_bytes;

      for (auto segment_chunk_pair_to_merge : segment_chunk_pairs_to_merge) {
        const auto segment = segment_chunk_pair_to_merge.first;
        stats->num_merged_dictionaries++;
        // Create new dictionary encoded segment with adjusted attribute vector and shared dictionary
        const auto new_attribute_vector = _create_new_attribute_vector(segment, shared_dictionary, allocator);
        const auto new_dictionary_segment =
            std::make_shared<DictionarySegment<T>>(shared_dictionary, new_attribute_vector, true);

        // Replace segment in chunk
        segment_chunk_pair_to_merge.second->replace_segment(column_id, new_dictionary_segment);
      }

      Assert(new_dictionary_memory_usage < previous_dictionary_memory_usage,
             "New dictionary memory usage should be lower than previous");
      const auto bytes_saved = previous_dictionary_memory_usage - new_dictionary_memory_usage;
      stats->total_bytes_saved += bytes_saved;

      auto log_stream = std::stringstream();
      log_stream << "[Table=" << table_name << ", Column=" << column_name << "] Merged "
                 << segment_chunk_pairs_to_merge.size() << " dictionaries saving " << bytes_saved << " bytes";
      Hyrise::get().log_manager.add_message(SharedDictionariesPlugin::LOG_NAME, log_stream.str(), LogLevel::Debug);
    }
  }
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> SharedDictionariesColumnProcessor<T>::_create_new_attribute_vector(
    const std::shared_ptr<DictionarySegment<T>> segment, const std::shared_ptr<const pmr_vector<T>> shared_dictionary,
    const PolymorphicAllocator<T>& allocator) {
  const auto chunk_size = segment->size();
  const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());

  auto uncompressed_attribute_vector = pmr_vector<uint32_t>{allocator};
  uncompressed_attribute_vector.reserve(chunk_size);

  for (auto chunk_index = ChunkOffset{0}; chunk_index < chunk_size; ++chunk_index) {
    const auto search_value_opt = segment->get_typed_value(chunk_index);
    if (search_value_opt) {
      // Find and add new value id using binary search
      const auto search_iter =
          std::lower_bound(shared_dictionary->cbegin(), shared_dictionary->cend(), search_value_opt.value());
      Assert(search_iter != shared_dictionary->end(), "Shared dictionary does not contain value.");
      const auto found_index = std::distance(shared_dictionary->cbegin(), search_iter);
      uncompressed_attribute_vector.emplace_back(found_index);
    } else {
      // Assume that search value is NULL
      uncompressed_attribute_vector.emplace_back(max_value_id);
    }
  }

  return std::shared_ptr<const BaseCompressedVector>(compress_vector(
      uncompressed_attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {max_value_id}));
}

// Copied from DictionarySegment::memory_usage
template <typename T>
size_t SharedDictionariesColumnProcessor<T>::_calc_dictionary_memory_usage(
    const std::shared_ptr<const pmr_vector<T>> dictionary) {
  if constexpr (std::is_same_v<T, pmr_string>) {
    return string_vector_memory_usage(*dictionary, MemoryUsageCalculationMode::Full);
  }
  return dictionary->size() * sizeof(typename decltype(dictionary)::element_type::value_type);
}

template <typename T>
bool SharedDictionariesColumnProcessor<T>::_should_merge(
    const double jaccard_index, const size_t current_dictionary_size, const size_t shared_dictionary_size,
    const std::vector<SegmentChunkPair<T>>& shared_segment_chunk_pairs) {
  if (jaccard_index >= jaccard_index_threshold) {
    if (!_increases_attribute_vector_width(shared_dictionary_size, current_dictionary_size)) {
      return std::none_of(shared_segment_chunk_pairs.cbegin(), shared_segment_chunk_pairs.cend(),
                          [shared_dictionary_size](const SegmentChunkPair<T> segment_chunk_pair) {
                            return _increases_attribute_vector_width(shared_dictionary_size,
                                                                     segment_chunk_pair.first->dictionary()->size());
                          });
    }
  }
  return false;
}

template <typename T>
void SharedDictionariesColumnProcessor<T>::_add_segment_chunk_pair(MergePlan& merge_plan,
                                                                   const SegmentChunkPair<T>& segment_chunk_pair,
                                                                   bool is_already_merged) {
  if (is_already_merged) {
    merge_plan.contains_already_merged_segment = true;
  }
  if (!is_already_merged) {
    merge_plan.contains_non_merged_segment = true;
    merge_plan.non_merged_dictionary_bytes += _calc_dictionary_memory_usage(segment_chunk_pair.first->dictionary());
    merge_plan.non_merged_total_bytes += segment_chunk_pair.first->memory_usage(MemoryUsageCalculationMode::Full);
  }
  merge_plan.segment_chunk_pairs_to_merge.emplace_back(segment_chunk_pair);
}

template <typename T>
double SharedDictionariesColumnProcessor<T>::_calc_jaccard_index(size_t union_size, size_t intersection_size) {
  DebugAssert(union_size >= intersection_size, "Union size should be larger than or equal intersection size.");
  return union_size == 0 ? 0.0 : static_cast<double>(intersection_size) / static_cast<double>(union_size);
}

template <typename T>
bool SharedDictionariesColumnProcessor<T>::_increases_attribute_vector_width(const size_t shared_dictionary_size,
                                                                             const size_t current_dictionary_size) {
  if (current_dictionary_size < std::numeric_limits<uint8_t>::max() &&
      shared_dictionary_size >= std::numeric_limits<uint8_t>::max()) {
    return true;
  }

  if (current_dictionary_size < std::numeric_limits<uint16_t>::max() &&
      shared_dictionary_size >= std::numeric_limits<uint16_t>::max()) {
    return true;
  }

  return false;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(SharedDictionariesColumnProcessor);

}  // namespace opossum
