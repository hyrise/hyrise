#pragma once

#include "hyrise.hpp"
#include "shared_dictionaries_plugin.hpp"
#include "storage/table.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

/**
 * Finds and merges dictionaries with shared data for the given column of a table.
 * Each dictionary segment is compared based on its similarity with all other existing shared dictionaries.
 * If the similarity (e.g. jaccard-index) is above the threshold it merges the dictionaries.
 * If none of the existing shared dictionaries meet the threshold it compares the current dictionary with the
 * previous dictionary and eventually merges them to create a new shared dictionary.
 */
template <typename T>
class SharedDictionariesColumnProcessor {
  friend class SharedDictionariesPluginTest;

 public:
  /**
 * Merge plan for a shared dictionary containing the segments to merge and additional information
 */
  struct MergePlan {
    std::shared_ptr<const pmr_vector<T>> shared_dictionary;
    std::vector<SegmentChunkPair<T>> segment_chunk_pairs_to_merge = {};
    bool contains_non_merged_segment = false;
    bool contains_already_merged_segment = false;
    uint64_t non_merged_total_bytes = 0;
    uint64_t non_merged_dictionary_bytes = 0;

    explicit MergePlan(const std::shared_ptr<const pmr_vector<T>>& init_shared_dictionary)
        : shared_dictionary(init_shared_dictionary) {}

    void add_segment_chunk_pair(const SegmentChunkPair<T>& segment_chunk_pair, bool is_already_merged);
  };

  const std::shared_ptr<Table> table;
  const std::string table_name;
  const ColumnID column_id;
  const std::string column_name;
  const double jaccard_index_threshold;
  SharedDictionariesPlugin::SharedDictionariesStats& stats;

  SharedDictionariesColumnProcessor(const std::shared_ptr<Table>& init_table, const std::string& init_table_name,
                                    const ColumnID init_column_id, const std::string& init_column_name,
                                    const double init_jaccard_index_threshold,
                                    SharedDictionariesPlugin::SharedDictionariesStats& init_stats);

  void process();

 private:
  void _initialize_merge_plans(std::vector<std::shared_ptr<MergePlan>>& merge_plans);

  std::pair<std::optional<size_t>, std::shared_ptr<const pmr_vector<T>>> _union_with_best_existing_shared_dictionary(
      const std::shared_ptr<const pmr_vector<T>> current_dictionary,
      const std::vector<std::shared_ptr<MergePlan>>& merge_plans, const PolymorphicAllocator<T>& allocator);

  std::shared_ptr<const pmr_vector<T>> _union_with_previous_dictionary(
      const std::shared_ptr<const pmr_vector<T>> current_dictionary,
      const SegmentChunkPair<T> previous_segment_chunk_pair, const PolymorphicAllocator<T>& allocator);

  void _process_merge_plans(const std::vector<std::shared_ptr<MergePlan>>& merge_plans,
                            const PolymorphicAllocator<T>& allocator);

  std::shared_ptr<const BaseCompressedVector> _create_new_attribute_vector(
      const std::shared_ptr<DictionarySegment<T>> segment, const std::shared_ptr<const pmr_vector<T>> shared_dictionary,
      const PolymorphicAllocator<T>& allocator);

  static size_t _calc_dictionary_memory_usage(const std::shared_ptr<const pmr_vector<T>> dictionary);

  bool _should_merge(const double jaccard_index, const size_t current_dictionary_size,
                     const size_t shared_dictionary_size,
                     const std::vector<SegmentChunkPair<T>>& shared_segment_chunk_pairs);

  static double _calc_jaccard_index(const size_t union_size, const size_t intersection_size);

  static bool _increases_attribute_vector_width(const size_t shared_dictionary_size,
                                                const size_t current_dictionary_size);
};

EXPLICITLY_DECLARE_DATA_TYPES(SharedDictionariesColumnProcessor);

}  // namespace opossum
