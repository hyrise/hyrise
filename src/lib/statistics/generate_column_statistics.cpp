#include "generate_column_statistics.hpp"

#include <boost/container/pmr/monotonic_buffer_resource.hpp>

#include "storage/segment_iterate.hpp"

namespace opossum {

/**
 * Specialisation for strings since they don't have numerical_limits and that's what the unspecialised implementation
 * uses.
 */
template <>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics<pmr_string>(const Table& table,
                                                                             const ColumnID column_id) {
  auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(table.row_count() * 10);
  auto distinct_set =
      std::unordered_set<std::string_view, std::hash<std::string_view>, std::equal_to<>,
                         PolymorphicAllocator<std::string_view>>(PolymorphicAllocator<std::string_view>{&temp_buffer});
  distinct_set.reserve(table.row_count());

  // For segments where the iterators do not return stable references, we temporarily copy the strings into temp_strings
  auto temp_strings = std::list<pmr_string, PolymorphicAllocator<pmr_string>>{&temp_buffer};

  auto null_value_count = size_t{0};

  // These strings must survive the temp_buffer, so we explicitly set the default resource.
  auto default_memory = boost::container::pmr::get_default_resource();
  auto min = pmr_string{default_memory};
  auto max = pmr_string{default_memory};

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_segment = table.get_chunk(chunk_id)->get_segment(column_id);

    segment_with_iterators<pmr_string>(*base_segment, [&](auto segment_it, const auto segment_end) {
      while (segment_it != segment_end) {
        if (segment_it->is_null()) {
          ++null_value_count;
        } else {
          // One would expect distinct_set.emplace() to be the same as the code below. However, "The element may be
          // constructed even if there already is an element with the key in the container, in which case the newly
          // constructed element will be destroyed immediately."
          // This is the case here, where simply using emplace takes ~50% longer.
          auto it = distinct_set.find(segment_it->value());
          if (it != distinct_set.end()) return;

          if constexpr (segment_it.ReferenceIsStable) {
            it = distinct_set.emplace_hint(it, segment_it->value());
          } else {
            temp_strings.emplace_back(segment_it->value());
            it = distinct_set.emplace_hint(it, temp_strings.back());
          }

          if (distinct_set.size() == 1) {
            min = *it;
            max = *it;
          } else {
            if (*it < min) min = *it;
            if (*it > max) max = *it;
          }
        }

        ++segment_it;
      }
    });
  }

  const auto null_value_ratio =
      table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  return std::make_shared<ColumnStatistics<pmr_string>>(null_value_ratio, distinct_count, min, max);
}

}  // namespace opossum
