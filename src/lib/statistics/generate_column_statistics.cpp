#include "generate_column_statistics.hpp"

#include <boost/container/pmr/monotonic_buffer_resource.hpp>

#include "storage/segment_iterate.hpp"

namespace opossum {

/**
 * Specialisation for strings since they don't have numerical_limits and that's what the unspecialised implementation
 * uses.
 */
template <>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics<std::string>(const Table& table,
                                                                              const ColumnID column_id) {
  // It would be nice to store string_views in the set, but the iterables hold copies of the values, not references.
  // SegmentPosition would have to be changed to `T& _value` and this brings a whole bunch of problems in iterators
  // that create stack copies of the accessed values (e.g., for ReferenceSegments)

  auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(table.row_count() * 10);
  auto distinct_set =
      std::unordered_set<std::string, std::hash<std::string>, std::equal_to<>, PolymorphicAllocator<std::string>>(
          PolymorphicAllocator<std::string>{&temp_buffer});
  distinct_set.reserve(table.row_count());

  auto null_value_count = size_t{0};

  auto min = std::string_view{};
  auto max = std::string_view{};

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_segment = table.get_chunk(chunk_id)->get_segment(column_id);

    segment_iterate<std::string>(*base_segment, [&](const auto& position) {
      if (position.is_null()) {
        ++null_value_count;
      } else {
        // One would expect distinct_set.emplace() to be the same as the code below. However, "The element may be
        // constructed even if there already is an element with the key in the container, in which case the newly
        // constructed element will be destroyed immediately."
        // This is the case here, where simply using emplace takes ~50% longer.
        auto it = distinct_set.find(position.value());
        if (it == distinct_set.end()) {
          it = distinct_set.emplace_hint(it, std::move(position.value()));
        }

        if (distinct_set.size() == 1) {
          min = *it;
          max = *it;
        } else {
          if (*it < min) min = *it;
          if (*it > max) max = *it;
        }
      }
    });
  }

  const auto null_value_ratio =
      table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  return std::make_shared<ColumnStatistics<std::string>>(null_value_ratio, distinct_count, std::string{min},
                                                         std::string{max});
}

}  // namespace opossum
