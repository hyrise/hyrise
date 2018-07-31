#include "generate_column_statistics.hpp"

namespace opossum {

/**
 * Specialisation for strings since they don't have numerical_limits and that's what the unspecialised implementation
 * uses.
 */
template <>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics<std::string>(const Table& table,
                                                                              const ColumnID column_id) {
  std::unordered_set<std::string> distinct_set;
  // It would be nice to use string_view here, but the iterables hold copies of the values, not references themselves.
  // ColumnIteratorValue would have to be changed to `T& _value` and this brings a whole bunch of problems in iterators
  // that create stack copies of the accessed values (e.g., for ReferenceColumns)

  auto null_value_count = size_t{0};

  auto min = std::string{};
  auto max = std::string{};

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_column = table.get_chunk(chunk_id)->get_column(column_id);

    resolve_column_type<std::string>(*base_column, [&](auto& column) {
      auto iterable = create_iterable_from_column<std::string>(column);
      iterable.for_each([&](const auto& column_value) {
        if (column_value.is_null()) {
          ++null_value_count;
        } else {
          if (distinct_set.empty()) {
            min = column_value.value();
            max = column_value.value();
          } else {
            min = std::min(min, column_value.value());
            max = std::max(max, column_value.value());
          }
          distinct_set.insert(column_value.value());
        }
      });
    });
  }

  const auto null_value_ratio =
      table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  return std::make_shared<ColumnStatistics<std::string>>(null_value_ratio, distinct_count, min, max);
}

}  // namespace opossum
