#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

class Table;
class Chunk;

template <typename T>
class ValidationUtils {
 public:
  struct ColumnStatistics {
    std::optional<T> min{};
    std::optional<T> max{};
    bool all_segments_unique{false};
    bool segments_continuous{false};
    bool segments_disjoint{false};
    bool contains_only_nulls{false};
    bool all_segments_dictionary{false};
  };

  static ColumnStatistics gather_segment_statistics(const std::shared_ptr<const Chunk>& chunk,
                                                    const ColumnID column_id);

  // Allow to early out if not (proven) unique.
  static ColumnStatistics collect_column_statistics(const std::shared_ptr<const Table>& table, const ColumnID column_id,
                                                    bool early_out = false);

  static std::optional<std::pair<T, T>> get_column_min_max_value(const std::shared_ptr<const Table>& table,
                                                                 const ColumnID column_id);
};

EXPLICITLY_DECLARE_DATA_TYPES(ValidationUtils);

}  // namespace hyrise
