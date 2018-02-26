#include "chunk_statistics.hpp"

#include <iterator>

#include "all_parameter_variant.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<ChunkColumnStatistics>
ChunkColumnStatistics::build_statistics(DataType data_type, std::shared_ptr<BaseColumn> column)
{
    std::shared_ptr<ChunkColumnStatistics> statistics;
    resolve_data_and_column_type(data_type, *column, [&statistics](auto type, auto& typed_column) {
        using ColumnDataType = typename decltype(type)::type;
        statistics = build_statistics_from_concrete_column<ColumnDataType>(typed_column);
    });
    return statistics;
}

bool ChunkStatistics::can_prune(const ColumnID column_id, const AllTypeVariant& value,
                                const PredicateCondition scan_type) const {
  DebugAssert(column_id < _statistics.size(), "The passed column ID should fit in the bounds of the statistics.");
  DebugAssert(_statistics[column_id], "The statistics should not contain any empty shared_ptrs.");
  return _statistics[column_id]->can_prune(value, scan_type);
}

}  // namespace opossum
