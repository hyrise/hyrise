#include "chunk_statistics2.hpp"

#include "segment_statistics2.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

ChunkStatistics2::ChunkStatistics2(const Cardinality row_count) : row_count(row_count) {}

std::optional<float> ChunkStatistics2::estimate_column_null_value_ratio(const ColumnID column_id) const {
  const auto base_segment_statistics = segment_statistics[column_id];

  auto null_value_ratio = std::optional<float>{};

  resolve_data_type(segment_statistics[column_id]->data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto segment_statistics2 =
    std::static_pointer_cast<SegmentStatistics2<ColumnDataType>>(base_segment_statistics);

    if (segment_statistics2->null_value_ratio) {
      null_value_ratio =  segment_statistics2->null_value_ratio->null_value_ratio;
    } else if (segment_statistics2->histogram) {
      if (row_count != 0) {
        null_value_ratio = 1.0f - (static_cast<float>(segment_statistics2->histogram->total_count()) / row_count);
      }
    }
  });

  return null_value_ratio;
}

std::ostream& operator<<(std::ostream& stream, const ChunkStatistics2& chunk_statistics) {
  stream << "RowCount: " << chunk_statistics.row_count << "; ";
  stream << "ApproxInvalidRowCount: " << chunk_statistics.approx_invalid_row_count << "; " << std::endl;
  for (auto column_id = ColumnID{0}; column_id < chunk_statistics.segment_statistics.size(); ++column_id) {
    stream << "SegmentStatistics of Column " << column_id << " {" << std::endl;
    const auto& base_segment_statistics = chunk_statistics.segment_statistics[column_id];

    resolve_data_type(base_segment_statistics->data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(base_segment_statistics);

      stream << *segment_statistics << std::endl;
    });

    stream << "}" << std::endl;
  }

  return stream;
}

}  // namespace opossum
