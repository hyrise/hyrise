#include "horizontal_statistics_slice.hpp"

#include "resolve_type.hpp"
#include "vertical_statistics_slice.hpp"
#include "utils/assert.hpp"

namespace opossum {

HorizontalStatisticsSlice::HorizontalStatisticsSlice(const Cardinality row_count) : row_count(row_count) {}

std::optional<float> HorizontalStatisticsSlice::estimate_column_null_value_ratio(const ColumnID column_id) const {
  const auto base_vertical_slices = vertical_slices[column_id];

  auto null_value_ratio = std::optional<float>{};

  resolve_data_type(vertical_slices[column_id]->data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto vertical_slices2 =
        std::static_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(base_vertical_slices);

    if (vertical_slices2->null_value_ratio) {
      null_value_ratio = vertical_slices2->null_value_ratio->null_value_ratio;
    } else if (vertical_slices2->histogram) {
      if (row_count != 0) {
        null_value_ratio = 1.0f - (static_cast<float>(vertical_slices2->histogram->total_count()) / row_count);
      }
    }
  });

  return null_value_ratio;
}

std::ostream& operator<<(std::ostream& stream, const HorizontalStatisticsSlice& chunk_statistics) {
  stream << "RowCount: " << chunk_statistics.row_count << "; ";
  for (auto column_id = ColumnID{0}; column_id < chunk_statistics.vertical_slices.size(); ++column_id) {
    stream << "SegmentStatistics of Column " << column_id << " {" << std::endl;
    const auto& base_vertical_slices = chunk_statistics.vertical_slices[column_id];

    resolve_data_type(base_vertical_slices->data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto vertical_slices =
          std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(base_vertical_slices);

      stream << *vertical_slices << std::endl;
    });

    stream << "}" << std::endl;
  }

  return stream;
}

}  // namespace opossum
