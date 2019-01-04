#include "feature/table_scan_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> TableScanFeatures::serialize() const {
  std::map<std::string, AllTypeVariant> table_scan_features =
  { {"is_column_comparison", is_column_comparison},
    {"scan_type", get_scan_type()},
    {"computable_or_column_expression_count", computable_or_column_expression_count},
    {"effective_chunk_count", effective_chunk_count},
  }

  return table_scan_features.merge(serialized_left_join_column)
      .merge(first_column.serialize())
      .merge(second_column.serialize())
      .merge(third_column.serialize());
};

}  // namespace cost_model
}  // namespace opossum