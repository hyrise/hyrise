#pragma once

#include <map>
#include <string>

#include "abstract_features.hpp"
#include "all_type_variant.hpp"
#include "column_features.hpp"

namespace opossum {
namespace cost_model {

struct TableScanFeatures : public AbstractFeatures {
  ColumnFeatures first_column{"first"};
  ColumnFeatures second_column{"second"};
  ColumnFeatures third_column{"third"};

  bool is_column_comparison = false;
  std::string scan_operator_type;
  size_t computable_or_column_expression_count = 0;
  size_t effective_chunk_count = 0;

  const std::map<std::string, AllTypeVariant> serialize() const override;
};

}  // namespace cost_model
}  // namespace opossum
