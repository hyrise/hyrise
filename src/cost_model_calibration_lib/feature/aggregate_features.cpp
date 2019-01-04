#include "aggregate_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> AggregateFeatures::serialize() const {
  return {
      {"aggregate_column_count", aggregate_column_count},
      {"group_by_column_count", group_by_column_count},
  };
}

}  // namespace cost_model
}  // namespace opossum