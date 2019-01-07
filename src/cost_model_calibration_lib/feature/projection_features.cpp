#include "feature/projection_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> ProjectionFeatures::serialize() const {return {
    {"input_column_count", static_cast<int64_t>(input_column_count)},
    {"output_column_count", static_cast<int64_t>(output_column_count)},
};};

}  // namespace cost_model
}  // namespace opossum