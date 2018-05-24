#include "cost_feature.hpp"

#include "utils/assert.hpp"

namespace opossum {

bool CostFeatureVariant::boolean() const {
  Assert(value.type() == typeid(bool), "CostFeatureVariant doesn't contain a bool");
  return boost::get<bool>(value);
}

float CostFeatureVariant::scalar() const {
  Assert(value.type() == typeid(float), "CostFeatureVariant doesn't contain a float");
  return boost::get<float>(value);
}

DataType CostFeatureVariant::data_type() const {
  Assert(value.type() == typeid(DataType), "CostFeatureVariant doesn't contain a DataType");
  return boost::get<DataType>(value);
}

PredicateCondition CostFeatureVariant::predicate_condition() const {
  Assert(value.type() == typeid(PredicateCondition), "CostFeatureVariant doesn't contain a PredicateCondition");
  return boost::get<PredicateCondition>(value);
}

}  // namespace opossum
