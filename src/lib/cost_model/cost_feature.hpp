#pragma once

#include <map>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 * List of features usable in AbstractCostModels.
 *
 * Using enum to provide the unified "AbstractCostFeatureProxy" to access the same features for LQPs and PQPs.
 * Also, this makes it easy to specify Cost formulas from data only, as e.g. CostModelRuntime does.
 */
enum class CostFeature {
  /**
   * Numerical features
   */
  LeftInputRowCount,
  RightInputRowCount,
  InputRowCountProduct,  // LeftInputRowCount * RightInputRowCount
  LeftInputReferenceRowCount,
  RightInputReferenceRowCount,  // *InputRowCount if the input is References, 0 otherwise
  LeftInputRowCountLogN,
  RightInputRowCountLogN,  // *InputRowCount * log(*InputRowCount)
  LargerInputRowCount,
  SmallerInputRowCount,  // Major = Input with more rows, Minor = Input with less rows
  LargerInputReferenceRowCount,
  SmallerInputReferenceRowCount,
  OutputRowCount,
  OutputReferenceRowCount,  // If input is References, then OutputRowCount. 0 otherwise.

  /**
   * Categorical features
   */
  LeftDataType,
  RightDataType,       // Only valid for Predicated Joins, TableScans
  PredicateCondition,  // Only valid for Predicated Joins, TableScans

  /**
   * Boolean features
   */
  LeftInputIsReferences,
  RightInputIsReferences,  // *Input is References
  RightOperandIsColumn,    // Only valid for TableScans
  LeftInputIsMajor         // LeftInputRowCount > RightInputRowCount
};

using CostFeatureWeights = std::map<CostFeature, float>;

/**
 * Wraps a Variant of all data types for CostFeatues and provides getters for the member types of the variants that
 * perform type checking at runtime.
 */
struct CostFeatureVariant {
 public:
  template <typename T>
  CostFeatureVariant(const T& value) : value(value) {}  // NOLINT - implicit conversion is intended

  bool boolean() const;
  float scalar() const;
  DataType data_type() const;
  PredicateCondition predicate_condition() const;

  boost::variant<float, DataType, PredicateCondition, bool> value;
};

}  // namespace opossum
