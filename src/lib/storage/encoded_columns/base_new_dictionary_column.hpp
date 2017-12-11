#pragma once

#include <limits>
#include <memory>

#include "storage/encoded_columns/base_encoded_column.hpp"

namespace opossum {

class BaseNsVector;

/**
 * @brief Base class of NewDictionaryColumn<T> exposing type independent interface
 */
class BaseNewDictionaryColumn : public BaseEncodedColumn {
 public:
  EncodingType encoding_type() const final { return EncodingType::Dictionary; }

  /**
   * @brief Returns index (i.e. ValueID) of first dictionary entry >= search value
   *
   * @param value the search value
   * @return INVALID_VALUE_ID if entries are smaller than value
   */
  virtual ValueID lower_bound(const AllTypeVariant& value) const = 0;

  /**
   * @brief Returns index (i.e. ValueID) of first dictionary entry > search value
   *
   * @param value the search value
   * @return INVALID_VALUE_ID if entries are smaller than or equal to value
   */
  virtual ValueID upper_bound(const AllTypeVariant& value) const = 0;

  /**
   * @brief The size of the dictionary
   */
  virtual size_t unique_values_count() const = 0;

  virtual std::shared_ptr<const BaseNsVector> attribute_vector() const = 0;

  /**
   * @brief Returns encoding specific null value ID
   */
  virtual const ValueID null_value_id() const = 0;
};
}  // namespace opossum
