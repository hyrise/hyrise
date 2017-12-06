#pragma once

#include <limits>
#include <memory>

#include "storage/encoded_columns/base_encoded_column.hpp"

namespace opossum {

class BaseAttributeVector;

// BaseDictionaryColumn is implemented by DictionaryColumn and offers methods from the DictionaryColumn
// that are needed for creating indices but that do not depend on the template parameter of the DictionaryColumn,
// thus allowing the single indexes relying on these methods to be untemplated.
class BaseDictionaryColumn : public BaseEncodedColumn {
 public:
  EncodingType encoding_type() const final { return EncodingType::Dictionary; }

  virtual ValueID lower_bound(const AllTypeVariant& value) const = 0;
  virtual ValueID upper_bound(const AllTypeVariant& value) const = 0;
  virtual size_t unique_values_count() const = 0;
  virtual std::shared_ptr<const BaseAttributeVector> attribute_vector() const = 0;
};
}  // namespace opossum
