#pragma once

#include <limits>
#include <memory>

#include "storage/base_encoded_column.hpp"

namespace opossum {

class BaseAttributeVector;

// BaseDeprecatedDictionaryColumn is implemented by DeprecatedDictionaryColumn and offers methods from the
// DeprecatedDictionaryColumn that are needed for creating indices but that do not depend on the template parameter of
// the DeprecatedDictionaryColumn, thus allowing the single indexes relying on these methods to be untemplated.
class BaseDeprecatedDictionaryColumn : public BaseEncodedColumn {
 public:
  EncodingType encoding_type() const final { return EncodingType::DeprecatedDictionary; }

  virtual ValueID lower_bound(const AllTypeVariant& value) const = 0;
  virtual ValueID upper_bound(const AllTypeVariant& value) const = 0;
  virtual size_t unique_values_count() const = 0;
  virtual std::shared_ptr<const BaseAttributeVector> attribute_vector() const = 0;
};
}  // namespace opossum
