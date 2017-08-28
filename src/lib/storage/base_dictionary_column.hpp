#pragma once

#include <limits>
#include <memory>

#include "base_column.hpp"

namespace opossum {

class BaseAttributeVector;

// TODO(anyone): Right now, INVALID_VALUE_ID and NULL_VALUE_ID are the same
// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// BaseDictionaryColumn is implemented by DicionaryColumn and offers methods from the DictionaryColumn
// that are needed for creating indices but that do not depend on the template parameter of the DictionaryColumn,
// thus allowing the single indexes relying on these methods to be untemplated.
class BaseDictionaryColumn : public BaseColumn {
 public:
  virtual ValueID lower_bound(const AllTypeVariant &value) const = 0;
  virtual ValueID upper_bound(const AllTypeVariant &value) const = 0;
  virtual size_t unique_values_count() const = 0;
  virtual std::shared_ptr<const BaseAttributeVector> attribute_vector() const = 0;
};
}  // namespace opossum
