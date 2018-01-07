#include "base_encoded_column.hpp"

#include "utils/assert.hpp"

namespace opossum {

// Encoded columns are immutable
void BaseEncodedColumn::append(const AllTypeVariant&) { Fail("Encoded column is immutable."); }

}  // namespace opossum
