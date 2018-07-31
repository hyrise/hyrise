#include "base_encoded_column.hpp"

#include "storage/abstract_column_visitor.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

void BaseEncodedColumn::append(const AllTypeVariant&) { Fail("Encoded column is immutable."); }

CompressedVectorType BaseEncodedColumn::compressed_vector_type() const { return CompressedVectorType::Invalid; }

}  // namespace opossum
