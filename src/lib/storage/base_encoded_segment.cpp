#include "base_encoded_segment.hpp"

#include "storage/abstract_column_visitor.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

void BaseEncodedSegment::append(const AllTypeVariant&) { Fail("Encoded column is immutable."); }

CompressedVectorType BaseEncodedSegment::compressed_vector_type() const { return CompressedVectorType::Invalid; }

}  // namespace opossum
