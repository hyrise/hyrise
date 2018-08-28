#include "base_encoded_segment.hpp"

#include "storage/abstract_segment_visitor.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

void BaseEncodedSegment::append(const AllTypeVariant&) { Fail("Encoded segment is immutable."); }

CompressedVectorType BaseEncodedSegment::compressed_vector_type() const { return CompressedVectorType::Invalid; }

}  // namespace opossum
