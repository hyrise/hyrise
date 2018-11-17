#include "base_encoded_segment.hpp"

#include "storage/abstract_segment_visitor.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

CompressedVectorType BaseEncodedSegment::compressed_vector_type() const { return CompressedVectorType::Invalid; }

}  // namespace opossum
