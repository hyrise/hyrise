#include "compressed_vector_type.hpp"

#include "magic_enum.hpp"

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type) {
  stream << magic_enum::enum_name(compressed_vector_type);
  return stream;
}

}  // namespace hyrise
