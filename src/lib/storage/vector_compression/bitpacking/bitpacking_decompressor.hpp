#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "compact_vector.hpp"
#include "bitpacking_vector.hpp"
#include "vector_types.hpp"

namespace opossum {

class BitpackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit BitpackingDecompressor(const BitpackingVector& vector);
  BitpackingDecompressor(const BitpackingDecompressor& other) = default;
  BitpackingDecompressor(BitpackingDecompressor&& other) = default;

  BitpackingDecompressor& operator=(const BitpackingDecompressor& other) {
    DebugAssert(&vector == &other.vector, "Cannot reassign BitpackingDecompressor");
    return *this;
  }
  BitpackingDecompressor& operator=(BitpackingDecompressor&& other) {
    DebugAssert(&vector == &other.vector, "Cannot reassign BitpackingDecompressor");
    return *this;
  }

  ~BitpackingDecompressor() override = default;

  uint32_t get(size_t i) final {
    return vector.get(i);
  }

  size_t size() const final { return vector.on_size(); }

 private:
  const BitpackingVector& vector;
};

}  // namespace opossum
