#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "compact_vector.hpp"
#include "compact_static_vector.hpp"
#include "vector_types.hpp"

namespace opossum {

class BitpackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit BitpackingDecompressor(const CompactStaticVector& data) : _data{data} {}

  BitpackingDecompressor(const BitpackingDecompressor& other) = default;
  BitpackingDecompressor(BitpackingDecompressor&& other) = default;

  BitpackingDecompressor& operator=(const BitpackingDecompressor& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign BitpackingDecompressor");
    return *this;
  }
  BitpackingDecompressor& operator=(BitpackingDecompressor&& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign BitpackingDecompressor");
    return *this;
  }

  ~BitpackingDecompressor() override = default;

  uint32_t get(size_t i) final {
    return _data.get(i);
  }

  size_t size() const final { return _data.size(); }

 private:
  const CompactStaticVector& _data;
};

}  // namespace opossum
