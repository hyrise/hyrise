#pragma once

#include "compact_vector.hpp"

#include "bitpacking_vector_type.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class BitPackingVector;

class BitPackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit BitPackingDecompressor(const pmr_compact_vector& data) : _data{data} {}

  BitPackingDecompressor(const BitPackingDecompressor& other) = default;
  BitPackingDecompressor(BitPackingDecompressor&& other) = default;

  BitPackingDecompressor& operator=(const BitPackingDecompressor& other) {
    if (this == &other) {
      return *this;
    }
    DebugAssert(&_data == &other._data, "Cannot reassign BitPackingDecompressor.");
    return *this;
  }

  BitPackingDecompressor& operator=(BitPackingDecompressor&& other) noexcept {
    DebugAssert(&_data == &other._data, "Cannot reassign BitPackingDecompressor.");
    return *this;
  }

  ~BitPackingDecompressor() override = default;

  uint32_t get(size_t index) final {
    return _data[index];
  }

  size_t size() const final {
    return _data.size();
  }

 private:
  const pmr_compact_vector& _data;
};

}  // namespace hyrise
