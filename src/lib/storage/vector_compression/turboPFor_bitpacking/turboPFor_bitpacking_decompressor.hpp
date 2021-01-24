#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "bitpack.h"

#include "types.hpp"

namespace opossum {

class TurboPForBitpackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit TurboPForBitpackingDecompressor(const pmr_vector<uint8_t>& data, const size_t size, const uint8_t b) : _data{data}, _size{size} {
    _b = b;
  }
  
  TurboPForBitpackingDecompressor(const TurboPForBitpackingDecompressor&) = default;
  TurboPForBitpackingDecompressor(TurboPForBitpackingDecompressor&&) = default;

  TurboPForBitpackingDecompressor& operator=(const TurboPForBitpackingDecompressor& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign TurboPForBitpackingDecompressor");
    return *this;
  }
  TurboPForBitpackingDecompressor& operator=(TurboPForBitpackingDecompressor&& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign TurboPForBitpackingDecompressor");
    return *this;
  }

  uint32_t get(size_t i) final {
    // GCC warns here: _data may be used uninitialized in this function [-Werror=maybe-uninitialized]
    // Clang does not complain. Also, _data is a reference, so there should be no way of it being uninitialized.
    // Since gcc's uninitialized-detection is known to be buggy, we ignore that.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

    return bitgetx32(_data.data(), i, _b);;

#pragma GCC diagnostic pop
  }

  size_t size() const final { return _size; }

 private:
  const pmr_vector<uint8_t>& _data;
  uint8_t _b;
  const size_t _size;
};

}  // namespace opossum
