#include "turboPFor_bitpacking_decompressor.hpp"

#include "turboPFor_bitpacking_vector.hpp"

namespace opossum {

TurboPForBitpackingDecompressor::TurboPForBitpackingDecompressor(const TurboPForBitpackingVector& vector)
    : 
    _data{vector._data},
    _size{vector._size},
      _b{vector._b} 
      {
        _decompressed = std::vector<uint32_t>(_size + 32);
        if (vector._size == 0) {
          return;
        }
+      bitunpack32(vector._data.data(), vector._size, _decompressed.data(), vector._b);
  }
}  // namespace opossum
