#include "turboPFor_bitpacking_decompressor.hpp"

#include "turboPFor_bitpacking_vector.hpp"

namespace opossum {

TurboPForBitpackingDecompressor::TurboPForBitpackingDecompressor(const TurboPForBitpackingVector& vector)
    : 
    _decompressed_data{std::make_shared<std::vector<uint32_t>>(vector._size + 32)},
    _data{vector._data},
    _size{vector._size},
      _b{vector._b} 
      {
          if (vector._size == 0) {
            return;
          }

          bitunpack32(_data->data(), vector._size, _decompressed_data->data(), vector._b);
      }

TurboPForBitpackingDecompressor& TurboPForBitpackingDecompressor::operator=(const TurboPForBitpackingDecompressor& other) {
  if (&other == this) {
    return *this;
  }

  _decompressed_data = other._decompressed_data;
  _size = other._size;
  _b = other._b;

  return *this;
}

TurboPForBitpackingDecompressor& TurboPForBitpackingDecompressor::operator=(TurboPForBitpackingDecompressor&& other) noexcept {
  if (&other == this) {
    return *this;
  }
  _decompressed_data = other._decompressed_data;
  _size = other._size;
  _b = other._b;

  return *this;
}

}  // namespace opossum
