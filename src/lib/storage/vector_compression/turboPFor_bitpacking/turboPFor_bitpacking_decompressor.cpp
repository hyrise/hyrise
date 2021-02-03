#include "turboPFor_bitpacking_decompressor.hpp"

#include "turboPFor_bitpacking_vector.hpp"

namespace opossum {

TurboPForBitpackingDecompressor::TurboPForBitpackingDecompressor(const TurboPForBitpackingVector& vector)
    : 
    _data{vector._data},
    _size{vector._size},
      _b{vector._b} 
      {
      
      }

TurboPForBitpackingDecompressor& TurboPForBitpackingDecompressor::operator=(const TurboPForBitpackingDecompressor& other) {
  if (&other == this) {
    return *this;
  }

  _size = other._size;
  _b = other._b;

  return *this;
}

TurboPForBitpackingDecompressor& TurboPForBitpackingDecompressor::operator=(TurboPForBitpackingDecompressor&& other) noexcept {
  if (&other == this) {
    return *this;
  }
  _size = other._size;
  _b = other._b;

  return *this;
}

}  // namespace opossum
