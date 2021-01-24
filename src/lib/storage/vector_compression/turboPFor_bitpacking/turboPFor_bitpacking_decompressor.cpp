#include "turboPFor_bitpacking_decompressor.hpp"

#include "turboPFor_bitpacking_vector.hpp"

namespace opossum {

TurboPForBitpackingDecompressor::TurboPForBitpackingDecompressor(const TurboPForBitpackingVector& vector)
    : _data{&vector._data},
      _size{vector._size},
      _b{vector._b} {}

TurboPForBitpackingDecompressor::TurboPForBitpackingDecompressor(const TurboPForBitpackingDecompressor& other)
    : _data{other._data},
      _size{other._size},
      _b{other._b} {}

TurboPForBitpackingDecompressor::TurboPForBitpackingDecompressor(TurboPForBitpackingDecompressor&& other) noexcept
    : _data{other._data},
      _size{other._size},
      _b{other._b} {}

TurboPForBitpackingDecompressor& TurboPForBitpackingDecompressor::operator=(const TurboPForBitpackingDecompressor& other) {
  if (&other == this) {
    return *this;
  }

  _data = other._data;
  _size = other._size;
  _b = other._b;

  return *this;
}

TurboPForBitpackingDecompressor& TurboPForBitpackingDecompressor::operator=(TurboPForBitpackingDecompressor&& other) noexcept {
  if (&other == this) {
    return *this;
  }
  _data = other._data;
  _size = other._size;
  _b = other._b;

  return *this;
}

}  // namespace opossum
