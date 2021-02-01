#pragma once
#define TURBOPFOR_DAC

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "bitpack.h"

#include "types.hpp"

namespace opossum {

class TurboPForBitpackingVector;

class TurboPForBitpackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit TurboPForBitpackingDecompressor(const TurboPForBitpackingVector& vector);
  TurboPForBitpackingDecompressor(const TurboPForBitpackingDecompressor&) = default;
  TurboPForBitpackingDecompressor(TurboPForBitpackingDecompressor&&) = default;

      
  TurboPForBitpackingDecompressor& operator=(const TurboPForBitpackingDecompressor& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign FixedSizeByteAlignedDecompressor");
    return *this;
  }

  TurboPForBitpackingDecompressor& operator=(TurboPForBitpackingDecompressor&& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign FixedSizeByteAlignedDecompressor");
    return *this;
  }

  ~TurboPForBitpackingDecompressor() override = default;

  uint32_t get(size_t i) final {
    return _decompressed[i];
  }

  size_t size() const final { return _size; }

 private:
  const pmr_vector<uint8_t>& _data;
  size_t _size;
  uint8_t _b;
  std::vector<uint32_t> _decompressed;
};

}  // namespace opossum
