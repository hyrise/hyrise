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
  TurboPForBitpackingDecompressor(const TurboPForBitpackingDecompressor& other) = default;
  TurboPForBitpackingDecompressor(TurboPForBitpackingDecompressor&& other) = default;

  TurboPForBitpackingDecompressor& operator=(const TurboPForBitpackingDecompressor& other);
  TurboPForBitpackingDecompressor& operator=(TurboPForBitpackingDecompressor&& other) noexcept;

  ~TurboPForBitpackingDecompressor() override = default;

  uint32_t get(size_t i) final {
    // GCC warns here: _data may be used uninitialized in this function [-Werror=maybe-uninitialized]
    // Clang does not complain. Also, _data is a reference, so there should be no way of it being uninitialized.
    // Since gcc's uninitialized-detection is known to be buggy, we ignore that.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

    auto val = (*_decompressed_data)[i];

    return val;

#pragma GCC diagnostic pop
  }

  size_t size() const final { return _size; }

 private:
  const std::shared_ptr<pmr_vector<uint8_t>> _data;
  std::shared_ptr<std::vector<uint32_t>> _decompressed_data;
  size_t _size;
  uint8_t _b;
};

}  // namespace opossum
