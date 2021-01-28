#pragma once
#define TURBOPFOR_DAC

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "bitpack.h"

#include "types.hpp"

#include "turboPFor_bitpacking_vector.hpp"

namespace opossum {

class TurboPForBitpackingVector;

class TurboPForBitpackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit TurboPForBitpackingDecompressor(const TurboPForBitpackingVector& vector);
  TurboPForBitpackingDecompressor(const TurboPForBitpackingDecompressor& other);
  TurboPForBitpackingDecompressor(TurboPForBitpackingDecompressor&& other) noexcept;

  TurboPForBitpackingDecompressor& operator=(const TurboPForBitpackingDecompressor& other);
  TurboPForBitpackingDecompressor& operator=(TurboPForBitpackingDecompressor&& other) noexcept;

  ~TurboPForBitpackingDecompressor() override = default;

  uint32_t get(size_t i) final {
    // GCC warns here: _data may be used uninitialized in this function [-Werror=maybe-uninitialized]
    // Clang does not complain. Also, _data is a reference, so there should be no way of it being uninitialized.
    // Since gcc's uninitialized-detection is known to be buggy, we ignore that.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

    // if (_size == 0) {
    //   std::cout << "error" << std::endl;
    // }

    // auto v1 = bitgetx32(_data->data(), i, _b);

    auto val = (*_decompressed_data)[i];

    // if (v1 != val) {
    //   std::cout << "error" << std::endl;
    // }

    return val;

#pragma GCC diagnostic pop
  }

  size_t size() const final { return _size; }

 private:
  const pmr_vector<uint8_t> *_data;
  std::unique_ptr<std::vector<uint32_t>> _decompressed_data;
  size_t _size;
  uint8_t _b;
};

}  // namespace opossum
