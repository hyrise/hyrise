#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedDecompressor : public BaseVectorDecompressor {
 public:
  explicit FixedSizeByteAlignedDecompressor(const pmr_vector<UnsignedIntType>& data) : _data{data} {}
  FixedSizeByteAlignedDecompressor(const FixedSizeByteAlignedDecompressor&) = default;
  FixedSizeByteAlignedDecompressor(FixedSizeByteAlignedDecompressor&&) = default;

  FixedSizeByteAlignedDecompressor& operator=(const FixedSizeByteAlignedDecompressor& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign FixedSizeByteAlignedDecompressor");
    return *this;
  }
  FixedSizeByteAlignedDecompressor& operator=(FixedSizeByteAlignedDecompressor&& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign FixedSizeByteAlignedDecompressor");
    return *this;
  }

  uint32_t get(size_t i) final {
    // GCC warns here: _data may be used uninitialized in this function [-Werror=maybe-uninitialized]
    // Clang does not complain. Also, _data is a reference, so there should be no way of it being uninitialized.
    // Since gcc's uninitialized-detection is known to be buggy, we ignore that.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

    return _data[i];

#pragma GCC diagnostic pop
  }

  size_t size() const final { return _data.size(); }

 private:
  const pmr_vector<UnsignedIntType>& _data;
};

}  // namespace opossum
