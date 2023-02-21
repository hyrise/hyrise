#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "types.hpp"

namespace hyrise {

template <typename UnsignedIntType>
class FixedWidthIntegerDecompressor : public BaseVectorDecompressor {
 public:
  explicit FixedWidthIntegerDecompressor(const std::span<const UnsignedIntType>& data_span) : _data_span{data_span} {}

  FixedWidthIntegerDecompressor(const FixedWidthIntegerDecompressor&) = default;
  FixedWidthIntegerDecompressor(FixedWidthIntegerDecompressor&&) = default;

  FixedWidthIntegerDecompressor& operator=(const FixedWidthIntegerDecompressor& other) {
    DebugAssert(_data_span.data() == other._data_span.data(), "Cannot reassign FixedWidthIntegerDecompressor");
    return *this;
  }

  FixedWidthIntegerDecompressor& operator=(FixedWidthIntegerDecompressor&& other) {
    DebugAssert(_data_span.data() == other._data_span.data(), "Cannot reassign FixedWidthIntegerDecompressor");
    return *this;
  }

  uint32_t get(size_t i) final {
    // GCC warns here: _data may be used uninitialized in this function [-Werror=maybe-uninitialized]
    // Clang does not complain. Also, _data is a reference, so there should be no way of it being uninitialized.
    // Since gcc's uninitialized-detection is known to be buggy, we ignore that.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

    return _data_span[i];

#pragma GCC diagnostic pop
  }

  size_t size() const final {
    return _data_span.size();
  }

 private:
  const std::span<const UnsignedIntType> _data_span;
};

}  // namespace hyrise
