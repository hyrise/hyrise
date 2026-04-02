#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "types.hpp"

namespace hyrise {

template <typename UnsignedIntType>
class FixedWidthIntegerDecompressor : public BaseVectorDecompressor {
 public:
  explicit FixedWidthIntegerDecompressor(const pmr_vector<UnsignedIntType>& data) : _data{data} {}

  FixedWidthIntegerDecompressor(const FixedWidthIntegerDecompressor&) = default;
  FixedWidthIntegerDecompressor(FixedWidthIntegerDecompressor&&) = default;

  FixedWidthIntegerDecompressor& operator=(const FixedWidthIntegerDecompressor& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign FixedWidthIntegerDecompressor.");
    return *this;
  }

  FixedWidthIntegerDecompressor& operator=(FixedWidthIntegerDecompressor&& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign FixedWidthIntegerDecompressor.");
    return *this;
  }

  uint32_t get(size_t i) final {
    return _data[i];
  }

  size_t size() const final {
    return _data.size();
  }

 private:
  const pmr_vector<UnsignedIntType>& _data;
};

}  // namespace hyrise
