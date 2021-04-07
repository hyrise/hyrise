#pragma once

#include "compact_vector.hpp"
#include "fixed_size_bit_aligned_vector_type.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"

namespace opossum {

class FixedSizeBitAlignedVector;

class FixedSizeBitAlignedDecompressor : public BaseVectorDecompressor {
 public:
  explicit FixedSizeBitAlignedDecompressor(const pmr_compact_vector<uint32_t>& data) : _data{&data} {}
  FixedSizeBitAlignedDecompressor(const FixedSizeBitAlignedDecompressor& other) = default;
  FixedSizeBitAlignedDecompressor(FixedSizeBitAlignedDecompressor&& other) = default;

  FixedSizeBitAlignedDecompressor& operator=(const FixedSizeBitAlignedDecompressor& other) {
    DebugAssert(_data == other._data, "Cannot reassign FixedSizeBitAlignedDecompressor");
    return *this;
  }
  FixedSizeBitAlignedDecompressor& operator=(FixedSizeBitAlignedDecompressor&& other) {
    DebugAssert(_data == other._data, "Cannot reassign FixedSizeBitAlignedDecompressor");
    return *this;
  }

  ~FixedSizeBitAlignedDecompressor() override = default;

  uint32_t get(size_t i) final { return (*_data)[i]; }

  size_t size() const final { return _data->size(); }

 private:
  const pmr_compact_vector<uint32_t>* _data;
};

}  // namespace opossum
