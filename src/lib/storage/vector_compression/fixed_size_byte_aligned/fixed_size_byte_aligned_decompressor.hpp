#pragma once

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedDecompressor : public BaseVectorDecompressor {
 public:
  explicit FixedSizeByteAlignedDecompressor(const pmr_vector<UnsignedIntType>& data) : _data{data} {}
  ~FixedSizeByteAlignedDecompressor() final = default;

  uint32_t get(size_t i) final { return _data[i]; }
  size_t size() const final { return _data.size(); }

 private:
  const pmr_vector<UnsignedIntType>& _data;
};

}  // namespace opossum
