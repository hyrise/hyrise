#pragma once

#include "base_ns_decoder.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedDecoder : public BaseNsDecoder {
 public:
  explicit FixedSizeByteAlignedDecoder(const pmr_vector<UnsignedIntType>& data) : _data{data} {}
  ~FixedSizeByteAlignedDecoder() final = default;

  uint32_t get(size_t i) final { return _data[i]; }
  size_t size() const final { return _data.size(); }

 private:
  const pmr_vector<UnsignedIntType>& _data;
};

}  // namespace opossum
