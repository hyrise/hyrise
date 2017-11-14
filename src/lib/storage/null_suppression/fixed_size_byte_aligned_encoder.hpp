#pragma once

#include "base_attribute_encoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"


namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedEncoder : public BaseAttributeEncoder {
 public:
  void init(size_t size) final;
  void append(uint32_t value) final;
  void finish() final;
  std::unique_ptr<BaseEncodedVector> get_vector() final;

 private:
  pmr::vector<UnsignedIntType> _data;
};

template <typename UnsignedIntType>
void FixedSizeByteAlignedEncoder::init(size_t size) {
  _data = pmr::vector<UnsignedIntType>{};
  _data.reserve(size);
}

template <typename UnsignedIntType>
void FixedSizeByteAlignedEncoder::append(uint32_t value) {
  _data.push_back(static_cast<UnsignedIntType>(value));
}

template <typename UnsignedIntType>
void FixedSizeByteAlignedEncoder::finish() final {}

template <typename UnsignedIntType>
std::unique_ptr<BaseEncodedVector> FixedSizeByteAlignedEncoder::get_vector() {
  return std::make_unique<FixedSizeByteAlignedVector>(std::move(_data));
}

}  // namespace opossum