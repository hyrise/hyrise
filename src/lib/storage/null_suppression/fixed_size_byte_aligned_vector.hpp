#pragma once

#include "base_ns_vector.hpp"
#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedVector : public BaseNsVector {
 public:
  FixedSizeByteAlignedVector(pmr_vector<uint32_t> vector);
  ~FixedSizeByteAligned() override = default;

  uint32_t get(const size_t i) const final;
  size_t size() const final;

  const pmr_vector<UnsignedIntType>& data() const;

 private:
  const pmr_vector<UnsignedIntType> _data;
};


template <typename UnsignedIntType>
FixedSizeByteAlignedVector::FixedSizeByteAlignedVector(pmr_vector<UnsignedIntType> vector) : _data{std::move(vector)} {}

template <typename UnsignedIntType>
uint32_t FixedSizeByteAlignedVector::get(const size_t i) const {
  return _data[i];
}

template <typename UnsignedIntType>
size_t FixedSizeByteAlignedVector::size() const {
  return _data.size();
}

}  // namespace opossum