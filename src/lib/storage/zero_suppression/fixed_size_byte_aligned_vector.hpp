#pragma once

#include <boost/iterator/transform_iterator.hpp>

#include <memory>

#include "base_ns_vector.hpp"
#include "fixed_size_byte_aligned_decoder.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedVector : public NsVector<FixedSizeByteAlignedVector<UnsignedIntType>> {
 public:
  explicit FixedSizeByteAlignedVector(pmr_vector<UnsignedIntType> data) : _data{std::move(data)} {}
  ~FixedSizeByteAlignedVector() = default;

  size_t _on_size() const { return _data.size(); }
  size_t _on_data_size() const { return sizeof(UnsignedIntType) * _data.size(); }

  auto _on_create_base_decoder() const { return std::unique_ptr<BaseZeroSuppressionDecoder>{_on_create_decoder()}; }

  auto _on_create_decoder() const { return std::make_unique<FixedSizeByteAlignedDecoder<UnsignedIntType>>(_data); }

  auto _on_cbegin() const { return boost::make_transform_iterator(_data.cbegin(), cast_to_uint32); }

  auto _on_cend() const { return boost::make_transform_iterator(_data.cend(), cast_to_uint32); }

  std::shared_ptr<BaseNsVector> _on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    auto data_copy = pmr_vector<UnsignedIntType>{_data, alloc};
    return std::allocate_shared<FixedSizeByteAlignedVector<UnsignedIntType>>(alloc, std::move(data_copy));
  }

 private:
  static uint32_t cast_to_uint32(UnsignedIntType value) { return static_cast<uint32_t>(value); }

 private:
  const pmr_vector<UnsignedIntType> _data;
};

}  // namespace opossum
