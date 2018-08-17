#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <boost/iterator/transform_iterator.hpp>

#include <memory>

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "fixed_size_byte_aligned_decompressor.hpp"

#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief Encodes values as either uint32_t, uint16_t, or uint8_t
 *
 * This is simplest vector compression scheme. It matches the old FittedAttributeVector
 */
template <typename UnsignedIntType>
class FixedSizeByteAlignedVector : public CompressedVector<FixedSizeByteAlignedVector<UnsignedIntType>> {
  static_assert(hana::contains(hana::tuple_t<uint8_t, uint16_t, uint32_t>, hana::type_c<UnsignedIntType>),
                "UnsignedIntType must be any of the three listed unsigned integer types.");

 public:
  explicit FixedSizeByteAlignedVector(pmr_vector<UnsignedIntType> data) : _data{std::move(data)} {}
  ~FixedSizeByteAlignedVector() = default;

  const pmr_vector<UnsignedIntType>& data() const { return _data; }

 public:
  size_t on_size() const { return _data.size(); }
  size_t on_data_size() const { return sizeof(UnsignedIntType) * _data.size(); }

  auto on_create_base_decoder() const { return std::unique_ptr<BaseVectorDecompressor>{on_create_decoder()}; }

  auto on_create_decoder() const { return std::make_unique<FixedSizeByteAlignedDecompressor<UnsignedIntType>>(_data); }

  auto on_begin() const {
    if constexpr (std::is_same_v<UnsignedIntType, uint32_t>) {
      return _data.cbegin();
    } else {
      return boost::make_transform_iterator(_data.cbegin(), cast_to_uint32);
    }
  }

  auto on_end() const {
    if constexpr (std::is_same_v<UnsignedIntType, uint32_t>) {
      return _data.cend();
    } else {
      return boost::make_transform_iterator(_data.cend(), cast_to_uint32);
    }
  }

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    auto data_copy = pmr_vector<UnsignedIntType>{_data, alloc};
    return std::make_unique<FixedSizeByteAlignedVector<UnsignedIntType>>(std::move(data_copy));
  }

 private:
  static uint32_t __attribute__((always_inline)) cast_to_uint32(UnsignedIntType value) {
    return static_cast<uint32_t>(value);
  }

 private:
  const pmr_vector<UnsignedIntType> _data;
};

}  // namespace opossum
