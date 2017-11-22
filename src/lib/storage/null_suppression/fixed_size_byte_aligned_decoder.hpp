#pragma once

#include <boost/iterator/transform_iterator.hpp>

#include <type_traits>

#include "base_ns_decoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"

namespace opossum {

/**
 * Implements the non-virtual interface of all decoders
 */
template <typename UnsignedIntType>
class FixedSizeByteAlignedDecoder : public NsDecoder<FixedSizeByteAlignedDecoder<UnsignedIntType>> {
 public:
  using Vector = FixedSizeByteAlignedVector<UnsignedIntType>;

 public:
  explicit FixedSizeByteAlignedDecoder(const Vector& vector) : _vector{vector} {}

  uint32_t _on_get(size_t i) { return _vector.data()[i]; }

  size_t _on_size() const { return _vector.size(); }

  auto _on_cbegin() const { return boost::make_transform_iterator(_vector.data().cbegin(), cast_to_uint32); }

  auto _on_cend() const { return boost::make_transform_iterator(_vector.data().cend(), cast_to_uint32); }

 private:
  static uint32_t cast_to_uint32(UnsignedIntType value) { return static_cast<uint32_t>(value); }

 private:
  const Vector& _vector;
};

}  // namespace opossum
