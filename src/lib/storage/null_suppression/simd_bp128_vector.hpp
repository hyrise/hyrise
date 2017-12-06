#pragma once

#include <emmintrin.h>

#include "base_ns_vector.hpp"
#include "oversized_types.hpp"
#include "simd_bp128_iterator.hpp"

#include "types.hpp"

namespace opossum {

class SimdBp128Decoder;

class SimdBp128Vector : public NsVector<SimdBp128Vector> {
 public:
  using ConstIterator = SimdBp128Iterator;

 public:
  explicit SimdBp128Vector(pmr_vector<uint128_t> vector, size_t size);
  ~SimdBp128Vector() = default;

  const pmr_vector<uint128_t>& data() const;

  size_t _on_size() const;
  size_t _on_data_size() const;

  std::unique_ptr<BaseNsDecoder> _on_create_base_decoder() const;
  std::unique_ptr<SimdBp128Decoder> _on_create_decoder() const;

  ConstIterator _on_cbegin() const;
  ConstIterator _on_cend() const;

  std::shared_ptr<BaseNsVector> _on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:
  friend class SimdBp128Decoder;

  const pmr_vector<uint128_t> _data;
  const size_t _size;
};
}  // namespace opossum
