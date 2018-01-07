/**
 * Zero Suppression Vector Template
 *
 * - Copy this file for a new zero suppression vector
 * - Add new vector vectors.hpp and zs_type.hpp
 */
#pragma once

#include <memory>

#include "base_zero_suppression_vector.hpp"

#include "types.hpp"

namespace opossum {

// TODO(you): Rename class
class ZsVectorTemplate : public ZeroSuppressionVector<ZsVectorTemplate> {
 public:
  explicit ZsVectorTemplate(pmr_vector<uint32_t> data) : _data{std::move(data)} {}
  ~ZsVectorTemplate() = default;

  size_t _on_size() const { return _data.size(); }
  size_t _on_data_size() const { return sizeof(uint32_t) * _data.size(); }

  auto _on_create_base_decoder() const { return std::unique_ptr<BaseZeroSuppressionDecoder>{_on_create_decoder()}; }

  // TODO(you): Decoder must inherit from BaseZeroSuppressionDecoder
  auto _on_create_decoder() const { return std::make_unique<ZsVectorTemplate>(_data); }

  // TODO(you): Return a constant forward iterator returning uint32_t
  auto _on_cbegin() const { return _data.cbegin(); }
  auto _on_cend() const { return _data.cend(); }

  std::shared_ptr<BaseZeroSuppressionVector> _on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    auto data_copy = pmr_vector<UnsignedIntType>{_data, alloc};
    return std::allocate_shared<ZsVectorTemplate>(alloc, std::move(data_copy));
  }

 private:
  // TODO(you): Vectors are supposed to be immutable so declare member variables const!
  const pmr_vector<uint32_t> _data;
};

}  // namespace opossum
