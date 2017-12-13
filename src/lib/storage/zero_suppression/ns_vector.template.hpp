/**
 * Zero Suppression Vector Template
 *
 * - Copy this file for a new zero suppression vector
 * - Add new vector ns_vectors.hpp and ns_type.hpp
 */
#pragma once

#include <memory>

#include "base_ns_vector.hpp"

#include "types.hpp"

namespace opossum {

// TODO(you): Rename class
class NsVectorTemplate : public NsVector<NsVectorTemplate> {
 public:
  explicit NsVectorTemplate(pmr_vector<uint32_t> data) : _data{std::move(data)} {}
  ~NsVectorTemplate() = default;

  size_t _on_size() const { return _data.size(); }
  size_t _on_data_size() const { return sizeof(uint32_t) * _data.size(); }

  auto _on_create_base_decoder() const { return std::unique_ptr<BaseNsDecoder>{_on_create_decoder()}; }

  // TODO(you): Decoder must inherit from BaseNsDecoder
  auto _on_create_decoder() const { return std::make_unique<NsDecoderTemplate>(_data); }

  // TODO(you): Return a constant forward iterator returning uint32_t
  auto _on_cbegin() const { return _data.cbegin(); }
  auto _on_cend() const { return _data.cend(); }

  std::shared_ptr<BaseNsVector> _on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    auto data_copy = pmr_vector<UnsignedIntType>{_data, alloc};
    return std::allocate_shared<NsVectorTemplate>(alloc, std::move(data_copy));
  }

 private:
  // TODO(you): Vectors are supposed to be immutable so declare member variables const!
  const pmr_vector<uint32_t> _data;
};

}  // namespace opossum
