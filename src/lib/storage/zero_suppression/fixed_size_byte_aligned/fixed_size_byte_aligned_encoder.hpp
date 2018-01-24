#pragma once

#include <algorithm>
#include <limits>

#include "storage/zero_suppression/base_zero_suppression_encoder.hpp"

#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"

namespace opossum {

class FixedSizeByteAlignedEncoder : public BaseZeroSuppressionEncoder {
 public:
  std::unique_ptr<BaseZeroSuppressionVector> encode(const PolymorphicAllocator<size_t>& alloc,
                                                    const pmr_vector<uint32_t>& vector,
                                                    const ZsVectorMetaInfo& meta_info = {}) final;

  std::unique_ptr<BaseZeroSuppressionEncoder> create_new() const final;

 private:
  static uint32_t _find_max_value(const pmr_vector<uint32_t>& vector) const;

  static std::unique_ptr<BaseZeroSuppressionVector> _encode_using_max_value(const PolymorphicAllocator<size_t>& alloc,
                                                                            const pmr_vector<uint32_t>& vector,
                                                                            const uint32_t max_value) const;

  template <typename UnsignedIntType>
  static std::unique_ptr<BaseZeroSuppressionVector> _encode_using_uint_type(const PolymorphicAllocator<size_t>& alloc,
                                                                            const pmr_vector<uint32_t>& vector) const;
};

}  // namespace opossum
