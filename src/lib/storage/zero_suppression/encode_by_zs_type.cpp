#include "encode_by_zs_type.hpp"

#include <map>
#include <memory>

#include "utils/assert.hpp"

#include "fixed_size_byte_aligned/fixed_size_byte_aligned_encoder.hpp"
#include "simd_bp128/simd_bp128_encoder.hpp"

namespace opossum {

namespace {

/**
 * @brief Mapping of zero suppression types to encoders
 *
 * Add your zero suppression encoder here!
 */
static const auto zs_encoder_for_type = std::map<ZsType, std::shared_ptr<BaseZeroSuppressionEncoder>>{
    {ZsType::FixedSizeByteAligned, std::make_shared<FixedSizeByteAlignedEncoder>()},
    {ZsType::SimdBp128, std::make_shared<SimdBp128Encoder>()}};

std::unique_ptr<BaseZeroSuppressionEncoder> create_encoder_by_zs_type(ZsType type) {
  Assert(type != ZsType::Invalid, "ZsType must be valid.");

  auto it = zs_encoder_for_type.find(type);
  Assert(it != zs_encoder_for_type.cend(), "All zero suppression types must be in zs_encoder_for_type.");

  const auto& encoder = it->second;
  return encoder->create_new();
}

}  // namespace

std::unique_ptr<BaseZeroSuppressionVector> encode_by_zs_type(const pmr_vector<uint32_t>& vector, ZsType type,
                                                             const PolymorphicAllocator<size_t>& alloc,
                                                             const ZsVectorMetaInfo& meta_info) {
  auto encoder = create_encoder_by_zs_type(type);
  return encoder->encode(vector, alloc, meta_info);
}

}  // namespace opossum
