#include "ns_utils.hpp"

#include <memory>
#include <vector>

#include "ns_vectors.hpp"
#include "ns_encoders.hpp"

#include "utils/assert.hpp"

namespace opossum {

std::unique_ptr<BaseNsEncoder> create_encoder_for_ns_type(NsType type) {
  switch (type) {
    case NsType::FixedSize32ByteAligned:
      return std::make_unique<FixedSizeByteAlignedEncoder<uint32_t>>();
    case NsType::FixedSize16ByteAligned:
      return std::make_unique<FixedSizeByteAlignedEncoder<uint16_t>>();
    case NsType::FixedSize8ByteAligned:
      return std::make_unique<FixedSizeByteAlignedEncoder<uint8_t>>();
    case NsType::SimdBp128:
      return std::make_unique<SimdBp128Encoder>();
    default:
      Fail("Unrecognized NsType encountered.");
      return nullptr;
  }
}

std::unique_ptr<BaseNsVector> encode_by_ns_type(NsType type, const std::vector<uint32_t>& vector) {
  auto encoder = create_encoder_for_ns_type(type);
  encoder->init(vector.size());

  for (const auto& value : vector) {
    encoder->append(value);
  }

  encoder->finish();
  return encoder->get_vector();
}

}  // namespace opossum
