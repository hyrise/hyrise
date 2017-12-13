#include "zs_utils.hpp"

#include <memory>
#include <vector>

#include "encoders.hpp"

#include "utils/assert.hpp"

namespace opossum {

namespace {

std::unique_ptr<BaseZeroSuppressionEncoder> create_encoder_by_zs_type(ZsType type) {
  Assert(type != ZsType::Invalid, "ZsType must be valid.");

  auto encoder = std::unique_ptr<BaseZeroSuppressionEncoder>{};

  hana::fold(zs_encoder_for_type, false, [&](auto match_found, auto pair) {
    const auto vector_type_c = hana::first(pair);
    const auto encoder_t = hana::second(pair);

    if (!match_found && (hana::value(vector_type_c) == type)) {
      using ZsEncoderType = typename decltype(encoder_t)::type;
      encoder = std::make_unique<ZsEncoderType>();
      return true;
    }

    return match_found;
  });

  return encoder;
}

}  // namespace

std::unique_ptr<BaseZeroSuppressionVector> encode_by_zs_type(ZsType type, const pmr_vector<uint32_t>& vector,
                                                const PolymorphicAllocator<size_t>& alloc) {
  auto encoder = create_encoder_by_zs_type(type);
  return encoder->encode(vector, alloc);
}

}  // namespace opossum
