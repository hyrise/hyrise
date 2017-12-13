#include "ns_utils.hpp"

#include <memory>
#include <vector>

#include "ns_encoders.hpp"

#include "utils/assert.hpp"

namespace opossum {

namespace {

std::unique_ptr<BaseNsEncoder> create_encoder_by_ns_type(NsType type) {
  Assert(type != NsType::Invalid, "NsType must be valid.");

  auto encoder = std::unique_ptr<BaseNsEncoder>{};

  hana::fold(ns_encoder_for_type, false, [&](auto match_found, auto pair) {
    const auto vector_type_c = hana::first(pair);
    const auto encoder_t = hana::second(pair);

    if (!match_found && (hana::value(vector_type_c) == type)) {
      using NsEncoderType = typename decltype(encoder_t)::type;
      encoder = std::make_unique<NsEncoderType>();
      return true;
    }

    return match_found;
  });

  return encoder;
}

}  // namespace

std::unique_ptr<BaseNsVector> encode_by_ns_type(NsType type, const pmr_vector<uint32_t>& vector,
                                                const PolymorphicAllocator<size_t>& alloc) {
  auto encoder = create_encoder_by_ns_type(type);
  return encoder->encode(vector, alloc);
}

}  // namespace opossum
