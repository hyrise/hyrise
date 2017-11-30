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

  hana::fold(ns_type_encoder_pair, false, [&](auto match_found, auto ns_pair) {
    if (!match_found && (hana::first(ns_pair) == type)) {
      using NsEncoderType = typename decltype(+hana::second(ns_pair))::type;
      encoder = std::make_unique<NsEncoderType>();
      return true;
    }

    return false;
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
