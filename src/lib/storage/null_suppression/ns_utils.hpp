#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "ns_type.hpp"
#include "base_ns_vector.hpp"


namespace opossum {

class BaseNsEncoder;

std::unique_ptr<BaseNsEncoder> create_encoder_for_ns_type(NsType type);
std::unique_ptr<BaseNsVector> encode_by_ns_type(NsType type, const std::vector<uint32_t>& vector);

template <typename Functor>
void resolve_ns_vector_type(const BaseNsVector& vector, const Functor& func) {
  hana::fold(ns_vector_type_pair, false, [&](auto match_found, auto ns_pair) {
    if (!match_found && (hana::second(ns_pair) == vector.type())) {
      using NsVectorType = typename decltype(+hana::first(ns_pair))::type;
      func(static_cast<const NsVectorType&>(vector));

      return true;
    }

    return false;
  });
}

}  // namespace opossum
