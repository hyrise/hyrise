#pragma once

#include <cstdint>
#include <memory>

#include "base_zero_suppression_vector.hpp"
#include "zs_type.hpp"
#include "zs_vector_meta_info.hpp"

#include "types.hpp"

namespace opossum {

std::unique_ptr<BaseZeroSuppressionVector> encode_by_zs_type(ZsType type, const pmr_vector<uint32_t>& vector,
                                                             const PolymorphicAllocator<size_t>& alloc,
                                                             const ZsVectorMetaInfo& meta_info = {});

}  // namespace opossum
