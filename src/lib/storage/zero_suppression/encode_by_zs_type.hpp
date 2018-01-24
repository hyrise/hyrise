#pragma once

#include <cstdint>
#include <memory>

#include "base_zero_suppression_vector.hpp"
#include "zs_type.hpp"
#include "zs_vector_meta_info.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Encodes a vector of uint32_t using a given ZsType
 *
 * @param meta_info optional struct that provides the encoding algorithms with additional information
 */
std::unique_ptr<BaseZeroSuppressionVector> encode_by_zs_type(const pmr_vector<uint32_t>& vector, ZsType type,
                                                             const PolymorphicAllocator<size_t>& alloc,
                                                             const ZsVectorMetaInfo& meta_info = {});

}  // namespace opossum
