#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"
#include "storage/zero_suppression/zs_type.hpp"

namespace opossum {

class BaseEncodedColumn;
class BaseColumnEncoder;
class BaseValueColumn;

/**
 * @brief Creates an encoder by encoding type
 */
std::unique_ptr<BaseColumnEncoder> create_encoder(EncodingType encoding_type);

/**
 * @brief Encodes a value column by the given encoding method
 *
 * @return encoded column if data type is supported else throws exception
 */
std::shared_ptr<BaseEncodedColumn> encode_column(EncodingType encoding_type, DataType data_type,
                                                 std::shared_ptr<const BaseValueColumn> column,
                                                 std::optional<ZsType> zero_suppression_type = {});

}  // namespace opossum
