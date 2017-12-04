#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "storage/encoded_columns/column_encoding_type.hpp"

namespace opossum {

class BaseColumn;
class BaseColumnEncoder;
class BaseValueColumn;

/**
 * @brief Creates an encoder by encoding type
 */
std::unique_ptr<BaseColumnEncoder> create_encoder(EncodingType encoding_type);

/**
 * @brief Encodes a value column by the given encoding method
 *
 * @return encoded column if data type is supported else nullptr
 */
std::shared_ptr<BaseColumn> encode_column(EncodingType encoding_type, DataType data_type,
                                          std::shared_ptr<BaseValueColumn> column);

}  // namespace opossum
