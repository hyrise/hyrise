/**
 * Column Encoder Template
 *
 * - Copy this file for new column encoders
 * - Add new encoder to encoders.hpp
 */
#pragma once

#include <memory>

#include "base_column_encoder.hpp"

#include "storage/value_column.hpp"
#include "utils/enum_constant.hpp"
#include "types.hpp"

namespace opossum {

// TODO: Rename class
class ColumnEncoderTemplate : public ColumnEncoder<DictionaryEncoder> {
 public:
  // TODO: Add new encoding to column_encoding_type.hpp and update _encoding_type
  static constexpr auto _encoding_type = enum_c<EncodingType::NewEncoding>;

  template <typename T>
  std::shared_ptr<BaseColumn> _encode(const std::shared_ptr<ValueColumn<T>>& value_column) {
    // TODO: Implement encoding
  }
};

}  // namespace opossum
