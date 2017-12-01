/**
 * Column Encoder Template
 *
 * - Copy this file for new column encoders
 * - Add new encoder to encoders.hpp
 */
#pragma once

#include <boost/hana/core/to.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <memory>

#include "base_column_encoder.hpp"

#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

// TODO: Rename class
class ColumnEncoderTemplate : public ColumnEncoder<DictionaryEncoder> {
 public:
  // TODO: Specify supported data types (use _all_data_types if all are supported)
  static constexpr auto _supported_types = hana::to_tuple(hana::tuple_t<int32_t, int64_t>);

  template <typename T>
  std::shared_ptr<BaseColumn> _encode(const std::shared_ptr<ValueColumn<T>>& value_column) {
    // TODO: Implement encoding
  }
};

}  // namespace opossum
