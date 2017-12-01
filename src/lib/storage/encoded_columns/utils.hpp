#pragma once

#include <boost/hana/fold.hpp>

#include <memory>

#include "encoded_columns.hpp"

namespace opossum {

template <typename ColumnDataType, typename Functor>
void resolve_encoded_column_type(const BaseEncodedColumn& column, const Functor& func) {
  hana::fold(encoded_column_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && (hana::first(pair) == column.encoding_type())) {
      using EncodedColumnType = typename decltype(+hana::second(pair))::type;
      functor(static_cast<const EncodedColumnType&>(vector));

      return true;
    }

    return false;
  });
}

}  // namespace opossum
