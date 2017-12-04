#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

#include <memory>

#include "encoded_columns.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief Resolves the type of an encoded column.
 *
 * Since encoded columns are immutable, the function accepts a constant reference.
 */
template <typename ColumnDataType, typename Functor>
void resolve_encoded_column_type(const BaseEncodedColumn& column, const Functor& functor) {
  hana::fold(encoded_column_info_for_type, false, [&](auto match_found, auto pair) {
    const auto encoding_type_c = hana::first(pair);
    const auto encoded_column_info_t = hana::second(pair);

    constexpr auto encoding_type = hana::value(encoding_type_c);

    if (!match_found && (encoding_type == column.encoding_type())) {
      const auto data_type_supported = encoding_supports_data_type(encoding_type_c, hana::type_c<ColumnDataType>);

      // Compile only for supported data types
      if constexpr (decltype(data_type_supported)::value) {
        using EncodedColumnInfoType = typename decltype(encoded_column_info_t)::type;
        using EncodedColumnType = typename EncodedColumnInfoType::template ColumnTemplate<ColumnDataType>;
        functor(static_cast<const EncodedColumnType&>(column));
      }

      return true;
    }

    return match_found;
  });
}

}  // namespace opossum
