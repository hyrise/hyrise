#pragma once

#include <boost/hana/at_key.hpp>
#include <boost/hana/contains.hpp>
#include <boost/hana/equal.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/type.hpp>

#include <cstdint>

#include "all_type_variant.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

namespace hana = boost::hana;

enum class EncodingType : uint8_t { Unencoded, DeprecatedDictionary, Dictionary, RunLength, FrameOfReference };

/**
 * @brief Maps each encoding type to its supported data types
 *
 * This map ensures that column and encoder templates are only
 * instantiated for supported types and not for all data types.
 *
 * Use data_types if the encoding supports all data types.
 */
constexpr auto supported_data_types_for_type =
    hana::make_map(hana::make_pair(enum_c<EncodingType, EncodingType::DeprecatedDictionary>, data_types),
                   hana::make_pair(enum_c<EncodingType, EncodingType::Dictionary>, data_types),
                   hana::make_pair(enum_c<EncodingType, EncodingType::RunLength>, data_types),
                   hana::make_pair(enum_c<EncodingType, EncodingType::FrameOfReference>, hana::tuple_t<int32_t, int64_t>));

//  Example for an encoding that doesn’t support all data types:
//  hane::make_pair(enum_c<EncodingType, EncodingType::NewEncoding>, hana::tuple_t<int32_t, int64_t>)

/**
 * @return an integral constant implicitly convertible to bool
 *
 * Hint: Use decltype(result)::value if you want to use the result
 *       in a constant expression such as constexpr-if.
 */
template <typename ColumnEncodingType, typename ColumnDataType>
constexpr auto encoding_supports_data_type(ColumnEncodingType encoding_type, ColumnDataType data_type) {
  return hana::contains(hana::at_key(supported_data_types_for_type, encoding_type), data_type);
}

template <EncodingType encoding_type, typename T>
constexpr auto encoding_supports = encoding_supports_data_type(
    enum_c<EncodingType, EncodingType::FrameOfReference>, hana::type_c<T>);

}  // namespace opossum
