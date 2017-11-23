#pragma once

#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <boost/hana/prepend.hpp>
#include <boost/hana/transform.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/zip.hpp>
#include <boost/mpl/push_front.hpp>
#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/transform.hpp>
#include <boost/variant.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "null_value.hpp"
#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

namespace detail {

#define EXPAND_TO_HANA_TYPE(s, data, elem) boost::hana::type_c<elem>

// clang-format off
#define DATA_TYPES                     (int32_t) (int64_t) (float)  (double)  (std::string)  // NOLINT
enum class DataType : uint8_t { Null, Int,      Long,     Float,   Double,   String };     // NOLINT
// clang-format on

static constexpr auto data_type_enum_values =
    hana::make_tuple(DataType::Int, DataType::Long, DataType::Float, DataType::Double, DataType::String);

// Extends to hana::make_tuple(hana::type_c<int32_t>, hana::type_c<int64_t>, ...);
static constexpr auto data_types =
    hana::make_tuple(BOOST_PP_SEQ_ENUM(BOOST_PP_SEQ_TRANSFORM(EXPAND_TO_HANA_TYPE, _, DATA_TYPES)));

/**
 * Holds pairs of all types and their respective enum representation.
 *
 * Equivalent to:
 * hana::make_tuple(hana::make_tuple(DataType::Int, hana::type_c<int32_t>),
 *                  hana::make_tuple(DataType::Long, hana::type_c<int64_t>),
 *                  ...);
 */
static constexpr auto data_type_pairs_as_tuples = hana::zip(data_type_enum_values, data_types);

struct to_pair {
  template <typename T>
  constexpr auto operator()(T tuple) {
    return hana::make_pair(hana::at_c<0>(tuple), hana::at_c<1>(tuple));  // NOLINT
  }
};

// Converts the tuples into pairs
static constexpr auto data_type_pairs = hana::transform(data_type_pairs_as_tuples, to_pair{});  // NOLINT

// Prepends NullValue to tuple of types
static constexpr auto data_types_including_null = hana::prepend(data_types, hana::type_c<NullValue>);

// Converts tuple to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(data_types_including_null));

// Creates boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<detail::TypesAsMplVector>::type;

}  // namespace detail

static constexpr auto data_types = detail::data_types;
static constexpr auto data_types_including_null = detail::data_types_including_null;
static constexpr auto data_type_pairs = detail::data_type_pairs;

using DataType = detail::DataType;
using AllTypeVariant = detail::AllTypeVariant;

// Function to check if AllTypeVariant is null
inline bool variant_is_null(const AllTypeVariant& variant) { return (variant.which() == 0); }

/**
 * Notes:
 *   – Use this instead of AllTypeVariant{}, AllTypeVariant{NullValue{}}, NullValue{}, etc.
 *     whenever a null value needs to be represented
 *   - comparing any AllTypeVariant to NULL_VALUE returns false in accordance with the ternary logic
 *   - use variant_is_null() if you want to check if an AllTypeVariant is null
 */
static const auto NULL_VALUE = AllTypeVariant{};

/**
 * @defgroup Macros for explicitly instantiating template classes
 *
 * In order to improve compile times, we explicitly instantiate
 * template classes which are going to be used with column types.
 * Because we do not want any redundant lists of column types spread
 * across the code base, we use EXPLICITLY_INSTANTIATE_DATA_TYPES.
 *
 * @{
 */

#define EXPLICIT_INSTANTIATION(r, template_class, type) template class template_class<type>;

// Explicitly instantiates the given template class for all types in DATA_TYPES
#define EXPLICITLY_INSTANTIATE_DATA_TYPES(template_class)                   \
  BOOST_PP_SEQ_FOR_EACH(EXPLICIT_INSTANTIATION, template_class, DATA_TYPES) \
  static_assert(true, "End call of macro with a semicolon")

/**@}*/

}  // namespace opossum
