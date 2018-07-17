#pragma once

#include <boost/hana/core/to.hpp>
#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/prepend.hpp>
#include <boost/hana/transform.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/zip.hpp>

#include <boost/mpl/push_front.hpp>

#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/size.hpp>
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

// clang-format off
#define DATA_TYPE_INFO                  \
  ((int32_t,     Int,        "int"))    \
  ((int64_t,     Long,       "long"))   \
  ((float,       Float,      "float"))  \
  ((double,      Double,     "double")) \
  ((std::string, String,     "string"))
// Type          Enum Value   String
// clang-format on

#define NUM_DATA_TYPES BOOST_PP_SEQ_SIZE(DATA_TYPE_INFO)

#define GET_ELEM(s, index, elem) BOOST_PP_TUPLE_ELEM(NUM_DATA_TYPES, index, elem)
#define APPEND_ENUM_NAMESPACE(s, d, enum_value) DataType::enum_value

#define DATA_TYPES BOOST_PP_SEQ_TRANSFORM(GET_ELEM, 0, DATA_TYPE_INFO)
#define DATA_TYPE_ENUM_VALUES BOOST_PP_SEQ_TRANSFORM(GET_ELEM, 1, DATA_TYPE_INFO)
#define DATA_TYPE_STRINGS BOOST_PP_SEQ_TRANSFORM(GET_ELEM, 2, DATA_TYPE_INFO)

// We use a boolean data type in the JitOperatorWrapper.
// However, adding it to DATA_TYPE_INFO would trigger many unnecessary template instantiations for all other operators
// and should thus be avoided for compilation performance reasons.
// We thus only add "Bool" to the DataType enum and define JIT_DATA_TYPE_INFO (with a boolean data type) in
// "lib/operators/jit_operator/jit_types.hpp".
// We need to append to the end of the enum to not break the matching of indices between DataType and AllTypeVariant.
enum class DataType : uint8_t { Null, BOOST_PP_SEQ_ENUM(DATA_TYPE_ENUM_VALUES), Bool };

static constexpr auto data_types = hana::to_tuple(hana::tuple_t<BOOST_PP_SEQ_ENUM(DATA_TYPES)>);
static constexpr auto data_type_enum_values =
    hana::make_tuple(BOOST_PP_SEQ_ENUM(BOOST_PP_SEQ_TRANSFORM(APPEND_ENUM_NAMESPACE, _, DATA_TYPE_ENUM_VALUES)));
static constexpr auto data_type_strings = hana::make_tuple(BOOST_PP_SEQ_ENUM(DATA_TYPE_STRINGS));

constexpr auto to_pair = [](auto tuple) { return hana::make_pair(hana::at_c<0>(tuple), hana::at_c<1>(tuple)); };

static constexpr auto data_type_enum_pairs = hana::transform(hana::zip(data_type_enum_values, data_types), to_pair);
static constexpr auto data_type_enum_string_pairs =
    hana::transform(hana::zip(data_type_enum_values, data_type_strings), to_pair);

// Prepends NullValue to tuple of types
static constexpr auto data_types_including_null = hana::prepend(data_types, hana::type_c<NullValue>);

// Converts tuple to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(data_types_including_null));

// Creates boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<detail::TypesAsMplVector>::type;

}  // namespace detail

static constexpr auto data_types = detail::data_types;
static constexpr auto data_types_including_null = detail::data_types_including_null;
static constexpr auto data_type_pairs = detail::data_type_enum_pairs;
static constexpr auto data_type_enum_string_pairs = detail::data_type_enum_string_pairs;

using DataType = detail::DataType;
using AllTypeVariant = detail::AllTypeVariant;

// Function to check if AllTypeVariant is null
inline bool variant_is_null(const AllTypeVariant& variant) { return (variant.which() == 0); }

bool is_floating_point_data_type(const DataType data_type);

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

/**
 * Checks whether two variants are equal, except when they contain float/double. In this case check whether they are
 * near, e.g. withing a certain absolute difference from each other.
 */
bool all_type_variant_near(const AllTypeVariant& lhs, const AllTypeVariant& rhs, double max_abs_error = 0.001);

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::AllTypeVariant> {
  size_t operator()(const opossum::AllTypeVariant& all_type_variant) const;
};

}  // namespace std
