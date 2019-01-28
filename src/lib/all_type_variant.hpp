#pragma once

#include <boost/hana/prepend.hpp>
#include <boost/hana/zip.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <variant>

#include "null_value.hpp"
#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

// This corresponds to the DataType enum in types.hpp. We have the list of data types in two different files because
// while including the DataType enum is cheap, building the variant is not. In the past, we had a couple of fancy
// macros here that converted a list of types into all the different items, but including that file took a second
// each time we needed a DataType somewhere.

#define DATA_TYPES (int32_t)(int64_t)(float)(double)(std::string)

static constexpr auto data_type_enum_values =
    hana::make_tuple(DataType::Int, DataType::Long, DataType::Float, DataType::Double, DataType::String);

static constexpr auto data_type_names = hana::make_tuple("int", "long", "float", "double", "string");

static constexpr auto data_types_including_null =
    hana::make_tuple(hana::type_c<NullValue>, hana::type_c<int32_t>, hana::type_c<int64_t>, hana::type_c<float>,
                     hana::type_c<double>, hana::type_c<std::string>);

static constexpr auto data_type_pairs = hana::make_tuple(
    hana::pair(DataType::Int, hana::type_c<int32_t>), hana::pair(DataType::Long, hana::type_c<int64_t>),
    hana::pair(DataType::Float, hana::type_c<float>), hana::pair(DataType::Double, hana::type_c<double>),
    hana::pair(DataType::String, hana::type_c<std::string>));

constexpr auto hana_to_pair = [](auto tuple) { return hana::make_pair(hana::at_c<0>(tuple), hana::at_c<1>(tuple)); };

static constexpr auto data_type_enum_string_pairs =
    hana::transform(hana::zip(data_type_enum_values, data_type_names), hana_to_pair);

using AllTypeVariant = std::variant<NullValue, int32_t, int64_t, float, double, std::string>;

// Function to check if AllTypeVariant is null
inline bool variant_is_null(const AllTypeVariant& variant) { return (variant.index() == 0); }

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

}  // namespace opossum

namespace std {

std::ostream& operator<<(std::ostream&, const opossum::AllTypeVariant&);

}  // namespace std
