#pragma once

#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/prepend.hpp>
#include <boost/hana/second.hpp>
#include <boost/hana/transform.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/mpl/push_front.hpp>
#include <boost/variant.hpp>

#include <string>

#include "null_value.hpp"
#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

// This holds pairs of all types and their respective string representation
static constexpr auto column_types =
    hana::make_tuple(hana::make_pair("int", hana::type_c<int32_t>), hana::make_pair("long", hana::type_c<int64_t>),
                     hana::make_pair("float", hana::type_c<float>), hana::make_pair("double", hana::type_c<double>),
                     hana::make_pair("string", hana::type_c<std::string>));  // NOLINT

extern const std::vector<std::string> type_by_all_type_variant_which;

// This holds only the possible data types.
static constexpr auto types = hana::transform(column_types, hana::second);  // NOLINT

static constexpr auto types_including_null = hana::prepend(types, hana::type_c<NullValue>);

// Convert tuple to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(types));

// Append NullValue to mpl vector
using TypesWithNullValue = boost::mpl::push_front<TypesAsMplVector, NullValue>::type;

// Create boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<TypesWithNullValue>::type;

// Function to check if AllTypeVariant is null
inline bool is_null(const AllTypeVariant& variant) { return (variant.which() == 0); }

/**
 * Notes:
 *   – Use this instead of AllTypeVariant{}, AllTypeVariant{NullValue{}}, NullValue{}, etc.
 *     whenever a null value needs to be represented
 *   - comparing any AllTypeVariant to NULL_VALUE returns false in accordance with the ternary logic
 *   - use is_null() if you want to check if an AllTypeVariant is null
 */
static const auto NULL_VALUE = AllTypeVariant{};

}  // namespace opossum
