#pragma once

#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/second.hpp>
#include <boost/hana/transform.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/variant.hpp>

#include <string>

#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

// This holds pairs of all types and their respective string representation
static constexpr auto column_types =
    hana::make_tuple(hana::make_pair("int", hana::type_c<int32_t>), hana::make_pair("long", hana::type_c<int64_t>),
                     hana::make_pair("float", hana::type_c<float>), hana::make_pair("double", hana::type_c<double>),
                     hana::make_pair("string", hana::type_c<std::string>));

// This holds only the possible data types.
static constexpr auto types = hana::transform(column_types, hana::second);

// Convert tuple to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(types));

// Create boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<TypesAsMplVector>::type;

}  // namespace opossum
