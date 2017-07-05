#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * AllParameterVariant holds either an AllTypeVariant or a ColumnName.
 * It should be used to generalize Opossum operator calls.
 */

// This holds pairs of all types and their respective string representation
static constexpr auto parameter_types =
    hana::make_tuple(hana::make_pair("AllTypeVariant", hana::type_c<AllTypeVariant>),
                     hana::make_pair("ColumnName", hana::type_c<ColumnName>),      // NOLINT
                     hana::make_pair("Parameter", hana::type_c<ParameterValue>));  // NOLINT

// This holds only the possible data types.
static constexpr auto parameter_types_as_hana_sequence = hana::transform(parameter_types, hana::second);  // NOLINT

// Convert tuple to mpl vector
using ParameterTypesAsMplVector =
    decltype(hana::to<hana::ext::boost::mpl::vector_tag>(parameter_types_as_hana_sequence));

// Create boost::variant from mpl vector
using AllParameterVariant = typename boost::make_variant_over<ParameterTypesAsMplVector>::type;

}  // namespace opossum
