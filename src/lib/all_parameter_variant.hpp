#pragma once

#include <boost/lexical_cast.hpp>
#include <string>

#include "all_type_variant.hpp"
#include "expression/parameter_expression.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * AllParameterVariant holds either an AllTypeVariant, a ColumnID or a Placeholder.
 * It should be used to generalize Opossum operator calls.
 */

// This holds pairs of all types and their respective string representation
static constexpr auto parameter_types =
    hana::make_tuple(hana::make_pair("AllTypeVariant", hana::type_c<AllTypeVariant>),
                     hana::make_pair("ColumnID", hana::type_c<ColumnID>),                      // NOLINT
                     hana::make_pair("LQPColumnReference", hana::type_c<LQPColumnReference>),  // NOLINT
                     hana::make_pair("Parameter", hana::type_c<ParameterID>));                 // NOLINT

// This holds only the possible data types.
static constexpr auto parameter_types_as_hana_sequence = hana::transform(parameter_types, hana::second);  // NOLINT

// Convert tuple to mpl vector
using ParameterTypesAsMplVector =
    decltype(hana::to<hana::ext::boost::mpl::vector_tag>(parameter_types_as_hana_sequence));

// Create boost::variant from mpl vector
using AllParameterVariant = typename boost::make_variant_over<ParameterTypesAsMplVector>::type;

// Function to check if AllParameterVariant is AllTypeVariant
inline bool is_variant(const AllParameterVariant& variant) { return (variant.type() == typeid(AllTypeVariant)); }

// Function to check if AllParameterVariant is a column id
inline bool is_column_id(const AllParameterVariant& variant) { return (variant.type() == typeid(ColumnID)); }

// Function to check if AllParameterVariant is a column origin
inline bool is_lqp_column_reference(const AllParameterVariant& variant) {
  return (variant.type() == typeid(LQPColumnReference));
}

// Function to check if AllParameterVariant is a ParameterID
inline bool is_parameter_id(const AllParameterVariant& variant) { return (variant.type() == typeid(ParameterID)); }

std::string to_string(const AllParameterVariant& x);

}  // namespace opossum
