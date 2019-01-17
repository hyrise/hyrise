#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/type.hpp>

#include "all_type_variant.hpp"                         // TODO Can we forward declare this?
#include "logical_query_plan/lqp_column_reference.hpp"  // NEEDEDINCLUDE

namespace opossum {

namespace hana = boost::hana;

/**
 * AllParameterVariant holds either an AllTypeVariant, a ColumnID or a Placeholder.
 * It should be used to generalize Opossum operator calls.
 */

// Create boost::variant from mpl vector
using AllParameterVariant = boost::variant<AllTypeVariant, ColumnID, ParameterID>;

// Function to check if AllParameterVariant is AllTypeVariant
inline bool is_variant(const AllParameterVariant& variant) { return (variant.type() == typeid(AllTypeVariant)); }

// Function to check if AllParameterVariant is a column id
inline bool is_column_id(const AllParameterVariant& variant) { return (variant.type() == typeid(ColumnID)); }

// Function to check if AllParameterVariant is a ParameterID
inline bool is_parameter_id(const AllParameterVariant& variant) { return (variant.type() == typeid(ParameterID)); }

std::string to_string(const AllParameterVariant& x);

}  // namespace opossum
