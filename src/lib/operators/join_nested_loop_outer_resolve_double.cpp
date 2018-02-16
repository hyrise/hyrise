#include "join_nested_loop_outer_resolve.hpp"

#include "types.hpp"

#include "storage/reference_column.hpp"

namespace opossum {

template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<double>, const ReferenceColumn&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<double>, const ValueColumn<double>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<double>, const DictionaryColumn<double>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<double>, const DeprecatedDictionaryColumn<double>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<double>, const RunLengthColumn<double>&) const;

}  // namespace opossum
