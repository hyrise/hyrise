#include "join_nested_loop_outer_resolve.hpp"

#include "types.hpp"

#include "storage/reference_column.hpp"

namespace opossum {

template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<float>, const ReferenceColumn&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<float>, const ValueColumn<float>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<float>, const DictionaryColumn<float>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<float>, const DeprecatedDictionaryColumn<float>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<float>, const RunLengthColumn<float>&) const;

}  // namespace opossum
