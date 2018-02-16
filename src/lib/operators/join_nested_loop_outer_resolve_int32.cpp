#include "join_nested_loop_outer_resolve.hpp"

#include "types.hpp"

#include "storage/reference_column.hpp"

namespace opossum {

template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int32_t>, const ReferenceColumn&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int32_t>, const ValueColumn<int32_t>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int32_t>, const DictionaryColumn<int32_t>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int32_t>, const DeprecatedDictionaryColumn<int32_t>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int32_t>, const RunLengthColumn<int32_t>&) const;

}  // namespace opossum
