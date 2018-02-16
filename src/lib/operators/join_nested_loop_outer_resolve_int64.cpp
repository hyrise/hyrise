#include "join_nested_loop_outer_resolve.hpp"

#include "types.hpp"

#include "storage/reference_column.hpp"

namespace opossum {

template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int64_t>, const ReferenceColumn&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int64_t>, const ValueColumn<int64_t>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int64_t>, const DictionaryColumn<int64_t>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int64_t>, const DeprecatedDictionaryColumn<int64_t>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<int64_t>, const RunLengthColumn<int64_t>&) const;

}  // namespace opossum
