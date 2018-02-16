#include "join_nested_loop_outer_resolve.hpp"

#include "types.hpp"

#include "storage/reference_column.hpp"

namespace opossum {

template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<std::string>, const ReferenceColumn&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<std::string>, const ValueColumn<std::string>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<std::string>, const DictionaryColumn<std::string>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<std::string>, const DeprecatedDictionaryColumn<std::string>&) const;
template void JoinNestedLoop::OuterResolve::operator()<>(boost::hana::basic_type<std::string>, const RunLengthColumn<std::string>&) const;

}  // namespace opossum
