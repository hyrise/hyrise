#pragma once

#include <memory>
#include <utility>
#include "../../types.hpp"

/**
 * Use these structs to check at compile time if an object of type is an integral type
 * 
 * Is integral type, if T is std::integral_type<T>::value is true or if T is one of the following types:
 * ChunkID, ChunkOffset, ColumnID, CpuID, NodeId, ValueID
 */
namespace opossum {

namespace details {
template <bool integral, bool chunk_id, bool chunk_offset, bool column_id, bool cpu_id, bool node_id, bool value_id>
struct is_integral_helper : std::true_type {};

template <>
struct is_integral_helper<false, false, false, false, false, false, false> : std::false_type {};

template <typename>
struct is_chunk_id : std::false_type {};

template <>
struct is_chunk_id<ChunkID> : std::true_type {};

template <typename>
struct is_chunk_offset : std::false_type {};

template <>
struct is_chunk_offset<ChunkOffset> : std::true_type {};

template <typename>
struct is_column_id : std::false_type {};

template <>
struct is_column_id<ColumnID> : std::true_type {};

template <typename>
struct is_cpu_id : std::false_type {};

template <>
struct is_cpu_id<CpuID> : std::true_type {};

template <typename>
struct is_node_id : std::false_type {};

template <>
struct is_node_id<NodeID> : std::true_type {};

template <typename>
struct is_value_id : std::false_type {};

template <>
struct is_value_id<ValueID> : std::true_type {};
}  // namespace details

template <class T>
struct is_integral : details::is_integral_helper<std::is_integral<T>::value, details::is_chunk_id<T>::value,
                                                  details::is_chunk_offset<T>::value, details::is_column_id<T>::value,
                                                  details::is_cpu_id<T>::value, details::is_node_id<T>::value,
                                                  details::is_value_id<T>::value> {};
}  // namespace opossum