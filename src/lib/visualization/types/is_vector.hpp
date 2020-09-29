#pragma once

namespace opossum {

namespace details {
template <class V>
struct is_vector_helper : std::false_type {};

template <class V>
struct is_vector_helper<std::vector<V>> : std::true_type {};
}  // namespace details

template <class V>
struct is_vector : details::is_vector_helper<typename std::remove_cv<V>::type> {};
}