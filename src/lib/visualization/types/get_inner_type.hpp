#pragma once

namespace opossum {
/**
 * Get the inner type of the nested type
 *
 * Usage example:
 * std::shared_ptr<int> ptr;
 * typedef decltype(get_inner_t(T)) inner_type; // inner_type is int
 * inner_type i = 1; // int i = 1;
 */
template <typename inner, template <typename a> typename outer>
inner get_inner_t(outer<inner>& nested_t) {
  (void)nested_t;
  inner i;
  return i;
}


template <typename inner, template <typename a, typename b> typename outer>
inner get_inner_vec_t(outer<inner, std::allocator<inner>>& nested_t) {
  (void)nested_t;
  inner i;
  return i;
}
}  // namespace opossum