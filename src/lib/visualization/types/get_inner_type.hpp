#pragma once
namespace opossum {
namespace details {
template <typename>
struct inner;

template <template <typename> class outter_t, typename inner_t>
struct inner<outter_t<inner_t>> {
  typedef typename std::remove_cv<typename std::remove_reference<inner_t>::type>::type type;
};

template <typename T>
auto get_vec_element = []() {
  T t;
  return t.data();
};
}  // namespace details

/**
 * Get the inner type of the nested type
 *
 * Usage example:
 * std::shared_ptr<int> ptr;
 * typedef get_inner_t<decltype(ptr)> inner_t;
 * inner_t i = 1; // int i = 1;
 */
template <typename _t>
using get_inner_t = typename details::inner<_t>::type;


/**
 * Get the type of the object that is stored in a vector
 *
 * Usage example:
 * std::vector<int> v; 
 * typedef get_inner_vec_t<decltype(v)> inner_t;
 * inner_t i = 1; // int i = 1;
 */
template <typename T>
using get_inner_vec_t = std::remove_pointer_t<decltype(details::get_vec_element<T>())>;
}  // namespace opossum
