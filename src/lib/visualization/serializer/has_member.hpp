#pragma once

#include <type_traits>

// see https://stackoverflow.com/questions/64139547/how-to-detect-whether-there-is-a-specific-private-member-variable-in-class?noredirect=1#comment113427202_64139547

namespace opossum {

/**
 * Namespcae details contains helper structs.
 * 
 * Helper structs have provide the required type as default.
 */
namespace details {
struct has_member_properties_helper {
  int properties;
};

/**
 * True if T has attribute properties (even if properties is a private member of T)
 * 
 * Usage: 
 * if constexpr (has_member_properties<SomeClass>::value) {
 *  // use SomeClass::properties
 * } else {
 *  // SomeClass does not have properties member
 * }
 */
template <typename T>
class has_member_properties : public T, has_member_properties_helper {
  /**
   * if T has properties member, has_member_properties will inherit properties member
   * from T and has_member_properties_helper. This is invalid. Check will be discared
   * based on SFINAE. If that happens, only true type will remain.
  */
  template <typename U = has_member_properties, typename = decltype(U::properties)>
  static constexpr std::false_type check(int);

  // will be called on check(0), if check(int) does not exist
  static constexpr std::true_type check(long);

  /***
   * If T does not have member properties, then both check(int) and check(long)
   * will remain. Type will be false type, because it uses check(int) since we pass 0.
   */
  using type = decltype(check(0));

 public:
  static constexpr auto value = type::value;
};

struct has_member__type_helper {
  int _type;
};

/**
 * True if T has attribute properties (even if _type is a private member of T)
 * 
 * Usage: 
 * if constexpr (has_member_properties<SomeClass>::value) {
 *  // use SomeClass::_type
 * } else {
 *  // SomeClass does not have properties member
 * }
 */
template <typename T>
class has_member__type : public T, has_member__type_helper {
  template <typename U = has_member__type, typename = decltype(U::_type)>
  static constexpr std::false_type check(int);

  static constexpr std::true_type check(long);

  using type = decltype(check(0));

 public:
  static constexpr auto value = type::value;
};
}  // namespace details

// wraps details::has_member_properties, to prevent error by inheriting from a non-class type
template <typename T>
struct has_member_properties {
  static constexpr auto value = []() {
    if constexpr (std::is_class<T>::value) {
      return details::has_member_properties<T>::value;
    } else {
      return false;
    }
  }();
};

template <typename T>
struct has_member__type {
  static constexpr auto value = []() {
    if constexpr (std::is_class<T>::value) {
      return details::has_member__type<T>::value;
    } else {
      return false;
    }
  }();
};

}  // namespace opossum