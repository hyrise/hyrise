#pragma once

namespace opossum {
template <typename Class, typename T>
struct propertyImpl {
  constexpr propertyImpl(T Class::*aMember, const char* aName) : member{aMember}, name{aName} {}

  using Type = T;

  T Class::*member;
  const char* name;
};

// One could overload this function to accept both a getter and a setter instead of a member.
template <typename Class, typename T>
constexpr auto property(T Class::*member, const char* name) {
  return propertyImpl<Class, T>{member, name};
}
}  // namespace opossum