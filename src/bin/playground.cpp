
#include <iostream>
#include <memory>
#include <type_traits>
#include <typeinfo>

#include <boost/hana/type.hpp>

#include "resolve_column_type.hpp"

using namespace opossum;

template <typename T>
void method(T&& val) {
  if
    constexpr(decltype(val)::is_reference_column()) { std::cout << "Hallo" << std::endl; }
}

int main() {
  auto column = ValueColumn<int>{};
  column.append(42);

  resolve_column_type("int", column, [](auto& column) { std::cout << typeid(column).name() << std::endl; });

  method(column);
}
