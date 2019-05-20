#include <iostream>

#include "types.hpp"

using namespace opossum;  // NOLINT

// T should always be std::optional<?> here, for non-optional types see template specializations
template <class T>
bool is_null(const T& optional_value) {
  return !optional_value.has_value();
}

template <> constexpr bool is_null<int32_t>(const int32_t&) { return false; }
template <> constexpr bool is_null<int64_t>(const int64_t&) { return false; }
template <> constexpr bool is_null<float>(const float&) { return false; }
template <> constexpr bool is_null<pmr_string>(const pmr_string&) { return false; }

int main() {
  auto a = int32_t{0};
  auto b = int64_t{0};
  auto c = 0.0f;
  auto d = pmr_string{};
  
  auto aa = std::optional{a};
  auto bb = std::optional{b};
  auto cc = std::optional{c};
  auto dd = std::optional{d};

  auto aaa = std::optional<decltype(a)>{};
  auto bbb = std::optional<decltype(b)>{};
  auto ccc = std::optional<decltype(c)>{};
  auto ddd = std::optional<decltype(d)>{};

  assert(is_null(a) == false);
  assert(is_null(b) == false);
  assert(is_null(c) == false);
  assert(is_null(d) == false);

  assert(is_null(aa) == false);
  assert(is_null(bb) == false);
  assert(is_null(cc) == false);
  assert(is_null(dd) == false);

  assert(is_null(aaa) == true);
  assert(is_null(bbb) == true);
  assert(is_null(ccc) == true);
  assert(is_null(ddd) == true);
}
