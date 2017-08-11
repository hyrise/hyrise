#pragma once

#include <cstddef>
#include <string>

#if __has_include(<optional>)
#include <optional>
#else
#include <experimental/optional>
#endif

#if __has_include(<string_view>)
#include <string_view>
#else
#include <experimental/string_view>
#endif

namespace opossum {
using std::to_string;

#if __has_include(<optional>)
template <class T>
using optional = ::std::optional<T>;
static auto nullopt = ::std::nullopt;
#else
template <class T>
using optional = ::std::experimental::optional<T>;
static auto nullopt = ::std::experimental::nullopt;
#endif

#if __has_include(<string_view>)
using string_view = ::std::string_view;
#else
using string_view = ::std::experimental::string_view;
#endif
}  // namespace opossum
