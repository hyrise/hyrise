#pragma once

#include <cstddef>
#include <string>

#if __has_include(<optional>)
#include <optional>
#else
#include <experimental/optional>
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
}  // namespace opossum
