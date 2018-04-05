#pragma once

#include <memory>

#define CREATE_PTR_ALIASES(name) \
  using name##SPtr = std::shared_ptr<name>; \
  using name##CSPtr = std::shared_ptr<const name>; \
  using name##WPtr = std::weak_ptr<name>; \
  using name##CWPtr = std::weak_ptr<const name>;

#define CREATE_TEMPLATE_PTR_ALIASES(name) \
  template<typename T> using name##SPtr = std::shared_ptr<name<T>>; \
  template<typename T> using name##CSPtr = std::shared_ptr<const name<T>>; \
  template<typename T> using name##WPtr = std::weak_ptr<name<T>>; \
  template<typename T> using name##CWPtr = std::weak_ptr<const name<T>>;

