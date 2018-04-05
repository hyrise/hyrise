#pragma once

#include <memory>

#define CREATE_PTR_ALIASES(name) \
  using name##SPtr = std::shared_ptr<name>; \
  using name##CSPtr = std::shared_ptr<const name>;