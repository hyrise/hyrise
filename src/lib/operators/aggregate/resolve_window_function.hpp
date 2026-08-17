#pragma once

#include <type_traits>

#include "expression/window_function_expression.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Resolve the window function and pass it to the functor as a compile-time constant.
template <typename Functor>
void resolve_window_function(const WindowFunction window_function, const Functor&& functor) {
  switch (window_function) {
    case WindowFunction::Min:
      functor(std::integral_constant<WindowFunction, WindowFunction::Min>{});
      break;
    case WindowFunction::Max:
      functor(std::integral_constant<WindowFunction, WindowFunction::Max>{});
      break;
    case WindowFunction::Sum:
      functor(std::integral_constant<WindowFunction, WindowFunction::Sum>{});
      break;
    case WindowFunction::Count:
      functor(std::integral_constant<WindowFunction, WindowFunction::Count>{});
      break;
    case WindowFunction::Avg:
      functor(std::integral_constant<WindowFunction, WindowFunction::Avg>{});
      break;
    case WindowFunction::CountDistinct:
      functor(std::integral_constant<WindowFunction, WindowFunction::CountDistinct>{});
      break;
    case WindowFunction::Any:
      functor(std::integral_constant<WindowFunction, WindowFunction::Any>{});
      break;
    default:
      Fail("Unsupported aggregate function.");
  }
}

}  // namespace hyrise
