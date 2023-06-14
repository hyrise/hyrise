#include "window_function_evaluator.hpp"

namespace hyrise {

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
}

}  // namespace hyrise
