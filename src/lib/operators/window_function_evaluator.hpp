#pragma once

#include <string>

#include "abstract_read_only_operator.hpp"

namespace hyrise {

class WindowFunctionEvaluator : public AbstractReadOnlyOperator {
  // TODO: Think about not copying the data, but adding segments to all chunks of the input table

  const std::string& name() const override;
};

}  // namespace hyrise
