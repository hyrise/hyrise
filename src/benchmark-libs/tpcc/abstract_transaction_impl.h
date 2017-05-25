#pragma once

#include <json.hpp>

namespace opossum {

class AbstractTransactionImpl {
 public:
  virtual void run_and_test_transaction_from_json(const nlohmann::json & json_params,
      const nlohmann::json & json_results) = 0;
};

}