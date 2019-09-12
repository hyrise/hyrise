#pragma once

#include <memory>
#include "storage/table.hpp"
#include "postgres_handler.hpp"
#include "operators/abstract_operator.hpp"


namespace opossum {


class ResponseBuilder {
 public:
  static void build_row_description(std::shared_ptr<const Table> table, std::shared_ptr<PostgresHandler> postgres_handler);
    
  static std::string build_command_complete_message(const OperatorType root_operator_type, const uint64_t row_count);
};


}  // namespace opossum
