#pragma once

#include <memory>
#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "storage/table.hpp"

namespace opossum {

// The ResultSerializer serializes the result data returned by Hyrise according to PostgreSQL Wire Protocol.
class ResultSerializer {
 public:
  // Serialize information about the result table
  template <typename SocketType>
  static void send_table_description(
      const std::shared_ptr<const Table>& table,
      const std::shared_ptr<PostgresProtocolHandler<SocketType>>& postgres_protocol_handler);

  template <typename SocketType>
  // Cast attributes of the result table and send them row-wise
  static void send_query_response(
      const std::shared_ptr<const Table>& table,
      const std::shared_ptr<PostgresProtocolHandler<SocketType>>& postgres_protocol_handler);

  // Build completion message after query execution containing the statement type and the number of rows affected
  static std::string build_command_complete_message(const OperatorType root_operator_type, const uint64_t row_count);
};

}  // namespace opossum
