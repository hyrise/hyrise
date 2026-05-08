#pragma once

#include <memory>
#include <string>

#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "storage/table.hpp"

namespace hyrise {

struct ExecutionInformation;

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
  static std::string build_command_complete_message(const ExecutionInformation& execution_information,
                                                    const uint64_t row_count);
  static std::string build_command_complete_message(const OperatorType root_operator_type, const uint64_t row_count);
};

extern template void ResultSerializer::send_table_description<Socket>(
    const std::shared_ptr<const Table>&, const std::shared_ptr<PostgresProtocolHandler<Socket>>&);

extern template void ResultSerializer::send_table_description<boost::asio::posix::stream_descriptor>(
    const std::shared_ptr<const Table>&,
    const std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>&);

extern template void ResultSerializer::send_query_response<Socket>(
    const std::shared_ptr<const Table>&, const std::shared_ptr<PostgresProtocolHandler<Socket>>&);

extern template void ResultSerializer::send_query_response<boost::asio::posix::stream_descriptor>(
    const std::shared_ptr<const Table>&,
    const std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>&);

}  // namespace hyrise
