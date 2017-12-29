#include "simple_query_command.hpp"

#include <server/hyrise_session.hpp>
#include <sql/sql_pipeline.hpp>
#include <utils/assert.hpp>

namespace opossum {

void SimpleQueryCommand::start(std::size_t size) {
  // Read the SQL query rom the connection
  _session.async_receive_packet(size);
}

void SimpleQueryCommand::handle_packet_received(const InputPacket& input_packet, std::size_t size) {
  switch (_state) {
    case SimpleQueryCommandState::Started: {
      auto sql = PostgresWireHandler::handle_query_packet(input_packet, size);
      start_query(sql);
      break;
    }
    default:
      DebugAssert(false, "Received a packet at an unexpected stage in the command");
      break;
  }
}

void SimpleQueryCommand::handle_event_received() {
  switch (_state) {
    case SimpleQueryCommandState::Executing: {
      send_row_description();
      break;
    }
    default:
      DebugAssert(false, "Received an event at an unexpected stage in the command");
      break;
  }
}

void SimpleQueryCommand::start_query(const std::string& sql) {
  _state = SimpleQueryCommandState::Executing;

  auto sql_pipeline = std::make_shared<SQLPipeline>(sql);
  sql_pipeline->execute_async([=]() {
    auto table = sql_pipeline->get_result_table();

    auto row_count = table ? table->row_count() : 0;
    std::cout << row_count << std::endl;

    _result_table = table;

    // Dispatch an event onto the main thread
    _session.signal_async_event();
  });
}

void SimpleQueryCommand::send_row_description() {
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::RowDescription);

  auto total_bytes = sizeof(uint32_t) + 6 + 3 * sizeof(uint32_t) + 4 * sizeof(uint16_t);
  PostgresWireHandler::write_value(output_packet, htonl(total_bytes));

  // Int16 Specifies the number of fields in a row (can be zero).
  PostgresWireHandler::write_value(output_packet, htons(1u));

  /* FROM: https://www.postgresql.org/docs/current/static/protocol-message-formats.html
   *
   * Then, for each field, there is the following:
   String
   The field name.

   Int32 - If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.

   Int16 - If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.

   Int32 - The object ID of the field's data type.
           Found at: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h

   Int16 - The data type size (see pg_type.typlen). Note that negative values denote variable-width types.

   Int32 - The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.

   Int16 - The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
   */
  PostgresWireHandler::write_string(output_packet, "foo_a");   // 6 bytes (null terminated)
  PostgresWireHandler::write_value(output_packet, htonl(0u));  // no object id
  PostgresWireHandler::write_value(output_packet, htons(0u));  // no attribute number

  PostgresWireHandler::write_value(output_packet, htonl(23));  // object id of int32

  PostgresWireHandler::write_value(output_packet, htons(sizeof(uint32_t)));  // regular int
  PostgresWireHandler::write_value(output_packet, htonl(-1));                // no modifier
  PostgresWireHandler::write_value(output_packet, htons(0u));                // text format

  _session.async_send_packet(output_packet);
  _state = SimpleQueryCommandState::RowDescriptionSent;
}

void SimpleQueryCommand::send_row_data() {
  /*
    DataRow (B)
    Byte1('D')
    Identifies the message as a data row.

    Int32
    Length of message contents in bytes, including self.

    Int16
    The number of column values that follow (possibly zero).

    Next, the following pair of fields appear for each column:

    Int32
    The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.

    Byten
    The value of the column, in the format indicated by the associated format code. n is the above length.
    */

  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::DataRow);

  auto total_bytes = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint32_t) + 3;
  PostgresWireHandler::write_value(output_packet, htonl(total_bytes));

  // 1 column
  PostgresWireHandler::write_value(output_packet, htons(1u));

  // Size of string representation of value, NOT of value type's size
  PostgresWireHandler::write_value(output_packet, htonl(3u));

  // Text mode means that all values are sent as non-terminated strings
  PostgresWireHandler::write_string(output_packet, "100", false);

  _session.async_send_packet(output_packet);
  _state = SimpleQueryCommandState::RowDataSent;
}

void SimpleQueryCommand::send_command_complete() {
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::CommandComplete);

  auto total_bytes = sizeof(uint32_t) + 9;
  PostgresWireHandler::write_value(output_packet, htonl(total_bytes));

  // Completed SELECT statement with 1 result row
  PostgresWireHandler::write_string(output_packet, "SELECT 1");

  _session.async_send_packet(output_packet);
  _state = SimpleQueryCommandState::CommandCompleteSent;
}

void SimpleQueryCommand::handle_packet_sent() {
  switch (_state) {
    case SimpleQueryCommandState::RowDescriptionSent:
      send_row_data();
      break;
    case SimpleQueryCommandState::RowDataSent:
      send_command_complete();
      break;
    case SimpleQueryCommandState::CommandCompleteSent:
      _session.terminate_command();
      break;
    default:
      DebugAssert(false, "Sent a packet at an unknown stage in the command");
      break;
  }
}

}  // namespace opossum
