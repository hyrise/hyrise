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
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::RowDescription);

  // Int16 Specifies the number of fields in a row (can be zero).
  if (!_result_table) {
    // There is no result table, e.g. after an INSERT command
    PostgresWireHandler::write_value(output_packet, htons(0u));
    _session.async_send_packet(output_packet);
    _state = SimpleQueryCommandState::RowDescriptionSent;
    return;
  }

  const auto& column_names = _result_table->column_names();

  // Int16 Specifies the number of fields in a row (can be zero).
  PostgresWireHandler::write_value(output_packet, htons(column_names.size()));

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
  const auto& column_types = _result_table->column_types();
  for (auto column_id = 0u; column_id < _result_table->column_count(); ++column_id) {
    PostgresWireHandler::write_string(output_packet, column_names[column_id]);
    PostgresWireHandler::write_value(output_packet, htonl(0u));  // no object id
    PostgresWireHandler::write_value(output_packet, htons(0u));  // no attribute number

    auto object_id = 0u;
    auto type_width = 0;
    switch (column_types[column_id]) {
      case DataType::Int:
        object_id = 23;
        type_width = 4;
        break;
      case DataType::Long:
        object_id = 20;
        type_width = 8;
        break;
      case DataType::Float:
        object_id = 700;
        type_width = 4;
        break;
      case DataType::Double:
        object_id = 701;
        type_width = 8;
        break;
      case DataType::String:
        object_id = 25;
        type_width = -1;
        break;
      default:
        Fail("Bad DataType");
    }

    PostgresWireHandler::write_value(output_packet, htonl(object_id));   // object id of type
    PostgresWireHandler::write_value(output_packet, htons(type_width));  // regular int
    PostgresWireHandler::write_value(output_packet, htonl(-1));          // no modifier
    PostgresWireHandler::write_value(output_packet, htons(0u));          // text format
  }

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
  const auto num_columns = _result_table->column_count();

  for (ChunkID chunk_id{0}; chunk_id < _result_table->chunk_count(); ++chunk_id) {
    const auto& chunk = _result_table->get_chunk(chunk_id);

    for (ChunkOffset chunk_offset{0}; chunk_offset < chunk.size(); ++chunk_offset) {
      // New packet for each row
      auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::DataRow);

      // Number of columns in row
      PostgresWireHandler::write_value(output_packet, htons(num_columns));

      for (ColumnID column_id{0}; column_id < num_columns; ++column_id) {
        std::string value_string_buffer;

        const auto& column = chunk.get_column(column_id);
        column->write_string_representation(value_string_buffer, chunk_offset);

        const auto value_size = value_string_buffer.length() - sizeof(uint32_t);
        // Remove unnecessary size at end of string
        value_string_buffer.resize(value_size);

        // Size of string representation of value, NOT of value type's size
        PostgresWireHandler::write_value(output_packet, htonl(value_size));

        // Text mode means that all values are sent as non-terminated strings
        PostgresWireHandler::write_string(output_packet, value_string_buffer, false);
      }

      _row_count++;
      _session.async_send_packet(output_packet);
    }
  }
  _state = SimpleQueryCommandState::RowDataSent;
}

void SimpleQueryCommand::send_command_complete() {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::CommandComplete);

  const auto completed = "SELECT " + std::to_string(_row_count);
  PostgresWireHandler::write_string(output_packet, completed);

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
