#include "send_query_response_task.hpp"

#include "SQLParserResult.h"

namespace opossum {

void SendQueryResponseTask::_send_row_description() {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::RowDescription);

  if (!_result_table) {
    // There is no result table, e.g. after an INSERT command
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

  _session->async_send_packet(output_packet);
}

void SendQueryResponseTask::_send_row_data() {
  if (!_result_table) {
    // There is no result table, e.g. after an INSERT command so the client doesn't expect any data
    return;
  }
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

    for (ChunkOffset chunk_offset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
      // New packet for each row
      auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::DataRow);

      // Number of columns in row
      PostgresWireHandler::write_value(output_packet, htons(num_columns));

      for (ColumnID column_id{0}; column_id < num_columns; ++column_id) {
        const auto& column = chunk->get_column(column_id);

        const auto value_string = type_cast<std::string>((*column)[chunk_offset]);

        // Size of string representation of value, NOT of value type's size
        PostgresWireHandler::write_value(output_packet, htonl(value_string.length()));

        // Text mode means that all values are sent as non-terminated strings
        PostgresWireHandler::write_string(output_packet, value_string, false);
      }

      ++_row_count;
      _session->async_send_packet(output_packet);
    }
  }
}

void SendQueryResponseTask::_send_command_complete() {
  if (!_result_table) {
    return;
  }

  std::string completed_msg;
  const auto* statement = _sql_pipeline.get_parsed_sql_statements().front()->getStatements().front();
  switch (statement->type()) {
    case hsql::StatementType::kStmtSelect: {
      completed_msg = "SELECT " + std::to_string(_row_count);
      break;
    }
    case hsql::StatementType::kStmtInsert: {
      // 0 is ignored OID and 1 inserted row
      completed_msg = "INSERT 0 1";
      break;
    }
    case hsql::StatementType::kStmtUpdate: {
      // We do not return how many rows are affected
      completed_msg = "UPDATE 0";
      break;
    }
    case hsql::StatementType::kStmtDelete: {
      // We do not return how many rows are affected
      completed_msg = "DELETE 0";
      break;
    }
    case hsql::StatementType::kStmtCreate: {
      // 0 rows retrieved (Postgres requires a CREATE TABLE statement to return SELECT)
      completed_msg = "SELECT 0";
      break;
    }
    default: { return _session->pipeline_error("Unknown statement type. Server doesn't know how to complete query."); }
  }

  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::CommandComplete);
  PostgresWireHandler::write_string(output_packet, completed_msg);
  _session->async_send_packet(output_packet);
}

void SendQueryResponseTask::_send_execution_info() {
  _session->pipeline_info(
      "Compilation time (µs): " + std::to_string(_sql_pipeline.compile_time_microseconds().count()) +
      "\nExecution time (µs): " + std::to_string(_sql_pipeline.execution_time_microseconds().count()));
}

void SendQueryResponseTask::_on_execute() {
  _send_row_description();
  _send_row_data();
  _send_command_complete();
  _send_execution_info();
  _session->query_response_sent();
}

}  // namespace opossum
