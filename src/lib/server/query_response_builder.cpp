#include "query_response_builder.hpp"

#include "server/postgres_wire_handler.hpp"
#include "sql/sql_pipeline.hpp"

#include "SQLParserResult.h"

#include "then_operator.hpp"

namespace opossum {

using opossum::then_operator::then;

std::vector<ColumnDescription> QueryResponseBuilder::build_row_description(const std::shared_ptr<const Table>& table) {
  std::vector<ColumnDescription> result;

  const auto& column_names = table->column_names();
  const auto& column_types = table->column_data_types();

  for (auto column_id = 0u; column_id < table->column_count(); ++column_id) {
    uint32_t object_id;
    int32_t type_id;

    switch (column_types[column_id]) {
      case DataType::Int:
        object_id = 23;
        type_id = 4;
        break;
      case DataType::Long:
        object_id = 20;
        type_id = 8;
        break;
      case DataType::Float:
        object_id = 700;
        type_id = 4;
        break;
      case DataType::Double:
        object_id = 701;
        type_id = 8;
        break;
      case DataType::String:
        object_id = 25;
        type_id = -1;
        break;
      default:
        Fail("Bad DataType");
    }

    result.emplace_back(ColumnDescription{column_names[column_id], object_id, type_id});
  }

  return result;
}

std::string QueryResponseBuilder::build_command_complete_message(const AbstractOperator& root_op, uint64_t row_count) {
  switch (root_op.type()) {
    case OperatorType::Insert: {
      // 0 is ignored OID and 1 inserted row
      return "INSERT 0 1";
    }
    case OperatorType::Update: {
      // We do not return how many rows are affected, because we don't track this information
      return "UPDATE -1";
    }
    case OperatorType::Delete: {
      // We do not return how many rows are affected, because we don't track this information
      return "DELETE -1";
    }
    default:
      // Assuming normal query
      return "SELECT " + std::to_string(row_count);
  }
}

std::string QueryResponseBuilder::build_execution_info_message(const std::shared_ptr<SQLPipeline>& sql_pipeline) {
  std::stringstream stream;
  stream << sql_pipeline->metrics();
  return stream.str();
}

boost::future<uint64_t> QueryResponseBuilder::send_query_response(const send_row_t& send_row, const Table& table) {
  // Essentially we're iterating over every row in every chunk in the table, generating and sending
  // its string representation. However, because of the asynchronous send_row call, we have to
  // use this two-level recursion instead of two nested for-loops

  return _send_query_response_chunks(send_row, table, ChunkID{0}) >> then >> [&]() { return table.row_count(); };
}

boost::future<void> QueryResponseBuilder::_send_query_response_chunks(const send_row_t& send_row, const Table& table,
                                                                      ChunkID current_chunk_id) {
  if (current_chunk_id == table.chunk_count()) return boost::make_ready_future();

  const auto& chunk = table.get_chunk(current_chunk_id);

  return _send_query_response_rows(send_row, *chunk, ChunkOffset{0}) >> then >>
         std::bind(QueryResponseBuilder::_send_query_response_chunks, send_row, std::ref(table),
                   ChunkID{current_chunk_id + 1});
}

boost::future<void> QueryResponseBuilder::_send_query_response_rows(const send_row_t& send_row, const Chunk& chunk,
                                                                    ChunkOffset current_chunk_offset) {
  if (current_chunk_offset == chunk.size()) return boost::make_ready_future();

  std::vector<std::string> row_strings(chunk.column_count());

  for (ColumnID column_id{0}; column_id < ColumnID{chunk.column_count()}; ++column_id) {
    const auto& segment = chunk.get_segment(column_id);
    row_strings[column_id] = boost::lexical_cast<pmr_string>((*segment)[current_chunk_offset]);
  }

  return send_row(row_strings) >> then >> std::bind(QueryResponseBuilder::_send_query_response_rows, send_row,
                                                    std::ref(chunk), ChunkOffset{current_chunk_offset + 1});
}

}  // namespace opossum
