#pragma once

#include "sql/SQLStatement.h"

#include "server/client_connection.hpp"
#include "storage/table.hpp"

namespace opossum {

class SQLPipeline;

class QueryResponseBuilder {
 public:
  static std::vector<ColumnDescription> build_row_description(const std::shared_ptr<const Table>& table);
  static std::string build_command_complete_message(hsql::StatementType statement_type, uint64_t row_count);
  static std::string build_execution_info_message(const std::shared_ptr<SQLPipeline>& sql_pipeline);

  using send_row_t = std::function<boost::future<void>(const std::vector<std::string>&)>;

  static boost::future<uint64_t> send_query_response(const send_row_t& send_row, const Table& table);

 protected:
  static boost::future<void> _send_query_response_chunks(const send_row_t& send_row, const Table& table,
                                                         ChunkID current_chunk_id);
  static boost::future<void> _send_query_response_rows(const send_row_t& send_row, const Chunk& chunk,
                                                       ChunkOffset current_chunk_offset);
};

}  // namespace opossum
