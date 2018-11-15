#include "sqlite_wrapper.hpp"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/string_utils.hpp"

namespace opossum {

SQLiteWrapper::SQLiteWrapper() {
  int rc = sqlite3_open(":memory:", &_db);

  if (rc != SQLITE_OK) {
    sqlite3_close(_db);
    throw std::runtime_error("Cannot open database: " + std::string(sqlite3_errmsg(_db)) + "\n");
  }
}

SQLiteWrapper::~SQLiteWrapper() { sqlite3_close(_db); }

void SQLiteWrapper::create_table_from_tbl(const std::string& file, const std::string& table_name) {
  std::ifstream infile(file);
  Assert(infile.is_open(), "SQLiteWrapper: Could not find file " + file);

  std::string line;
  std::getline(infile, line);
  std::vector<std::string> column_names = split_string_by_delimiter(line, '|');
  std::getline(infile, line);
  std::vector<std::string> column_types;

  for (const std::string& type : split_string_by_delimiter(line, '|')) {
    std::string actual_type = split_string_by_delimiter(type, '_')[0];
    if (actual_type == "int" || actual_type == "long") {
      column_types.push_back("INT");
    } else if (actual_type == "float" || actual_type == "double") {
      column_types.push_back("REAL");
    } else if (actual_type == "string") {
      column_types.push_back("TEXT");
    } else {
      DebugAssert(false, "SQLiteWrapper: column type " + type + " not supported.");
    }
  }

  std::stringstream query;
  query << "CREATE TABLE " << table_name << "(";
  for (size_t i = 0; i < column_names.size(); i++) {
    query << column_names[i] << " " << column_types[i];

    if ((i + 1) < column_names.size()) {
      query << ", ";
    }
  }
  query << ");";

  size_t rows_added = 0;
  query << "INSERT INTO " << table_name << " VALUES ";
  while (std::getline(infile, line)) {
    if (rows_added) query << ", ";
    query << "(";
    std::vector<std::string> values = split_string_by_delimiter(line, '|');
    for (size_t i = 0; i < values.size(); i++) {
      if (column_types[i] == "TEXT" && values[i] != "null") {
        query << "'" << values[i] << "'";
      } else {
        query << values[i];
      }

      if ((i + 1) < values.size()) {
        query << ", ";
      }
    }
    query << ")";
    ++rows_added;
  }
  query << ";";

  _exec_sql(query.str());
}

void SQLiteWrapper::create_table(const Table& table, const std::string& table_name) {
  std::vector<std::string> column_types;

  for (const auto& column_definition : table.column_definitions()) {
    switch (column_definition.data_type) {
      case DataType::Int:
      case DataType::Long:
        column_types.push_back("INT");
        break;
      case DataType::Float:
      case DataType::Double:
        column_types.push_back("REAL");
        break;
      case DataType::String:
        column_types.push_back("TEXT");
        break;
      case DataType::Null:
      case DataType::Bool:
        Fail("SQLiteWrapper: column type not supported.");
        break;
    }
  }

  std::stringstream create_table_query;
  create_table_query << "CREATE TABLE " << table_name << "(";
  for (auto column_id = ColumnID{0}; column_id < table.column_definitions().size(); column_id++) {
    create_table_query << table.column_definitions()[column_id].name << " " << column_types[column_id];

    if (column_id + 1u < table.column_definitions().size()) {
      create_table_query << ", ";
    }
  }
  create_table_query << ");";
  _exec_sql(create_table_query.str());

  for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
      std::stringstream insert_query;

      // stringstream has than annoying property of truncating floats by default
      insert_query << std::setprecision(std::numeric_limits<long double>::digits10 + 1) << std::endl;

      insert_query << "INSERT INTO " << table_name << " VALUES (";
      for (auto column_id = ColumnID{0}; column_id < table.column_count(); column_id++) {
        const auto segment = chunk->get_segment(column_id);
        const auto value = (*segment)[chunk_offset];

        if (column_types[column_id] == "TEXT" && !variant_is_null(value)) {
          insert_query << "'" << value << "'";
        } else {
          insert_query << value;
        }

        if ((column_id + 1u) < table.column_count()) {
          insert_query << ", ";
        }
      }
      insert_query << ");";
      _exec_sql(insert_query.str());
    }
  }
}

std::shared_ptr<Table> SQLiteWrapper::execute_query(const std::string& sql_query) {
  sqlite3_stmt* result_row;

  auto sql_pipeline = SQLPipelineBuilder{sql_query}.create_pipeline();
  const auto& queries = sql_pipeline.get_sql_strings();

  // We need to split the queries such that we only create columns/add rows from the final SELECT query
  std::vector<std::string> queries_before_select(queries.begin(), queries.end() - 1);
  std::string select_query = queries.back();

  int rc;
  for (const auto& query : queries_before_select) {
    rc = sqlite3_prepare_v2(_db, query.c_str(), -1, &result_row, 0);

    if (rc != SQLITE_OK) {
      sqlite3_finalize(result_row);
      sqlite3_close(_db);
      throw std::runtime_error("Failed to execute query \"" + query + "\": " + std::string(sqlite3_errmsg(_db)) + "\n");
    }

    while ((rc = sqlite3_step(result_row)) != SQLITE_DONE) {
    }
  }

  rc = sqlite3_prepare_v2(_db, select_query.c_str(), -1, &result_row, 0);

  if (rc != SQLITE_OK) {
    auto error_message = "Failed to execute query \"" + select_query + "\": " + std::string(sqlite3_errmsg(_db));
    sqlite3_finalize(result_row);
    sqlite3_close(_db);
    throw std::runtime_error(error_message);
  }

  auto result_table = _create_table(result_row, sqlite3_column_count(result_row));

  sqlite3_reset(result_row);

  while ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    _add_row(result_table, result_row, sqlite3_column_count(result_row));
  }

  sqlite3_finalize(result_row);
  return result_table;
}

std::shared_ptr<Table> SQLiteWrapper::_create_table(sqlite3_stmt* result_row, int column_count) {
  std::vector<bool> column_nullable(column_count, false);
  std::vector<std::string> column_types(column_count, "");
  std::vector<std::string> column_names(column_count, "");

  bool no_result = true;
  int rc;
  while ((rc = sqlite3_step(result_row)) == SQLITE_ROW) {
    for (int i = 0; i < column_count; ++i) {
      if (no_result) {
        column_names[i] = sqlite3_column_name(result_row, i);
      }

      switch (sqlite3_column_type(result_row, i)) {
        case SQLITE_INTEGER: {
          column_types[i] = "int";
          break;
        }

        case SQLITE_FLOAT: {
          column_types[i] = "double";
          break;
        }

        case SQLITE_TEXT: {
          column_types[i] = "string";
          break;
        }

        case SQLITE_NULL: {
          column_nullable[i] = true;
          break;
        }

        case SQLITE_BLOB:
        default: {
          sqlite3_finalize(result_row);
          sqlite3_close(_db);
          throw std::runtime_error("Column type not supported.");
        }
      }
    }
    no_result = false;
  }

  if (!no_result) {
    TableColumnDefinitions column_definitions;
    for (int i = 0; i < column_count; ++i) {
      if (column_types[i].empty()) {
        // Hyrise does not have explicit NULL columns
        column_types[i] = "int";
      }

      const auto data_type = data_type_to_string.right.at(column_types[i]);
      column_definitions.emplace_back(column_names[i], data_type, column_nullable[i]);
    }

    return std::make_shared<Table>(column_definitions, TableType::Data);
  }

  return nullptr;
}

void SQLiteWrapper::reset_table_from_copy(const std::string& table_name_to_reset,
                                          const std::string& table_name_to_copy_from) const {
  std::stringstream command_ss;
  command_ss << "DROP TABLE IF EXISTS " << table_name_to_reset << ";";
  command_ss << "CREATE TABLE " << table_name_to_reset << " AS SELECT * FROM " << table_name_to_copy_from << ";";

  _exec_sql(command_ss.str());
}

void SQLiteWrapper::_add_row(std::shared_ptr<Table> table, sqlite3_stmt* result_row, int column_count) {
  std::vector<AllTypeVariant> row;

  for (int i = 0; i < column_count; ++i) {
    switch (sqlite3_column_type(result_row, i)) {
      case SQLITE_INTEGER: {
        row.push_back(AllTypeVariant{sqlite3_column_int(result_row, i)});
        break;
      }

      case SQLITE_FLOAT: {
        row.push_back(AllTypeVariant{sqlite3_column_double(result_row, i)});
        break;
      }

      case SQLITE_TEXT: {
        row.push_back(AllTypeVariant{std::string(reinterpret_cast<const char*>(sqlite3_column_text(result_row, i)))});
        break;
      }

      case SQLITE_NULL: {
        row.push_back(NULL_VALUE);
        break;
      }

      case SQLITE_BLOB:
      default: {
        sqlite3_finalize(result_row);
        sqlite3_close(_db);
        throw std::runtime_error("Column type not supported.");
      }
    }
  }

  table->append(row);
}

void SQLiteWrapper::_exec_sql(const std::string& sql) const {
  char* err_msg;
  auto rc = sqlite3_exec(_db, sql.c_str(), 0, 0, &err_msg);

  if (rc != SQLITE_OK) {
    auto msg = std::string(err_msg);
    sqlite3_free(err_msg);
    sqlite3_close(_db);
    Fail("Failed to create table. SQL error: " + msg + "\n");
  }
}

}  // namespace opossum
