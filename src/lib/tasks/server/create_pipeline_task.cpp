#include "create_pipeline_task.hpp"

#include <boost/algorithm/string.hpp>

#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

void CreatePipelineTask::_on_execute() {
  auto result = std::make_unique<CreatePipelineResult>();

  try {
    result->sql_pipeline = std::make_shared<SQLPipeline>(SQLPipelineBuilder{_sql}.create_pipeline());
  } catch (const std::exception& exception) {
    // Try LOAD file_name table_name
    if (_allow_load_table && _is_load_table()) {
      result->load_table = std::make_pair(_file_name, _table_name);
    } else {
      // Setting the exception this way ensures that the details are preserved in the futures
      // Important: std::current_exception apparently does not work
      // (exceptions are not correctly being re-thrown when 'get'ing the future)
      return _promise.set_exception(boost::current_exception());
    }
  }

  _promise.set_value(std::move(result));
}

bool CreatePipelineTask::_is_load_table() {
  std::vector<std::string> words;
  boost::split(words, _sql, boost::is_any_of(" "));

  // We expect exactly LOAD file_name table_name
  if (words.size() != 3) return false;
  if (words[0] != "LOAD" && words[0] != "load") return false;

  _file_name = std::move(words[1]);
  _table_name = std::move(words[2]);

  // Last character is always a \0-byte
  _table_name.resize(_table_name.length() - 1);

  // Remove semicolon if it is the last character
  if (_table_name.back() == ';') _table_name.resize(_table_name.length() - 1);

  return true;
}

}  // namespace opossum
