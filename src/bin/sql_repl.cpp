#include "sql_repl.hpp"

#include <iostream>

#include "SQLParser.h"
// #include "operators/import_csv.hpp"

namespace opossum {

SqlRepl::SqlRepl(const std::string & prompt, std::istream & in, std::ostream & out)
    : _prompt(prompt)
    , _in(in.rdbuf())
    , _out(out.rdbuf())
    , _commands({"loadtable", "exit"}) {}

void SqlRepl::repl() {
  for (;;)
  {
    _out << _prompt;
    _print(_eval(_read()));
  }
}

std::string SqlRepl::_read() {
  std::string line;
  std::getline(_in, line);
  return line;
}

std::string SqlRepl::_eval(const std::string & input) {
  std::string input_no_spaces = SqlRepl::trim(input);
  if (_commands.find(input_no_spaces.substr(0, input_no_spaces.find('('))) != _commands.end())
  {
    return _eval_command(input_no_spaces);
  }
  return _eval_sql(input);
}

void SqlRepl::_print(const std::string & result) {
  _out << result << "\n";
}

std::string SqlRepl::_eval_command(const std::string & cmd) {
  return cmd;
}

std::string SqlRepl::_eval_sql(const std::string & sql) {
  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString(sql, &parse_result);

  if (!parse_result.isValid())
  {
    return "Error: SQL query not valid.";
  }

  // Compile the parse result.
  if (!_translator.translate_parse_result(parse_result))
  {
    return "Error while compiling: " + _translator.get_error_msg();
  }

  _plan = _translator.get_query_plan();

  for (const auto& task : _plan.tasks()) {
    task->get_operator()->execute();
  }

  auto result = _plan.tree_roots().back()->get_output();

  return parse_result.isValid() ? "Valid" : "Not valid";
}

std::string SqlRepl::trim(const std::string & str) {
  size_t first = str.find_first_not_of(' ');
  if (std::string::npos == first)
  {
    return str;
  }
  size_t last = str.find_last_not_of(' ');
  return str.substr(first, (last - first + 1));
}

}  // namespace opossum

int main(int argc, char** argv) {
  opossum::SqlRepl sql;
  sql.repl();
}
