#include "sql_repl.hpp"

#include <iostream>

#include "SQLParser.h"

namespace opossum {

SqlRepl::SqlRepl(const std::string & prompt, std::istream & in, std::ostream & out)
    : _prompt(prompt)
    , _in(in.rdbuf())
    , _out(out.rdbuf()) {}

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
  hsql::SQLParserResult parse_result;

  hsql::SQLParser::parseSQLString(input, &parse_result);
  bool isValid = parse_result.isValid();

  return isValid ? "Valid" : "Not Valid";
}

void SqlRepl::_print(const std::string & result) {
  _out << result << "\n";
}

}  // namespace opossum

int main(int argc, char** argv) {
  opossum::SqlRepl sql;
  sql.repl();
}

//
