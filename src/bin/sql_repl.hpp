
#include <string>
#include <iostream>
#include <set>

#include "sql/sql_query_translator.hpp"

namespace opossum {

class SqlRepl
{
 public:
  explicit SqlRepl(const std::string & prompt = "> ", std::istream & in = std::cin, std::ostream & out = std::cout);

  void repl();

 protected:
  std::string _read();
  std::string _eval(const std::string & input);
  void _print(const std::string & result);
  std::string _eval_command(const std::string & sql);
  std::string _eval_sql(const std::string & sql);

  static std::string trim(const std::string & str);

  std::string _prompt;
  std::istream _in;
  std::ostream _out;

  const std::set<std::string> _commands;

  SQLQueryTranslator _translator;
  SQLQueryPlan _plan;
};

}  // namespace opossum
