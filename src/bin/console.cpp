#include "console.hpp"

#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>

#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "sql/sql_query_translator.hpp"
#include "operators/print.hpp"

namespace {

  // Helper functions

  std::string trim(const std::string & str) {
    size_t first = str.find_first_not_of(' ');
    if (std::string::npos == first)
    {
      return str;
    }
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last - first + 1));
  }

  std::shared_ptr<opossum::Table> generate_tpcc_table(const std::string & tablename) {
    if ("ITEM" == tablename) return tpcc::TpccTableGenerator().generate_items_table();
    if ("WAREHOUSE" == tablename) return tpcc::TpccTableGenerator().generate_warehouse_table();
    if ("STOCK" == tablename) return tpcc::TpccTableGenerator().generate_stock_table();
    if ("DISTRICT" == tablename) return tpcc::TpccTableGenerator().generate_district_table();
    if ("CUSTOMER" == tablename) return tpcc::TpccTableGenerator().generate_customer_table();
    if ("HISTORY" == tablename) return tpcc::TpccTableGenerator().generate_history_table();
    if ("NEW-ORDER" == tablename) return tpcc::TpccTableGenerator().generate_new_order_table();
    if ("ORDER" == tablename) {
      auto order_line_counts = tpcc::TpccTableGenerator().generate_order_line_counts();
      return tpcc::TpccTableGenerator().generate_order_table(order_line_counts);
    }
    if ("ORDER-LINE" == tablename) {
      auto order_line_counts = tpcc::TpccTableGenerator().generate_order_line_counts();
      return tpcc::TpccTableGenerator().generate_order_line_table(order_line_counts);
    }
    return nullptr;
  }

}

namespace opossum {

// Command functions declaration

int exit(const std::string &);
int load_tpcc(const std::string &);

// Console implementation

Console::Console(const std::string & prompt)
    : _prompt(prompt)
    , _commands() {
      register_command("exit", exit);
      register_command("load", load_tpcc);
      register_command("loadtpcc", load_tpcc);
    }

int Console::read() {
  char* buffer; // Buffer of line entered by user

  buffer = readline(_prompt.c_str()); // Prompt user for input

  if(strcmp(buffer, "") != 0) { // Only save non-empty commands to history
    add_history(buffer);
  }

  std::string input(buffer);
  free(buffer); // Free buffer, since readline() allocates new string every time
  buffer = NULL;

  return _eval(input);
}

void Console::register_command(const std::string & name, const CommandFunction & f) {
  _commands[name] = f;
}

void Console::setPrompt(const std::string & prompt) {
  _prompt = prompt;
}

std::string Console::prompt() const {
  return _prompt;
}

int Console::_eval(const std::string & input) {
  if (input.empty()) return ReturnCode::Ok;
  std::string input_trimmed = trim(input);

  RegisteredCommands::iterator it;
  if ((it = _commands.find(input_trimmed.substr(0, input_trimmed.find('(')))) != std::end(_commands)) {
    return _eval_command(it->second, input_trimmed);
  }

  return _eval_sql(input_trimmed);
}

int Console::_eval_command(const CommandFunction & f, const std::string & command) {
  size_t first = command.find('(') + 1;
  size_t last = command.find_last_of(')');

  if (std::string::npos == first)
  {
    return static_cast<int>(f(""));
  }

  std::string arg = command.substr(first, last-first);
  return static_cast<int>(f(arg));
}

int Console::_eval_sql(const std::string & sql) {
  SQLQueryTranslator translator;
  SQLQueryPlan plan;

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString(sql, &parse_result);

  if (!parse_result.isValid())
  {
    std::cout << "Error: SQL query not valid." << std::endl;
    return 1;
  }

  // Compile the parse result.
  if (!translator.translate_parse_result(parse_result))
  {
    std::cout << "Error while compiling: " << translator.get_error_msg() << std::endl;
    return 1;
  }

  plan = translator.get_query_plan();

  for (const auto& task : plan.tasks()) {
    task->get_operator()->execute();
  }

  auto result = plan.tree_roots().back()->get_output();
  Print::print(result);

  return ReturnCode::Ok;
}

// Command functions implementation

int exit(const std::string &) {
  return Console::ReturnCode::Quit;
}

int load_tpcc(const std::string & tablename) {
  if (tablename.empty() || "ALL" == tablename)
  {
    std::cout << "Generating TPCC tables ..." << std::endl;
    auto tables = tpcc::TpccTableGenerator().generate_all_tables();
    for (auto& pair : tables) {
      StorageManager::get().add_table(pair.first, pair.second);
    }
    return Console::ReturnCode::Ok;
  }

  std::cout << "Generating TPCC table: " << tablename << " ..." << std::endl;
  auto table = generate_tpcc_table(tablename);
  if (table == nullptr)
  {
    return Console::ReturnCode::Error;
  }

  opossum::StorageManager::get().add_table(tablename, table);
  return Console::ReturnCode::Ok;
}

}  // namespace opossum


int main(int argc, char** argv) {
  using Return = opossum::Console::ReturnCode;

  opossum::Console console("> ");

  int retCode;
  while ((retCode = console.read()) != Return::Quit) {
    if (retCode == Return::Ok)
    {
      console.setPrompt("> ");
    } else {
      console.setPrompt("!> ");
    }
  }

  std::cout << "Bye." << std::endl;
}
