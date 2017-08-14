#include "console.hpp"

#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>
#include <ctime>
#include <iomanip>

#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "sql/sql_query_translator.hpp"
#include "operators/print.hpp"

namespace {

  //

  opossum::Console * _instance = nullptr;

  // Helper functions

  std::string time_stamp() {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
  }

  std::string trim(const std::string & str) {
    size_t first = str.find_first_not_of(' ');
    if (std::string::npos == first)
    {
      return "";
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

// Console implementation

Console::Console(const std::string & prompt, const std::string & log_file)
  : _prompt(prompt)
  , _commands()
  , _out(std::cout.rdbuf())
  , _log(log_file, std::ios_base::app | std::ios_base::out) {

  // Init readline basics
  rl_attempted_completion_function = &Console::command_completion;

  register_command("exit", exit);
  register_command("load", load_tpcc);
  register_command("loadtpcc", load_tpcc);

  _instance = this;

  out("--- Session start --- " + time_stamp() + "\n", false);
}

Console::~Console() {
  out("--- Session end --- " + time_stamp() + "\n", false);
  _instance = nullptr;
}

int Console::read() {
  char* buffer; // Buffer of line entered by user

  buffer = readline(_prompt.c_str()); // Prompt user for input
  std::string input = trim(std::string(buffer));

  if(!input.empty()) { // Only save non-empty commands to history
    add_history(buffer);
  }

  free(buffer); // Free buffer, since readline() allocates new string every time

  return _eval(input);
}

void Console::register_command(const std::string & name, const CommandFunction & f) {
  _commands[name] = f;
}

Console::RegisteredCommands Console::commands() {
  return _commands;
}

void Console::setPrompt(const std::string & prompt) {
  _prompt = prompt;
}

std::string Console::prompt() const {
  return _prompt;
}

int Console::_eval(const std::string & input) {
  if (input.empty()) return ReturnCode::Ok;

  out(_prompt + input + "\n", false);

  RegisteredCommands::iterator it;
  if ((it = _commands.find(input.substr(0, input.find('(')))) != std::end(_commands)) {
    return _eval_command(it->second, input);
  }

  return _eval_sql(input);
}

int Console::_eval_command(const CommandFunction & f, const std::string & command) {
  size_t first = command.find('(');
  size_t last = command.find_last_of(')');

  if (std::string::npos == first)
  {
    return static_cast<int>(f(""));
  }

  std::string args = command.substr(first+1, last-(first+1));
  return static_cast<int>(f(args));
}

int Console::_eval_sql(const std::string & sql) {
  SQLQueryTranslator translator;
  SQLQueryPlan plan;

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString(sql, &parse_result);

  if (!parse_result.isValid())
  {
    out("Error: SQL query not valid.\n");
    return 1;
  }

  // Compile the parse result.
  if (!translator.translate_parse_result(parse_result))
  {
    out("Error while compiling: " + translator.get_error_msg() + "\n");
    return 1;
  }

  plan = translator.get_query_plan();

  for (const auto& task : plan.tasks()) {
    task->get_operator()->execute();
  }

  out(plan.tree_roots().back()->get_output());

  return ReturnCode::Ok;
}

void Console::out(const std::string & output, bool console_print) {
  if (console_print) {
    _out << output;
  }
  _log << output;
  _log.flush();
}

void Console::out(std::shared_ptr<const Table> table) {
  Print::print(table, 0, _out);
  Print::print(table, 0, _log);
}

// Command functions

int Console::exit(const std::string &) {
  return Console::ReturnCode::Quit;
}

int Console::load_tpcc(const std::string & tablename) {
  if (tablename.empty() || "ALL" == tablename)
  {
    _instance->out("Generating TPCC tables (this might take a while) ...\n");
    auto tables = tpcc::TpccTableGenerator().generate_all_tables();
    for (auto& pair : tables) {
      StorageManager::get().add_table(pair.first, pair.second);
    }
    return Console::ReturnCode::Ok;
  }

  _instance->out("Generating TPCC table: \"" + tablename + "\" ...\n");
  auto table = generate_tpcc_table(tablename);
  if (table == nullptr)
  {
    _instance->out("Error: No TPCC table named \"" + tablename + "\" available.\n");
    return Console::ReturnCode::Error;
  }

  opossum::StorageManager::get().add_table(tablename, table);
  return Console::ReturnCode::Ok;
}

// GNU readline interface to our commands

char ** Console::command_completion(const char * text, int start, int end) {
  char ** completion_matches = nullptr;
  if (start == 0)
  {
    completion_matches = rl_completion_matches(text, &Console::command_generator);
  }
  return completion_matches;
}

char * Console::command_generator(const char * text, int state) {
  static Console::RegisteredCommands::iterator it;
  auto& commands = _instance->_commands;
  if (state == 0) {
    it = commands.begin();
  }

  while ( it != commands.end() ) {
    auto & command = it->first;
    ++it;
    if ( command.find(text) != std::string::npos ) {
      char * completion = new char[command.size()];
      strcpy(completion, command.c_str());
      return completion;
    }
  }
  return nullptr;
}

}  // namespace opossum


int main(int argc, char** argv) {
  using Return = opossum::Console::ReturnCode;

  opossum::Console console("> ", "./console.log");

  int retCode;
  while ((retCode = console.read()) != Return::Quit) {
    if (retCode == Return::Ok)
    {
      console.setPrompt("> ");
    } else {
      console.setPrompt("!> ");
    }
  }

  console.out("Bye.\n");
}
