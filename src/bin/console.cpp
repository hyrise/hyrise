#include "console.hpp"

#include <readline/history.h>
#include <readline/readline.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "operators/print.hpp"
#include "sql/sql_query_translator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "utils/helper.hpp"

namespace {

opossum::Console* _instance = nullptr;
}

namespace opossum {

// Console implementation

Console::Console(const std::string& prompt, const std::string& log_file)
    : _prompt(prompt),
      _multiline_input(""),
      _commands(),
      _commands_completion(),
      _out(std::cout.rdbuf()),
      _log(log_file, std::ios_base::app | std::ios_base::out) {
  // Init readline basics, tells readline to use our custom command completion function
  rl_attempted_completion_function = &Console::command_completion;
  rl_completer_word_break_characters = const_cast<char*>("\t\n\"\\'`@$><=;|&{(");

  // Register default commands to Console
  register_command("exit", exit);
  register_command("quit", exit);
  register_command("help", help);
  register_command("load", load_tpcc);

  // Register more commands specifically for command completion purposes, e.g.
  // for TPCC generation, 'load CUSTOMER', 'load DISTRICT', etc
  auto tpcc_generators = tpcc::TpccTableGenerator::tpcc_table_generator_functions();
  for (tpcc::TpccTableGeneratorFunctions::iterator it = tpcc_generators.begin(); it != tpcc_generators.end(); ++it) {
    _commands_completion.push_back("load " + it->first);
  }

  _instance = this;

  // Timestamp dump only to logfile
  out("--- Session start --- " + time_stamp() + "\n", false);
}

Console::~Console() {
  // Timestamp dump only to logfile
  out("--- Session end --- " + time_stamp() + "\n", false);
  _instance = nullptr;
}

int Console::read() {
  char* buffer;

  // Prompt user for input
  buffer = readline(_prompt.c_str());
  std::string input = trim(std::string(buffer));

  // Only save non-empty commands to history
  if (!input.empty()) {
    add_history(buffer);
  }

  // Free buffer, since readline() allocates new string every time
  free(buffer);

  return _eval(input);
}

int Console::_eval(const std::string& input) {
  if (input.empty() && _multiline_input.empty()) {
    return ReturnCode::Ok;
  }

  // Dump command to logfile (the Console already has it from the input)
  out(_prompt + input + "\n", false);

  // Check if a registered command was entered
  RegisteredCommands::iterator it;
  if ((it = _commands.find(input.substr(0, input.find_first_of(" \n")))) != std::end(_commands)) {
    return _eval_command(it->second, input);
  }

  // Check for multiline-input
  if (input.back() == '\\') {
    _multiline_input += input.substr(0, input.size() - 1);
    if (_multiline_input.back() != ' ') {
      _multiline_input += ' ';
    }
    return ReturnCode::Multiline;
  }

  // Check for the last command of a multiline-input
  if (!_multiline_input.empty()) {
    int retCode = _eval_sql(_multiline_input + input);
    _multiline_input = "";
    return retCode;
  }

  // If nothing from the above, regard input as SQL query
  return _eval_sql(input);
}

int Console::_eval_command(const CommandFunction& func, const std::string& command) {
  size_t first = command.find(' ');
  size_t last = command.find('\n');

  if (std::string::npos == first) {
    return static_cast<int>(func(""));
  }

  std::string args = command.substr(first + 1, last - (first + 1));
  return static_cast<int>(func(args));
}

int Console::_eval_sql(const std::string& sql) {
  SQLQueryTranslator translator;
  SQLQueryPlan plan;

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parse(sql, &parse_result);

  // Check if SQL query is valid
  if (!parse_result.isValid()) {
    out("Error: SQL query not valid.\n");
    return 1;
  }

  // Compile the parse result
  if (!translator.translate_parse_result(parse_result)) {
    out("Error while compiling: " + translator.get_error_msg() + "\n");
    return 1;
  }

  plan = translator.get_query_plan();

  // Execute query plan
  try {
    for (const auto& task : plan.tasks()) {
      task->get_operator()->execute(); 
    }
  } catch (const std::exception& exception) {
    out("Exception thrown while executing query plan:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  // Print result (to Console and logfile)
  out(plan.tree_roots().back()->get_output());

  return ReturnCode::Ok;
}

void Console::register_command(const std::string& name, const CommandFunction& f) {
  _commands[name] = f;
  _commands_completion.push_back(name + " ");
}

Console::RegisteredCommands Console::commands() { return _commands; }

void Console::setPrompt(const std::string& prompt) { _prompt = prompt; }

std::string Console::prompt() const { return _prompt; }

void Console::out(const std::string& output, bool console_print) {
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

int Console::exit(const std::string&) { return Console::ReturnCode::Quit; }

int Console::help(const std::string&) {
  _instance->out("HYRISE SQL Interface\n\n");
  _instance->out("Available commands:\n");
  _instance->out("  load [TABLENAME] - Load available TPC-C tables, or a specific table if TABLENAME is specified\n");
  _instance->out("  exit             - Exit the HYRISE Console\n");
  _instance->out("  quit             - Exit the HYRISE Console\n");
  _instance->out("  help             - Show this message\n\n");
  _instance->out("After TPC-C tables are loaded, SQL queries can be executed.\n");
  _instance->out("Example:\n");
  _instance->out("SELECT * FROM DISTRICT\n");
  return Console::ReturnCode::Ok;
}

// HYRISE SQL Interface
// Enter load to load the TPC-C tables. Then, you can enter SQL queries. Type help for more information.
int Console::load_tpcc(const std::string& tablename) {
  if (tablename.empty() || "ALL" == tablename) {
    _instance->out("Generating TPCC tables (this might take a while) ...\n");
    auto tables = tpcc::TpccTableGenerator().generate_all_tables();
    for (auto& pair : tables) {
      StorageManager::get().add_table(pair.first, pair.second);
    }
    return Console::ReturnCode::Ok;
  }

  _instance->out("Generating TPCC table: \"" + tablename + "\" ...\n");
  auto table = tpcc::TpccTableGenerator::generate_tpcc_table(tablename);
  if (table == nullptr) {
    _instance->out("Error: No TPCC table named \"" + tablename + "\" available.\n");
    return Console::ReturnCode::Error;
  }

  opossum::StorageManager::get().add_table(tablename, table);
  return Console::ReturnCode::Ok;
}

// GNU readline interface to our commands

char** Console::command_completion(const char* text, int start, int end) {
  char** completion_matches = nullptr;
  rl_completion_suppress_append = 1;
  rl_attempted_completion_over = 1;
  if (start == 0) {
    completion_matches = rl_completion_matches(text, &Console::command_generator);
  }
  return completion_matches;
}

char* Console::command_generator(const char* text, int state) {
  static std::vector<std::string>::iterator it;
  auto& commands = _instance->_commands_completion;
  if (state == 0) {
    it = commands.begin();
  }

  while (it != commands.end()) {
    auto& command = *it;
    ++it;
    if (command.find(text) != std::string::npos) {
      char* completion = new char[command.size()];
      snprintf(completion, command.size() + 1, "%s", command.c_str());
      return completion;
    }
  }
  return nullptr;
}

}  // namespace opossum

int main(int argc, char** argv) {
  using Return = opossum::Console::ReturnCode;

  opossum::Console console("> ", "./console.log");

  console.out("HYRISE SQL Interface\n");
  console.out("Enter 'load' to load the TPC-C tables. Then, you can enter SQL queries. Type 'help' for more information.\n\n");

  // Main REPL loop
  int retCode;
  while ((retCode = console.read()) != Return::Quit) {
    if (retCode == Return::Ok) {
      console.setPrompt("> ");
    } else if (retCode == Return::Multiline) {
      console.setPrompt("... ");
    } else {
      console.setPrompt("!> ");
    }
  }

  console.out("Bye.\n");
}
