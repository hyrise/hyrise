#include "console.hpp"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <readline/history.h>
#include <readline/readline.h>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "operators/import_csv.hpp"
#include "operators/print.hpp"
#include "sql/sql_query_translator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"

namespace {

// Returns a string containing a timestamp of the current date and time
std::string current_timestamp() {
  auto t = std::time(nullptr);
  auto tm = *std::localtime(&t);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return oss.str();
}
}  // namespace

namespace opossum {

// Console implementation

Console::Console()
    : _prompt("> "),
      _multiline_input(""),
      _commands(),
      _tpcc_commands(),
      _out(std::cout.rdbuf()),
      _log("console.log", std::ios_base::app | std::ios_base::out),
      _verbose(false) {
  // Init readline basics, tells readline to use our custom command completion function
  rl_attempted_completion_function = &Console::command_completion;
  rl_completer_word_break_characters = const_cast<char*>("\t\n\"\\'`@$><=;|&{(");

  // Register default commands to Console
  register_command("exit", exit);
  register_command("quit", exit);
  register_command("help", help);
  register_command("generate", generate_tpcc);
  register_command("load", load_table);
  register_command("script", exec_script);

  // Register more commands specifically for command completion purposes, e.g.
  // for TPCC generation, 'generate CUSTOMER', 'generate DISTRICT', etc
  auto tpcc_generators = tpcc::TpccTableGenerator::tpcc_table_generator_functions();
  for (tpcc::TpccTableGeneratorFunctions::iterator it = tpcc_generators.begin(); it != tpcc_generators.end(); ++it) {
    _tpcc_commands.push_back("generate " + it->first);
  }
}

Console& Console::get() {
  static Console instance;
  return instance;
}

int Console::read() {
  char* buffer;

  // Prompt user for input
  buffer = readline(_prompt.c_str());
  std::string input(buffer);
  boost::algorithm::trim<std::string>(input);

  // Only save non-empty commands to history
  if (!input.empty()) {
    add_history(buffer);
  }

  // Free buffer, since readline() allocates new string every time
  free(buffer);

  return _eval(input);
}

int Console::execute_script(const std::string& filepath) { return exec_script(filepath); }

int Console::_eval(const std::string& input) {
  if (input.empty() && _multiline_input.empty()) {
    return ReturnCode::Ok;
  }

  // Dump command to logfile, and to the Console if input comes from a script file
  out(_prompt + input + "\n", _verbose);

  // Check if a registered command was entered
  RegisteredCommands::iterator it;
  if ((it = _commands.find(input.substr(0, input.find_first_of(" \n(")))) != std::end(_commands)) {
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
  size_t first = command.find('(');
  size_t last = command.find(')');

  if (std::string::npos == first) {
    first = command.find(' ');
    last = command.find('\n');
  }

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

  // Measure the query plan execution time
  auto started = std::chrono::high_resolution_clock::now();

  // Execute query plan
  try {
    for (const auto& task : plan.tasks()) {
      task->get_operator()->execute();
    }
  } catch (const std::exception& exception) {
    out("Exception thrown while executing query plan:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  // Measure the query plan execution time
  auto done = std::chrono::high_resolution_clock::now();
  auto elapsed_ms = std::chrono::duration<double>(done - started).count();

  auto table = plan.tree_roots().back()->get_output();
  auto row_count = table->row_count();

  // Print result (to Console and logfile)
  out(table);
  out("===\n");
  out(std::to_string(row_count) + " rows (" + std::to_string(elapsed_ms) + " ms)\n");

  return ReturnCode::Ok;
}

void Console::register_command(const std::string& name, const CommandFunction& f) { _commands[name] = f; }

Console::RegisteredCommands Console::commands() { return _commands; }

void Console::setPrompt(const std::string& prompt) { _prompt = prompt; }

void Console::setLogfile(const std::string& logfile) {
  _log = std::ofstream(logfile, std::ios_base::app | std::ios_base::out);
}

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
  auto& console = Console::get();
  console.out("HYRISE SQL Interface\n\n");
  console.out("Available commands:\n");
  console.out(
      "  generate [TABLENAME] - Generate available TPC-C tables, or a specific table if TABLENAME is specified\n");
  console.out(
      "  load FILE TABLENAME  - Load table from disc specified by filepath FILE, store it with name TABLENAME\n");
  console.out(
      "  load(FILE TABLENAME) - Load table from disc specified by filepath FILE, store it with name TABLENAME\n");
  console.out("                         (with filepath completion)\n");
  console.out("  script SCRIPTFILE    - Execute script specified by SCRIPTFILE\n");
  console.out("  script(SCRIPTFILE)   - Execute script specified by SCRIPTFILE\n");
  console.out("                         (with filepath completion)\n");
  console.out("  exit                 - Exit the HYRISE Console\n");
  console.out("  quit                 - Exit the HYRISE Console\n");
  console.out("  help                 - Show this message\n\n");
  console.out("After TPC-C tables are generated, SQL queries can be executed.\n");
  console.out("Example:\n");
  console.out("SELECT * FROM DISTRICT\n");
  return Console::ReturnCode::Ok;
}

int Console::generate_tpcc(const std::string& tablename) {
  auto& console = Console::get();
  if (tablename.empty() || "ALL" == tablename) {
    console.out("Generating TPCC tables (this might take a while) ...\n");
    auto tables = tpcc::TpccTableGenerator().generate_all_tables();
    for (auto& pair : tables) {
      StorageManager::get().add_table(pair.first, pair.second);
    }
    return Console::ReturnCode::Ok;
  }

  console.out("Generating TPCC table: \"" + tablename + "\" ...\n");
  auto table = tpcc::TpccTableGenerator::generate_tpcc_table(tablename);
  if (table == nullptr) {
    console.out("Error: No TPCC table named \"" + tablename + "\" available.\n");
    return Console::ReturnCode::Error;
  }

  opossum::StorageManager::get().add_table(tablename, table);
  return Console::ReturnCode::Ok;
}

int Console::load_table(const std::string& args) {
  auto& console = Console::get();
  std::string input = args;
  boost::algorithm::trim<std::string>(input);
  std::vector<std::string> arguments;
  boost::algorithm::split(arguments, input, boost::is_space());

  if (arguments.size() != 2) {
    console.out("Usage:\n");
    console.out("  load FILEPATH TABLENAME\n");
    console.out("  load(FILEPATH TABLENAME)\n");
    return ReturnCode::Error;
  }

  auto importer = std::make_shared<ImportCsv>(arguments.at(0), arguments.at(1));
  try {
    importer->execute();
  } catch (const std::exception& exception) {
    console.out("Exception thrown while importing CSV:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  return ReturnCode::Ok;
}

int Console::exec_script(const std::string& script_file) {
  auto& console = Console::get();
  auto filepath = script_file;
  boost::algorithm::trim(filepath);
  std::ifstream script(filepath);

  if (!script.good()) {
    console.out("Error: Script file '" + filepath + "' does not exist.\n");
    return ReturnCode::Error;
  }

  console.out("Executing script file: " + filepath + "\n");
  console._verbose = true;
  std::string command;
  int retCode = ReturnCode::Ok;
  while (std::getline(script, command)) {
    retCode = console._eval(command);
    if (retCode == ReturnCode::Error || retCode == ReturnCode::Quit) {
      break;
    }
  }
  console.out("Executing script file done\n");
  console._verbose = false;
  return retCode;
}

// GNU readline interface to our commands

char** Console::command_completion(const char* text, int start, int end) {
  char** completion_matches = nullptr;

  std::vector<std::string> tokens;
  std::string input(text);
  boost::algorithm::split(tokens, input, boost::is_space());

  if (!tokens.empty()) {
    if (tokens.at(0) == "generate") {
      completion_matches = rl_completion_matches(text, &Console::command_generator_tpcc);
    } else if (start == 0) {
      completion_matches = rl_completion_matches(text, &Console::command_generator);
    }
  }

  return completion_matches;
}

char* Console::command_generator(const char* text, int state) {
  static RegisteredCommands::iterator it;
  auto& commands = Console::get()._commands;

  if (state == 0) {
    it = commands.begin();
  }

  while (it != commands.end()) {
    auto& command = it->first;
    ++it;
    if (command.find(text) != std::string::npos) {
      char* completion = new char[command.size()];
      snprintf(completion, command.size() + 1, "%s", command.c_str());
      return completion;
    }
  }
  return nullptr;
}

char* Console::command_generator_tpcc(const char* text, int state) {
  static std::vector<std::string>::iterator it;
  auto& commands = Console::get()._tpcc_commands;
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
  auto& console = opossum::Console::get();

  console.setPrompt("> ");
  console.setLogfile("console.log");

  // Timestamp dump only to logfile
  console.out("--- Session start --- " + current_timestamp() + "\n", false);

  int retCode = Return::Ok;

  // Display Usage if too many arguments are provided
  if (argc > 2) {
    retCode = Return::Quit;
    console.out("Usage:\n");
    console.out("  ./opossumConsole [SCRIPTFILE] - Start the interactive SQL interface.\n");
    console.out("                                  Execute script if specified by SCRIPTFILE.\n");
  }

  // Execute .sql script if specified
  if (argc == 2) {
    retCode = console.execute_script(std::string(argv[1]));
    // Terminate Console if an error occured during script execution
    if (retCode == Return::Error) {
      retCode = Return::Quit;
    }
  }

  // Display welcome message if Console started normally
  if (argc == 1) {
    console.out("HYRISE SQL Interface\n");
    console.out("Enter 'generate' to generate the TPC-C tables. Then, you can enter SQL queries.\n");
    console.out("Type 'help' for more information.\n\n");
  }

  // Main REPL loop
  while (retCode != Return::Quit) {
    retCode = console.read();
    if (retCode == Return::Ok) {
      console.setPrompt("> ");
    } else if (retCode == Return::Multiline) {
      console.setPrompt("... ");
    } else {
      console.setPrompt("!> ");
    }
  }

  console.out("Bye.\n");

  // Timestamp dump only to logfile
  console.out("--- Session end --- " + current_timestamp() + "\n", false);
}
