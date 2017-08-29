#include "console.hpp"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <csignal>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "operators/import_csv.hpp"
#include "operators/print.hpp"
#include "sql/sql_planner.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "utils/load_table.hpp"

namespace {

// Buffer for program state
sigjmp_buf jmp_env;

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
  rl_completer_word_break_characters = const_cast<char*>(" \t\n\"\\'`@$><=;|&{(");

  // Register default commands to Console
  register_command("exit", exit);
  register_command("quit", exit);
  register_command("help", help);
  register_command("generate", generate_tpcc);
  register_command("load", load_table);
  register_command("script", exec_script);

  // Register words specifically for command completion purposes, e.g.
  // for TPC-C table generation, 'CUSTOMER', 'DISTRICT', etc
  auto tpcc_generators = tpcc::TpccTableGenerator::tpcc_table_generator_functions();
  for (tpcc::TpccTableGeneratorFunctions::iterator it = tpcc_generators.begin(); it != tpcc_generators.end(); ++it) {
    _tpcc_commands.push_back(it->first);
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
  if (buffer == nullptr) {
    return ReturnCode::Quit;
  }

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
  // Do nothing if no input was given
  if (input.empty() && _multiline_input.empty()) {
    return ReturnCode::Ok;
  }

  // Dump command to logfile, and to the Console if input comes from a script file
  out(_prompt + input + "\n", _verbose);

  // Check if we already are in multiline input
  if (_multiline_input.empty()) {
    // Check if a registered command was entered
    RegisteredCommands::iterator it;
    if ((it = _commands.find(input.substr(0, input.find_first_of(" \n;")))) != std::end(_commands)) {
      return _eval_command(it->second, input);
    }

    // Regard query as complete if input is valid and not already in multiline
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(input, &parse_result);
    if (parse_result.isValid()) {
      return _eval_sql(input);
    }
  }

  // Regard query as complete if last character is semicolon, regardless of multiline or not
  if (input.back() == ';') {
    int retCode = _eval_sql(_multiline_input + input);
    _multiline_input = "";
    return retCode;
  }

  // If query is not complete(/valid), and the last character is not a semicolon, enter/continue multiline
  _multiline_input += input;
  _multiline_input += ' ';
  return ReturnCode::Multiline;
}

int Console::_eval_command(const CommandFunction& func, const std::string& command) {
  std::string cmd = command;
  if (command.back() == ';') {
    cmd = command.substr(0, command.size() - 1);
  }
  boost::algorithm::trim<std::string>(cmd);

  size_t first = cmd.find(' ');
  size_t last = cmd.find('\n');

  // If no whitespace is found, zero arguments are provided
  if (std::string::npos == first) {
    return static_cast<int>(func(""));
  }

  std::string args = cmd.substr(first + 1, last - (first + 1));

  // Remove whitespace duplicates in args
  auto both_are_spaces = [](char lhs, char rhs) { return (lhs == rhs) && (lhs == ' '); };
  args.erase(std::unique(args.begin(), args.end(), both_are_spaces), args.end());

  return static_cast<int>(func(args));
}

int Console::_eval_sql(const std::string& sql) {
  SQLQueryPlan plan;
  hsql::SQLParserResult parse_result;

  // Measure the query parse time
  auto started = std::chrono::high_resolution_clock::now();

  try {
    hsql::SQLParser::parse(sql, &parse_result);
  } catch (const std::exception& exception) {
    out("Exception thrown while parsing SQL query:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  // Measure the query parse time
  auto done = std::chrono::high_resolution_clock::now();
  auto parse_elapsed_ms = std::chrono::duration<double>(done - started).count();

  // Check if SQL query is valid
  if (!parse_result.isValid()) {
    out("Error: SQL query not valid.\n");
    return 1;
  }

  // Measure the plan compile time
  started = std::chrono::high_resolution_clock::now();

  // Compile the parse result
  try {
    plan = SQLPlanner::plan(parse_result);
  } catch (const std::exception& exception) {
    out("Exception thrown while compiling query plan:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  // Measure the plan compile time
  done = std::chrono::high_resolution_clock::now();
  auto plan_elapsed_ms = std::chrono::duration<double>(done - started).count();

  // Measure the query plan execution time
  started = std::chrono::high_resolution_clock::now();

  // Execute query plan
  try {
    // Compile the parse result
    plan = SQLPlanner::plan(parse_result);
    for (const auto& task : plan.tasks()) {
      task->get_operator()->execute();
    }
  } catch (const std::exception& exception) {
    out("Exception thrown while executing query plan:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  // Measure the query plan execution time
  done = std::chrono::high_resolution_clock::now();
  auto execution_elapsed_ms = std::chrono::duration<double>(done - started).count();

  auto table = plan.tree_roots().back()->get_output();
  auto row_count = table->row_count();

  // Print result (to Console and logfile)
  out(table);
  out("===\n");
  out(std::to_string(row_count) + " rows (PARSE: " + std::to_string(parse_elapsed_ms) + " ms, COMPILE: " +
      std::to_string(plan_elapsed_ms) + " ms, EXECUTE: " + std::to_string(execution_elapsed_ms) + " ms (wall time))\n");

  return ReturnCode::Ok;
}

void Console::register_command(const std::string& name, const CommandFunction& func) { _commands[name] = func; }

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
  console.out("  script SCRIPTFILE    - Execute script specified by SCRIPTFILE\n");
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
    return ReturnCode::Error;
  }

  const std::string& filepath = arguments.at(0);
  const std::string& tablename = arguments.at(1);

  std::vector<std::string> file_parts;
  boost::algorithm::split(file_parts, filepath, boost::is_any_of("."));
  const std::string& extension = file_parts.back();

  console.out("Loading " + filepath + " into table \"" + tablename + "\" ...\n");
  if (extension == "csv") {
    auto importer = std::make_shared<ImportCsv>(filepath, tablename);
    try {
      importer->execute();
    } catch (const std::exception& exception) {
      console.out("Exception thrown while importing CSV:\n  " + std::string(exception.what()) + "\n");
      return ReturnCode::Error;
    }
  } else if (extension == "tbl") {
    try {
      auto table = opossum::load_table(filepath, 0);
      StorageManager::get().add_table(tablename, table);
    } catch (const std::exception& exception) {
      console.out("Exception thrown while importing TBL:\n  " + std::string(exception.what()) + "\n");
      return ReturnCode::Error;
    }
  } else {
    console.out("Error: Unsupported file extension '" + extension + "'\n");
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

void Console::handle_signal(int sig) {
  if (sig == SIGINT) {
    // Reset console state
    auto& console = Console::get();
    console._out << "\n";
    console._multiline_input = "";
    console._prompt = "!> ";
    console._verbose = false;
    // Restore program state stored in jmp_env set with sigsetjmp(2)
    siglongjmp(jmp_env, 1);
  }
}

// GNU readline interface to our commands

char** Console::command_completion(const char* text, int start, int end) {
  char** completion_matches = nullptr;

  std::string input(rl_line_buffer);

  // Remove whitespace duplicates to not get empty tokens after boost::algorithm::split
  auto both_are_spaces = [](char lhs, char rhs) { return (lhs == rhs) && (lhs == ' '); };
  input.erase(std::unique(input.begin(), input.end(), both_are_spaces), input.end());

  std::vector<std::string> tokens;
  boost::algorithm::split(tokens, input, boost::is_space());

  // Choose completion function depending on the input. If it starts with "generate",
  // suggest TPC-C tablenames for completion.
  const std::string& first_word = tokens.at(0);
  if (first_word == "generate") {
    // Completion only for two words, "generate", and the TABLENAME
    if (tokens.size() <= 2) {
      completion_matches = rl_completion_matches(text, &Console::command_generator_tpcc);
    }
    // Turn off filepath completion for TPC-C table generation
    rl_attempted_completion_over = 1;
  } else if (first_word == "quit" || first_word == "exit" || first_word == "help") {
    // Turn off filepath completion
    rl_attempted_completion_over = 1;
  } else if ((first_word == "load" || first_word == "script") && tokens.size() > 2) {
    // Turn off filepath completion after first argument for "load" and "script"
    rl_attempted_completion_over = 1;
  } else if (start == 0) {
    completion_matches = rl_completion_matches(text, &Console::command_generator);
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

  // Bind CTRL-C to behaviour specified in opossum::Console::handle_signal
  std::signal(SIGINT, &opossum::Console::handle_signal);

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

  // Set jmp_env to current program state in preparation for siglongjmp(2)
  while (sigsetjmp(jmp_env, 1) != 0) {
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
