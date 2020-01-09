#include "console.hpp"

#include <readline/history.h>
#include <readline/readline.h>
#include <sys/stat.h>

#include <chrono>
#include <csetjmp>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptors.hpp>

#include "SQLParser.h"
#include "concurrency/transaction_context.hpp"
#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/export.hpp"
#include "operators/get_table.hpp"
#include "operators/import.hpp"
#include "operators/print.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/optimizer.hpp"
#include "pagination.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "storage/chunk_encoder.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/invalid_input_exception.hpp"
#include "utils/load_table.hpp"
#include "utils/string_utils.hpp"
#include "visualization/join_graph_visualizer.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

#define ANSI_COLOR_RED "\x1B[31m"    // NOLINT
#define ANSI_COLOR_GREEN "\x1B[32m"  // NOLINT
#define ANSI_COLOR_RESET "\x1B[0m"   // NOLINT

#define ANSI_COLOR_RED_RL "\001\x1B[31m\002"    // NOLINT
#define ANSI_COLOR_GREEN_RL "\001\x1B[32m\002"  // NOLINT
#define ANSI_COLOR_RESET_RL "\001\x1B[0m\002"   // NOLINT

namespace {

/**
 * Buffer for program state
 *
 * We use this to make Ctrl+C work on all platforms by jumping back into main() from the Ctrl+C signal handler. This
 * was the only way to get this to work on all platforms inclusing macOS.
 * See here (https://github.com/hyrise/hyrise/pull/198#discussion_r135539719) for a discussion about this.
 *
 * The known caveats of goto/longjmp aside, this will probably also cause problems (queries continuing to run in the
 * background) when the scheduler/multithreading is enabled.
 */
sigjmp_buf jmp_env;

// Returns a string containing a timestamp of the current date and time
std::string current_timestamp() {
  auto t = std::time(nullptr);
  auto tm = *std::localtime(&t);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return oss.str();
}

// Removes the coloring commands (e.g. '\x1B[31m') from input, to have a clean logfile.
// If remove_rl_codes_only is true, then it only removes the Readline specific escape sequences '\001' and '\002'
std::string remove_coloring(const std::string& input, bool remove_rl_codes_only = false) {
  // matches any characters that need to be escaped in RegEx except for '|'
  std::regex special_chars{R"([-[\]{}()*+?.,\^$#\s])"};
  std::string sequences = "\x1B[31m|\x1B[32m|\x1B[0m|\001|\002";
  if (remove_rl_codes_only) {
    sequences = "\001|\002";
  }
  std::string sanitized_sequences = std::regex_replace(sequences, special_chars, R"(\$&)");

  // Remove coloring commands and escape sequences before writing to logfile
  std::regex expression{"(" + sanitized_sequences + ")"};
  return std::regex_replace(input, expression, "");
}

std::vector<std::string> tokenize(std::string input) {
  boost::algorithm::trim<std::string>(input);

  // Remove whitespace duplicates to not get empty tokens after boost::algorithm::split
  auto both_are_spaces = [](char left, char right) { return (left == right) && (left == ' '); };
  input.erase(std::unique(input.begin(), input.end(), both_are_spaces), input.end());

  std::vector<std::string> tokens;
  boost::algorithm::split(tokens, input, boost::is_space());

  return tokens;
}

}  // namespace

namespace opossum {

// Console implementation

Console::Console()
    : _prompt("> "),
      _multiline_input(""),
      _out(std::cout.rdbuf()),
      _log("console.log", std::ios_base::app | std::ios_base::out),
      _verbose(false),
      _pagination_active(false),
      _pqp_cache(std::make_shared<SQLPhysicalPlanCache>()),
      _lqp_cache(std::make_shared<SQLLogicalPlanCache>()) {
  // Init readline basics, tells readline to use our custom command completion function
  rl_attempted_completion_function = &Console::_command_completion;
  rl_completer_word_break_characters = const_cast<char*>(" \t\n\"\\'`@$><=;|&{(");  // NOLINT (legacy API)

  // Register default commands to Console
  register_command("exit", std::bind(&Console::_exit, this, std::placeholders::_1));
  register_command("quit", std::bind(&Console::_exit, this, std::placeholders::_1));
  register_command("help", std::bind(&Console::_help, this, std::placeholders::_1));
  register_command("generate_tpcc", std::bind(&Console::_generate_tpcc, this, std::placeholders::_1));
  register_command("generate_tpch", std::bind(&Console::_generate_tpch, this, std::placeholders::_1));
  register_command("generate_tpcds", std::bind(&Console::_generate_tpcds, this, std::placeholders::_1));
  register_command("load", std::bind(&Console::_load_table, this, std::placeholders::_1));
  register_command("export", std::bind(&Console::_export_table, this, std::placeholders::_1));
  register_command("script", std::bind(&Console::_exec_script, this, std::placeholders::_1));
  register_command("print", std::bind(&Console::_print_table, this, std::placeholders::_1));
  register_command("visualize", std::bind(&Console::_visualize, this, std::placeholders::_1));
  register_command("begin", std::bind(&Console::_begin_transaction, this, std::placeholders::_1));
  register_command("rollback", std::bind(&Console::_rollback_transaction, this, std::placeholders::_1));
  register_command("commit", std::bind(&Console::_commit_transaction, this, std::placeholders::_1));
  register_command("txinfo", std::bind(&Console::_print_transaction_info, this, std::placeholders::_1));
  register_command("pwd", std::bind(&Console::_print_current_working_directory, this, std::placeholders::_1));
  register_command("setting", std::bind(&Console::_change_runtime_setting, this, std::placeholders::_1));
  register_command("load_plugin", std::bind(&Console::_load_plugin, this, std::placeholders::_1));
  register_command("unload_plugin", std::bind(&Console::_unload_plugin, this, std::placeholders::_1));
}

int Console::read() {
  char* buffer;

  // Prompt user for input
  buffer = readline(_prompt.c_str());
  if (!buffer) {
    return ReturnCode::Quit;
  }

  std::string input(buffer);
  boost::algorithm::trim<std::string>(input);

  // Only save non-empty commands to history
  if (!input.empty()) {
    add_history(buffer);
    // Save command to history file
    if (!_history_file.empty()) {
      if (append_history(1, _history_file.c_str()) != 0) {
        out("Error appending to history file: " + _history_file + "\n");
      }
    }
  }

  // Free buffer, since readline() allocates new string every time
  free(buffer);  // NOLINT (legacy API)

  return _eval(input);
}

int Console::execute_script(const std::string& filepath) { return _exec_script(filepath); }

int Console::_eval(const std::string& input) {
  // Do nothing if no input was given
  if (input.empty() && _multiline_input.empty()) {
    return ReturnCode::Ok;
  }

  // Dump command to logfile, and to the Console if input comes from a script file
  // Also remove Readline specific escape sequences ('\001' and '\002') to make it look normal
  out(remove_coloring(_prompt + input + "\n", true), _verbose);

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
    int return_code = _eval_sql(_multiline_input + input);
    _multiline_input = "";
    return return_code;
  }

  // If query is not complete(/valid), and the last character is not a semicolon, enter/continue multiline
  _multiline_input += input;
  _multiline_input += '\n';
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
  auto both_are_spaces = [](char left, char right) { return (left == right) && (left == ' '); };
  args.erase(std::unique(args.begin(), args.end(), both_are_spaces), args.end());

  return static_cast<int>(func(args));
}

bool Console::_initialize_pipeline(const std::string& sql) {
  try {
    auto builder = SQLPipelineBuilder{sql}
                       .with_lqp_cache(_lqp_cache)
                       .with_pqp_cache(_pqp_cache)
                       .dont_cleanup_temporaries();  // keep tables for debugging and visualization
    if (_explicitly_created_transaction_context) {
      builder.with_transaction_context(_explicitly_created_transaction_context);
    }
    _sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
  } catch (const InvalidInputException& exception) {
    out(std::string(exception.what()) + '\n');
    return false;
  }

  return true;
}

int Console::_eval_sql(const std::string& sql) {
  if (!_initialize_pipeline(sql)) return ReturnCode::Error;

  try {
    _sql_pipeline->get_result_tables();
  } catch (const InvalidInputException& exception) {
    out(std::string(exception.what()) + "\n");
    if (_handle_rollback() && !_explicitly_created_transaction_context && _sql_pipeline->statement_count() > 1) {
      out("All previous statements have been committed.\n");
    }
    return ReturnCode::Error;
  }

  const auto [pipeline_status, table] = _sql_pipeline->get_result_table();
  if (pipeline_status == SQLPipelineStatus::RolledBack) {
    _handle_rollback();
    out("A transaction conflict has been detected:");
    out(_sql_pipeline->failed_pipeline_statement()->get_sql_string());
    if (_explicitly_created_transaction_context) {
      out("The transaction has been rolled back");
    } else {
      out("The statement was rolled back, but previous statements have been auto-committed");
    }
    return ReturnCode::Error;
  }
  Assert(pipeline_status == SQLPipelineStatus::Success, "Unexpected pipeline status");

  auto row_count = table ? table->row_count() : 0;

  // Print result (to Console and logfile)
  if (table) {
    out(table);
  }

  out("===\n");
  out(std::to_string(row_count) + " rows total\n");

  std::ostringstream stream;
  stream << _sql_pipeline->metrics();

  out(stream.str());

  return ReturnCode::Ok;
}

void Console::register_command(const std::string& name, const CommandFunction& func) { _commands[name] = func; }

Console::RegisteredCommands Console::commands() { return _commands; }

void Console::set_prompt(const std::string& prompt) {
  if (HYRISE_DEBUG) {
    _prompt = ANSI_COLOR_RED_RL "(debug)" ANSI_COLOR_RESET_RL + prompt;
  } else {
    _prompt = ANSI_COLOR_GREEN_RL "(release)" ANSI_COLOR_RESET_RL + prompt;
  }
}

void Console::set_logfile(const std::string& logfile) {
  _log = std::ofstream(logfile, std::ios_base::app | std::ios_base::out);
}

void Console::load_history(const std::string& history_file) {
  _history_file = history_file;

  // Check if history file exist, create empty history file if not
  std::ifstream file(_history_file);
  if (!file.good()) {
    out("Creating history file: " + _history_file + "\n");
    if (write_history(_history_file.c_str()) != 0) {
      out("Error creating history file: " + _history_file + "\n");
      return;
    }
  }

  if (read_history(_history_file.c_str()) != 0) {
    out("Error reading history file: " + _history_file + "\n");
  }
}

void Console::out(const std::string& output, bool console_print) {
  if (console_print) {
    _out << output;
  }
  // Remove coloring commands like '\x1B[32m' when writing to logfile
  _log << remove_coloring(output);
  _log.flush();
}

void Console::out(const std::shared_ptr<const Table>& table, const PrintFlags flags) {
  int size_y;
  int size_x;
  rl_get_screen_size(&size_y, &size_x);

  const bool fits_on_one_page = table->row_count() < static_cast<uint64_t>(size_y) - 1;

  static bool pagination_disabled = false;
  if (!fits_on_one_page && !std::getenv("TERM") && !pagination_disabled) {
    out("Your TERM environment variable is not set - most likely because you are running the console from an IDE. "
        "Pagination is disabled.\n\n");
    pagination_disabled = true;
  }

  // Paginate only if table has more rows that fit in the terminal
  if (fits_on_one_page || pagination_disabled) {
    Print::print(table, flags, _out);
  } else {
    std::stringstream stream;
    Print::print(table, flags, stream);
    _pagination_active = true;
    Pagination(stream).display();
    _pagination_active = false;
  }
}

// Command functions

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
int Console::_exit(const std::string&) { return Console::ReturnCode::Quit; }

int Console::_help(const std::string&) {
  auto encoding_options = std::string{"                                                 Encoding options: "};
  encoding_options += boost::algorithm::join(
      encoding_type_to_string.right | boost::adaptors::transformed([](auto it) { return it.first; }), ", ");
  // Split the encoding options in lines of 120 and add padding. For each input line, it takes up to 120 characters
  // and replaces the following space(s) with a new line. `(?: +|$)` is a non-capturing group that matches either
  // a non-zero number of spaces or the end of the line.
  auto line_wrap = std::regex{"(.{1,120})(?: +|$)"};
  encoding_options =
      regex_replace(encoding_options, line_wrap, "$1\n                                                    ");
  // Remove the 49 spaces and the new line added at the end
  encoding_options.resize(encoding_options.size() - 50);

  // clang-format off
  out("HYRISE SQL Interface\n\n");
  out("Available commands:\n");
  out("  generate tpcc NUM_WAREHOUSES [CHUNK_SIZE] - Generate all TPC-C tables\n");
  out("  generate_tpch SCALE_FACTOR [CHUNK_SIZE] - Generate all TPC-H tables\n");
  out("  generate_tpcds SCALE_FACTOR [CHUNK_SIZE] - Generate all TPC-DS tables\n");
  out("  load FILEPATH [TABLENAME [ENCODING]]    - Load table from disk specified by filepath FILEPATH, store it with name TABLENAME\n");  // NOLINT
  out("                                               The import type is chosen by the type of FILEPATH.\n");
  out("                                                 Supported types: '.bin', '.csv', '.tbl'\n");
  out("                                               If no table name is specified, the filename without extension is used\n");  // NOLINT
  out(encoding_options + "\n");  // NOLINT
  out("  export TABLENAME FILEPATH               - Export table named TABLENAME from storage manager to filepath FILEPATH\n");  // NOLINT
  out("                                               The export type is chosen by the type of FILEPATH.\n");
  out("                                                 Supported types: '.bin', '.csv'\n");
  out("  script SCRIPTFILE                       - Execute script specified by SCRIPTFILE\n");
  out("  print TABLENAME                         - Fully print the given table (including MVCC data)\n");
  out("  visualize [options] [SQL]               - Visualize a SQL query\n");
  out("                                               Options\n");
  out("                                                - {exec, noexec} Execute the query before visualization.\n");
  out("                                                                 Default: exec\n");
  out("                                                - {lqp, unoptlqp, pqp, joins} Type of plan to visualize. unoptlqp gives the\n");  // NOLINT
  out("                                                                       unoptimized lqp; joins visualized the join graph.\n");  // NOLINT
  out("                                                                       Default: pqp\n");
  out("                                              SQL\n");
  out("                                                - Optional, a query to visualize. If not specified, the last\n");
  out("                                                  previously executed query is visualized.\n");
  out("  begin                                   - Manually create a new transaction (Auto-commit is active unless begin is called)\n");  // NOLINT
  out("  rollback                                - Roll back a manually created transaction\n");
  out("  commit                                  - Commit a manually created transaction\n");
  out("  txinfo                                  - Print information on the current transaction\n");
  out("  pwd                                     - Print current working directory\n");
  out("  load_plugin FILE                        - Load and start plugin stored at FILE\n");
  out("  unload_plugin NAME                      - Stop and unload the plugin libNAME.so/dylib (also clears the query cache)\n");  // NOLINT
  out("  quit                                    - Exit the HYRISE Console\n");
  out("  help                                    - Show this message\n\n");
  out("  setting [property] [value]              - Change a runtime setting\n\n");
  out("           scheduler (on|off)             - Turn the scheduler on (default) or off\n\n");
  // clang-format on

  return Console::ReturnCode::Ok;
}

int Console::_generate_tpcc(const std::string& args) {
  const auto arguments = tokenize(args);

  if (arguments.empty() || arguments.size() > 2) {
    // clang-format off
    out("Usage: ");
    out("  generate_tpcc NUM_WAREHOUSES [CHUNK_SIZE]   Generate TPC-C tables with the specified number of warehouses. \n");  // NOLINT
    out("                                              Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) + " by default. \n");  // NOLINT
    // clang-format on
    return ReturnCode::Error;
  }

  auto num_warehouses = std::stoi(arguments[1]);

  auto chunk_size = Chunk::DEFAULT_SIZE;
  if (arguments.size() > 1) {
    chunk_size = boost::lexical_cast<ChunkOffset>(arguments[1]);
  }

  out("Generating all TPCC tables (this might take a while) ...\n");
  TPCCTableGenerator{num_warehouses, chunk_size}.generate_and_store();

  return ReturnCode::Ok;
}

int Console::_generate_tpch(const std::string& args) {
  const auto arguments = tokenize(args);

  if (arguments.empty() || arguments.size() > 2) {
    // clang-format off
    out("Usage: ");
    out("  generate_tpch SCALE_FACTOR [CHUNK_SIZE]   Generate TPC-H tables with the specified scale factor. \n");
    out("                                            Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) + " by default. \n");  // NOLINT
    // clang-format on
    return ReturnCode::Error;
  }

  auto scale_factor = std::stof(arguments[0]);

  auto chunk_size = Chunk::DEFAULT_SIZE;
  if (arguments.size() > 1) {
    chunk_size = boost::lexical_cast<ChunkOffset>(arguments[1]);
  }

  out("Generating all TPCH tables (this might take a while) ...\n");
  TPCHTableGenerator{scale_factor, chunk_size}.generate_and_store();

  return ReturnCode::Ok;
}

int Console::_generate_tpcds(const std::string& args) {
  const auto arguments = tokenize(args);

  if (arguments.empty() || arguments.size() > 2) {
    out("Usage: ");
    out("  generate_tpcds SCALE_FACTOR [CHUNK_SIZE]   Generate TPC-DS tables with the specified scale factor. \n");
    out("                                             Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) +
        " by default. \n");
    return ReturnCode::Error;
  }

  auto scale_factor = static_cast<uint32_t>(std::stoul(arguments[0]));

  auto chunk_size = Chunk::DEFAULT_SIZE;
  if (arguments.size() > 1) {
    chunk_size = boost::lexical_cast<ChunkOffset>(arguments[1]);
  }

  out("Generating all TPC-DS tables (this might take a while) ...\n");
  TpcdsTableGenerator{scale_factor, chunk_size}.generate_and_store();

  return ReturnCode::Ok;
}

int Console::_load_table(const std::string& args) {
  std::vector<std::string> arguments = trim_and_split(args);

  if (arguments.empty() || arguments.size() > 3) {
    out("Usage:\n");
    out("  load FILEPATH [TABLENAME [ENCODING]]\n");
    return ReturnCode::Error;
  }

  const auto filepath = std::filesystem::path{arguments[0]};
  const auto tablename = arguments.size() >= 2 ? arguments[1] : std::string{filepath.stem()};

  out("Loading " + std::string(filepath) + " into table \"" + tablename + "\"\n");

  if (Hyrise::get().storage_manager.has_table(tablename)) {
    out("Table \"" + tablename + "\" already existed. Replacing it.\n");
  }

  try {
    auto importer = std::make_shared<Import>(filepath, tablename, Chunk::DEFAULT_SIZE);
    importer->execute();
  } catch (const std::exception& exception) {
    out("Error: Exception thrown while importing table:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  const std::string encoding = arguments.size() == 3 ? arguments[2] : "Unencoded";

  const auto encoding_type = encoding_type_to_string.right.find(encoding);
  if (encoding_type == encoding_type_to_string.right.end()) {
    const auto encoding_options = boost::algorithm::join(
        encoding_type_to_string.right | boost::adaptors::transformed([](auto it) { return it.first; }), ", ");
    out("Error: Invalid encoding type: '" + encoding + "', try one of these: " + encoding_options + "\n");
    return ReturnCode::Error;
  }

  // Check if the specified encoding can be used
  const auto& table = Hyrise::get().storage_manager.get_table(tablename);
  bool supported = true;
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    if (!encoding_supports_data_type(encoding_type->second, table->column_data_type(column_id))) {
      out("Encoding \"" + encoding + "\" not supported for column \"" + table->column_name(column_id) +
          "\", table left unencoded\n");
      supported = false;
    }
  }

  if (supported) {
    out("Encoding \"" + tablename + "\" using " + encoding + "\n");
    std::vector<ChunkID> immutable_chunks;
    for (ChunkID chunk_id(0); chunk_id < table->chunk_count(); ++chunk_id) {
      if (!table->get_chunk(chunk_id)->is_mutable()) {
        immutable_chunks.push_back(chunk_id);
      }
    }
    ChunkEncoder::encode_chunks(table, immutable_chunks, encoding_type->second);
  }

  return ReturnCode::Ok;
}

int Console::_export_table(const std::string& args) {
  std::vector<std::string> arguments = trim_and_split(args);

  if (arguments.size() != 2) {
    out("Usage:\n");
    out("  export TABLENAME FILEPATH\n");
    return ReturnCode::Error;
  }

  const std::string& tablename = arguments[0];
  const std::string& filepath = arguments[1];

  auto& storage_manager = Hyrise::get().storage_manager;
  if (!storage_manager.has_table(tablename)) {
    out("Error: Table does not exist in StorageManager");
    return ReturnCode::Error;
  }

  out("Exporting \"" + tablename + "\" into \"" + filepath + "\" ...\n");
  auto get_table = std::make_shared<GetTable>(tablename);
  get_table->execute();

  auto exporter = std::make_shared<Export>(get_table, filepath);
  try {
    exporter->execute();
  } catch (const std::exception& exception) {
    out("Error: Exception thrown while exporting:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  return ReturnCode::Ok;
}

int Console::_print_table(const std::string& args) {
  std::vector<std::string> arguments = trim_and_split(args);

  if (arguments.size() != 1) {
    out("Usage:\n");
    out("  print TABLENAME\n");
    return ReturnCode::Error;
  }

  const std::string& tablename = arguments.at(0);

  auto get_table = std::make_shared<GetTable>(tablename);
  try {
    get_table->execute();
  } catch (const std::exception& exception) {
    out("Error: Exception thrown while loading table:\n  " + std::string(exception.what()) + "\n");
    return ReturnCode::Error;
  }

  out(get_table->get_output(), PrintFlags::Mvcc);

  return ReturnCode::Ok;
}

int Console::_visualize(const std::string& input) {
  /**
   * "visualize" supports three dimensions of options:
   *    - "noexec"; or implicit "exec", the execution of the specified query
   *    - "lqp", "unoptlqp", "joins"; or implicit "pqp"
   *    - a sql query can either be specified or not. If it isn't, the last previously executed query is visualized
   */

  std::vector<std::string> input_words;
  boost::algorithm::split(input_words, input, boost::is_any_of(" \n"));

  constexpr auto EXEC = "exec";
  constexpr auto NOEXEC = "noexec";
  constexpr auto PQP = "pqp";
  constexpr auto LQP = "lqp";
  constexpr auto UNOPTLQP = "unoptlqp";
  constexpr auto JOINS = "joins";

  // Determine whether the specified query is to be executed before visualization
  auto no_execute = false;  // Default
  if (input_words.front() == NOEXEC || input_words.front() == EXEC) {
    no_execute = input_words.front() == NOEXEC;
    input_words.erase(input_words.begin());
  }

  // Determine the plan type to visualize
  enum class PlanType { LQP, UnoptLQP, PQP, Joins };
  auto plan_type = PlanType::PQP;
  auto plan_type_str = std::string{"pqp"};
  if (input_words.front() == LQP || input_words.front() == UNOPTLQP || input_words.front() == PQP ||
      input_words.front() == JOINS) {
    if (input_words.front() == LQP) {
      plan_type = PlanType::LQP;
    } else if (input_words.front() == UNOPTLQP) {
      plan_type = PlanType::UnoptLQP;
    } else if (input_words.front() == JOINS) {
      plan_type = PlanType::Joins;
    }

    plan_type_str = input_words.front();
    input_words.erase(input_words.begin());
  }

  // Removes plan type and noexec (+ leading whitespace) so that only the sql string is left.
  const auto sql = boost::algorithm::join(input_words, " ");

  // If no SQL is provided, use the last execution. Else, create a new pipeline.
  if (!sql.empty()) {
    if (!_initialize_pipeline(sql)) return ReturnCode::Error;
  }

  // If there is no pipeline (i.e., neither was SQL passed in with the visualize command,
  // nor was there a previous execution), return an error
  if (!_sql_pipeline) {
    out("Error: Nothing to visualize.\n");
    return ReturnCode::Error;
  }

  if (no_execute && !sql.empty() && _sql_pipeline->requires_execution()) {
    out("Error: We do not support the visualization of multiple dependant statements in 'noexec' mode.\n");
    return ReturnCode::Error;
  }

  const auto img_filename = plan_type_str + ".png";

  switch (plan_type) {
    case PlanType::LQP:
    case PlanType::UnoptLQP: {
      std::vector<std::shared_ptr<AbstractLQPNode>> lqp_roots;

      try {
        const auto& lqps = (plan_type == PlanType::LQP) ? _sql_pipeline->get_optimized_logical_plans()
                                                        : _sql_pipeline->get_unoptimized_logical_plans();
        for (const auto& lqp : lqps) {
          lqp_roots.push_back(lqp);
        }
      } catch (const std::exception& exception) {
        out(std::string(exception.what()) + "\n");
        _handle_rollback();
        return ReturnCode::Error;
      }

      LQPVisualizer visualizer;
      visualizer.visualize(lqp_roots, img_filename);
    } break;

    case PlanType::PQP: {
      try {
        if (!no_execute) {
          _sql_pipeline->get_result_table();
        }

        PQPVisualizer visualizer;
        visualizer.visualize(_sql_pipeline->get_physical_plans(), img_filename);
      } catch (const std::exception& exception) {
        out(std::string(exception.what()) + "\n");
        _handle_rollback();
        return ReturnCode::Error;
      }
    } break;

    case PlanType::Joins: {
      out("NOTE: Join graphs will show only Cross and Inner joins, not Semi, Left, Right, Full outer, "
          "AntiNullAsTrue and AntiNullAsFalse joins.\n");

      auto join_graphs = std::vector<JoinGraph>{};

      const auto& lqps = _sql_pipeline->get_optimized_logical_plans();
      for (const auto& lqp : lqps) {
        const auto sub_lqps = lqp_find_subplan_roots(lqp);

        for (const auto& sub_lqp : sub_lqps) {
          const auto sub_lqp_join_graphs = JoinGraph::build_all_in_lqp(sub_lqp);
          for (auto& sub_lqp_join_graph : sub_lqp_join_graphs) {
            join_graphs.emplace_back(sub_lqp_join_graph);
          }
        }
      }

      JoinGraphVisualizer visualizer;
      visualizer.visualize(join_graphs, img_filename);
    } break;
  }

  auto scripts_dir = std::string{"./scripts/"};
  auto ret = system((scripts_dir + "planviz/is_iterm2.sh 2>/dev/null").c_str());
  if (ret != 0) {
    // Try in parent directory
    scripts_dir = std::string{"."} + scripts_dir;
    ret = system((scripts_dir + "planviz/is_iterm2.sh").c_str());
  }
  if (ret != 0) {
    std::string msg{"Currently, only iTerm2 can print the visualization inline. You can find the plan at "};
    msg += img_filename + "\n";
    out(msg);

    return ReturnCode::Ok;
  }

  auto cmd = scripts_dir + "/planviz/imgcat.sh " + img_filename;
  ret = system(cmd.c_str());
  Assert(ret == 0, "Printing the image using ./scripts/imgcat.sh failed.");

  return ReturnCode::Ok;
}

int Console::_change_runtime_setting(const std::string& input) {
  auto property = input.substr(0, input.find_first_of(" \n"));
  auto value = input.substr(input.find_first_of(" \n") + 1, input.size());

  if (property == "scheduler") {
    if (value == "on") {
      Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
      out("Scheduler turned on\n");
    } else if (value == "off") {
      Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
      out("Scheduler turned off\n");
    } else {
      out("Usage: scheduler (on|off)\n");
      return 1;
    }
    return 0;
  }

  out("Error: Unknown property\n");
  return 1;
}

int Console::_exec_script(const std::string& script_file) {
  auto filepath = script_file;
  boost::algorithm::trim(filepath);
  std::ifstream script(filepath);

  const auto is_regular_file = [](const std::string& path) {
    struct stat path_stat {};
    stat(path.c_str(), &path_stat);
    return S_ISREG(path_stat.st_mode);  // NOLINT
  };

  if (!script.good()) {
    out("Error: Script file '" + filepath + "' does not exist.\n");
    return ReturnCode::Error;
  } else if (!is_regular_file(filepath)) {
    out("Error: '" + filepath + "' is not a regular file.\n");
    return ReturnCode::Error;
  }

  out("Executing script file: " + filepath + "\n");
  _verbose = true;
  std::string command;
  int return_code = ReturnCode::Ok;
  while (std::getline(script, command)) {
    return_code = _eval(command);
    if (return_code == ReturnCode::Error || return_code == ReturnCode::Quit) {
      break;
    }
  }
  out("Executing script file done\n");
  _verbose = false;
  return return_code;
}

void Console::handle_signal(int sig) {
  if (sig == SIGINT) {
    auto& console = Console::get();
    // When in pagination mode, just quit pagination. Otherwise, reset Console.
    if (console._pagination_active) {
      Pagination::push_ctrl_c();
    } else {
      // Reset console state
      console._out << "\n";
      console._multiline_input = "";
      console.set_prompt("!> ");
      console._verbose = false;
      // Restore program state stored in jmp_env set with sigsetjmp(2).
      // See comment on jmp_env for details
      siglongjmp(jmp_env, 1);
    }
  }
}

int Console::_begin_transaction(const std::string& input) {
  if (_explicitly_created_transaction_context) {
    const auto transaction_id = std::to_string(_explicitly_created_transaction_context->transaction_id());
    out("Error: There is already an active transaction (" + transaction_id + "). ");
    out("Type `rollback` or `commit` before beginning a new transaction.\n");
    return ReturnCode::Error;
  }

  _explicitly_created_transaction_context = Hyrise::get().transaction_manager.new_transaction_context();

  const auto transaction_id = std::to_string(_explicitly_created_transaction_context->transaction_id());
  out("New transaction (" + transaction_id + ") started.\n");
  return ReturnCode::Ok;
}

int Console::_rollback_transaction(const std::string& input) {
  if (!_explicitly_created_transaction_context) {
    out("Console is in auto-commit mode. Type `begin` to start a manual transaction.\n");
    return ReturnCode::Error;
  }

  _explicitly_created_transaction_context->rollback();

  const auto transaction_id = std::to_string(_explicitly_created_transaction_context->transaction_id());
  out("Transaction (" + transaction_id + ") has been rolled back.\n");

  _explicitly_created_transaction_context = nullptr;
  return ReturnCode::Ok;
}

int Console::_commit_transaction(const std::string& input) {
  if (!_explicitly_created_transaction_context) {
    out("Console is in auto-commit mode. Type `begin` to start a manual transaction.\n");
    return ReturnCode::Error;
  }

  _explicitly_created_transaction_context->commit();

  const auto transaction_id = std::to_string(_explicitly_created_transaction_context->transaction_id());
  out("Transaction (" + transaction_id + ") has been committed.\n");

  _explicitly_created_transaction_context = nullptr;
  return ReturnCode::Ok;
}

int Console::_print_transaction_info(const std::string& input) {
  if (!_explicitly_created_transaction_context) {
    out("Console is in auto-commit mode. Type `begin` to start a manual transaction.\n");
    return ReturnCode::Error;
  }

  const auto transaction_id = std::to_string(_explicitly_created_transaction_context->transaction_id());
  const auto snapshot_commit_id = std::to_string(_explicitly_created_transaction_context->snapshot_commit_id());
  out("Active transaction: { transaction id = " + transaction_id + ", snapshot commit id = " + snapshot_commit_id +
      " }\n");
  return ReturnCode::Ok;
}

int Console::_print_current_working_directory(const std::string&) {
  out(std::filesystem::current_path().string() + "\n");
  return ReturnCode::Ok;
}

int Console::_load_plugin(const std::string& args) {
  auto arguments = trim_and_split(args);

  if (arguments.size() != 1) {
    out("Usage:\n");
    out("  load_plugin PLUGINPATH\n");
    return ReturnCode::Error;
  }

  const std::string& plugin_path_str = arguments[0];

  const std::filesystem::path plugin_path(plugin_path_str);
  const auto plugin_name = plugin_name_from_path(plugin_path);

  Hyrise::get().plugin_manager.load_plugin(plugin_path);

  out("Plugin (" + plugin_name + ") successfully loaded.\n");

  return ReturnCode::Ok;
}

int Console::_unload_plugin(const std::string& input) {
  auto arguments = trim_and_split(input);

  if (arguments.size() != 1) {
    out("Usage:\n");
    out("  unload_plugin NAME\n");
    return ReturnCode::Error;
  }

  const std::string& plugin_name = arguments[0];

  Hyrise::get().plugin_manager.unload_plugin(plugin_name);

  // The presence of some plugins might cause certain query plans to be generated which will not work if the plugin
  // is stopped. Therefore, we clear the cache. For example, a plugin might create indexes which lead to query plans
  // using IndexScans, these query plans might become unusable after the plugin is unloaded.
  _pqp_cache->clear();

  out("Plugin (" + plugin_name + ") stopped.\n");

  return ReturnCode::Ok;
}

// GNU readline interface to our commands

char** Console::_command_completion(const char* text, int start, int end) {
  char** completion_matches = nullptr;

  std::string input(rl_line_buffer);

  const auto tokens = tokenize(input);

  // Choose completion function depending on the input.
  const std::string& first_word = tokens[0];
  if (first_word == "visualize") {
    // Completion only for three words, "visualize", and at most two options
    if (tokens.size() <= 3) {
      completion_matches = rl_completion_matches(text, &Console::_command_generator_visualize);
    }
    // Turn off filepath completion
    rl_attempted_completion_over = 1;
  } else if (first_word == "setting") {
    if (tokens.size() <= 2) {
      completion_matches = rl_completion_matches(text, &Console::_command_generator_setting);
    } else if (tokens.size() <= 3 && tokens[1] == "scheduler") {
      completion_matches = rl_completion_matches(text, &Console::_command_generator_setting_scheduler);
    }
    // Turn off filepath completion
    rl_attempted_completion_over = 1;

    // NOLINTNEXTLINE(bugprone-branch-clone)
  } else if (first_word == "quit" || first_word == "exit" || first_word == "help") {
    // Turn off filepath completion
    rl_attempted_completion_over = 1;
  } else if ((first_word == "load" || first_word == "script") && tokens.size() > 2) {
    // Turn off filepath completion after first argument for "load" and "script"
    rl_attempted_completion_over = 1;
  } else if (start == 0) {
    completion_matches = rl_completion_matches(text, &Console::_command_generator_default);
  }

  return completion_matches;
}

char* Console::_command_generator(const char* text, int state, const std::vector<std::string>& commands) {
  static std::vector<std::string>::const_iterator it;
  if (state == 0) {
    it = commands.begin();
  }

  while (it != commands.end()) {
    auto& command = *it;
    ++it;
    if (command.find(text) != std::string::npos) {
      auto completion = new char[command.size()];  // NOLINT (legacy API)
      snprintf(completion, command.size() + 1, "%s", command.c_str());
      return completion;
    }
  }
  return nullptr;
}

char* Console::_command_generator_default(const char* text, int state) {
  auto commands = std::vector<std::string>();
  for (auto const& command : Console::get()._commands) {
    commands.push_back(command.first);
  }
  return _command_generator(text, state, commands);
}

char* Console::_command_generator_visualize(const char* text, int state) {
  return _command_generator(text, state, {"exec", "noexec", "pqp", "lqp", "unoptlqp", "joins"});
}

char* Console::_command_generator_setting(const char* text, int state) {
  return _command_generator(text, state, {"scheduler"});
}

char* Console::_command_generator_setting_scheduler(const char* text, int state) {
  return _command_generator(text, state, {"on", "off"});
}

bool Console::_handle_rollback() {
  auto failed_pipeline = _sql_pipeline->failed_pipeline_statement();
  if (failed_pipeline && failed_pipeline->transaction_context() && failed_pipeline->transaction_context()->aborted()) {
    out("The transaction has been rolled back.\n");
    _explicitly_created_transaction_context = nullptr;
    return true;
  }

  return false;
}

}  // namespace opossum

int main(int argc, char** argv) {
  // Make sure the TransactionManager is initialized before the console so that we don't run into destruction order
  // problems (#1635)
  opossum::Hyrise::get();

  using Return = opossum::Console::ReturnCode;
  auto& console = opossum::Console::get();

  // Bind CTRL-C to behaviour specified in Console::handle_signal
  std::signal(SIGINT, &opossum::Console::handle_signal);

  console.set_prompt("> ");
  console.set_logfile("console.log");

  // Load command history
  console.load_history(".repl_history");

  // Timestamp dump only to logfile
  console.out("--- Session start --- " + current_timestamp() + "\n", false);

  int return_code = Return::Ok;

  // Display Usage if too many arguments are provided
  if (argc > 2) {
    return_code = Return::Quit;
    console.out("Usage:\n");
    console.out("  ./hyriseConsole [SCRIPTFILE] - Start the interactive SQL interface.\n");
    console.out("                                 Execute script if specified by SCRIPTFILE.\n");
  }

  // Execute .sql script if specified
  if (argc == 2) {
    return_code = console.execute_script(std::string(argv[1]));
    // Terminate Console if an error occured during script execution
    if (return_code == Return::Error) {
      return_code = Return::Quit;
    }
  }

  // Display welcome message if Console started normally
  if (argc == 1) {
    console.out("HYRISE SQL Interface\n");
    console.out("Type 'help' for more information.\n\n");

    console.out("Hyrise is running a ");
    if (HYRISE_DEBUG) {
      console.out(ANSI_COLOR_RED "(debug)" ANSI_COLOR_RESET);
    } else {
      console.out(ANSI_COLOR_GREEN "(release)" ANSI_COLOR_RESET);
    }
    console.out(" build.\n\n");
  }

  // Set jmp_env to current program state in preparation for siglongjmp(2)
  // See comment on jmp_env for details
  while (sigsetjmp(jmp_env, 1) != 0) {
  }

  // Main REPL loop
  while (return_code != Return::Quit) {
    return_code = console.read();
    if (return_code == Return::Ok) {
      console.set_prompt("> ");
    } else if (return_code == Return::Multiline) {
      console.set_prompt("... ");
    } else {
      console.set_prompt("!> ");
    }
  }

  console.out("Bye.\n");

  // Timestamp dump only to logfile
  console.out("--- Session end --- " + current_timestamp() + "\n", false);
}
