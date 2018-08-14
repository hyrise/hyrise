#pragma once

#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/table.hpp"

namespace opossum {

class TransactionContext;

/*
 * SQL REPL Console for Opossum, built on GNU readline. https://cnswww.cns.cwru.edu/php/chet/readline/rltop.html
 * Can load TPCC tables via "generate TABLENAME" command, and can execute SQL statements based on
 * opossum::SqlQueryTranslator.
 */
class Console {
 public:
  using CommandFunction = std::function<int(const std::string&)>;
  using RegisteredCommands = std::unordered_map<std::string, CommandFunction>;

  enum ReturnCode { Multiline = -2, Quit = -1, Ok = 0, Error = 1 };

  static Console& get();

  /*
   * Prompts user for one line of input, evaluates the given input, and prints out the result.
   *
   * @returns ReturnCode::Quit if Console should be terminated,
   *          ReturnCode::Error if an error occured,
   *          ReturnCode::Multiline if the last command ended with a '\' and should be continued next line,
   *          ReturnCode::Ok if the input was evaluated/executed correctly.
   */
  int read();

  int execute_script(const std::string& filepath);

  /*
   * Register a custom command which can be called from the console.
   */
  void register_command(const std::string& name, const CommandFunction& func);
  RegisteredCommands commands();

  /*
   * Set prompt which is shown at the beginning of each line.
   */
  void set_prompt(const std::string& prompt);

  /*
   * Set logfile path.
   */
  void set_logfile(const std::string& logfile);

  /*
   * Load command history from history file.
   */
  void load_history(const std::string& history_file);

  /*
   * Prints to the log_file (and the console).
   *
   * @param output        The text that should be printed.
   * @param console_print If set to false, then \p output gets printed ONLY to the log_file.
   */
  void out(const std::string& output, bool console_print = true);

  /*
   * Outputs a table, either directly to the _out stream if it is small enough, or using pagination.
   *
   * @param output The output table.
   * @param flags  Flags for the Print operator.
   */
  void out(const std::shared_ptr<const Table>& table, uint32_t flags = 0);

  /*
   * Handler for SIGINT signal (caused by CTRL-C key sequence).
   * Resets the Console state and clears the current line.
   */
  static void handle_signal(int sig);

 protected:
  /*
   * Non-public constructor, since Console is a Singleton.
   */
  Console();

  /*
   * Evaluates given input string. Calls either _eval_command or _eval_sql.
   */
  int _eval(const std::string& input);

  /*
   * Evaluates given Console command.
   */
  int _eval_command(const CommandFunction& func, const std::string& command);

  /*
   * Evaluates given SQL statement using opossum::SqlQueryTranslator.
   */
  int _eval_sql(const std::string& sql);

  // Command functions, registered to be called from the Console
  int _exit(const std::string& args);
  int _help(const std::string& args);
  int _generate_tpcc(const std::string& tablename);
  int _load_table(const std::string& args);
  int _exec_script(const std::string& script_file);
  int _print_table(const std::string& args);
  int _visualize(const std::string& input);
  int _change_runtime_setting(const std::string& input);

  int _begin_transaction(const std::string& input);
  int _rollback_transaction(const std::string& input);
  int _commit_transaction(const std::string& input);
  int _print_transaction_info(const std::string& input);
  int _print_current_working_directory(const std::string& args);

  // Creates the pipelines and returns whether is was successful (true) or not (false)
  bool _initialize_pipeline(const std::string& sql);

  // Checks whether the execution of the SQLPipeline has been rolled back and displays information accordingly.
  // If rollback == true, the explicit context is deleted.
  // Returns true if rollbacked, else false
  bool _handle_rollback();

  // GNU readline interface to our commands
  static char** _command_completion(const char* text, int start, int end);
  static char* _command_generator(const char* text, int state);
  static char* _command_generator_tpcc(const char* text, int state);

  std::string _prompt;
  std::string _multiline_input;
  std::string _history_file;
  RegisteredCommands _commands;
  std::vector<std::string> _tpcc_commands;
  std::ostream _out;
  std::ofstream _log;
  bool _verbose;

  std::unique_ptr<SQLPipeline> _sql_pipeline;
  std::shared_ptr<TransactionContext> _explicitly_created_transaction_context;
  std::shared_ptr<PreparedStatementCache> _prepared_statements;
};

}  // namespace opossum
