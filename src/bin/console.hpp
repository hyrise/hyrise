#pragma once

#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/table.hpp"

namespace opossum {

/*
 * SQL REPL Console for Opossum, built on GNU readline. https://cnswww.cns.cwru.edu/php/chet/readline/rltop.html
 * Can load TPCC tables via "generate TABLENAME" command, and can execute SQL statements based on
 * opossum::SqlQueryTranslator.
 */
class Console {
 public:
  using CommandFunction = std::function<int(const std::string &)>;
  using RegisteredCommands = std::unordered_map<std::string, CommandFunction>;

  enum ReturnCode { Multiline = -2, Quit = -1, Ok = 0, Error = 1 };

  static Console &get();

  /*
   * Prompts user for one line of input, evaluates the given input, and prints out the result.
   *
   * @returns ReturnCode::Quit if Console should be terminated,
   *          ReturnCode::Error if an error occured,
   *          ReturnCode::Multiline if the last command ended with a '\' and should be continued next line,
   *          ReturnCode::Ok if the input was evaluated/executed correctly.
   */
  int read();

  int execute_script(const std::string &filepath);

  /*
   * Register a custom command which can be called from the console.
   */
  void register_command(const std::string &name, const CommandFunction &f);
  RegisteredCommands commands();

  /*
   * Set prompt which is shown at the beginning of each line.
   */
  void setPrompt(const std::string &prompt);

  /*
   * Set logfile path.
   */
  void setLogfile(const std::string &logfile);

  /*
   * Prints to the log_file (and the console).
   *
   * @param output        The text that should be printed.
   * @param console_print If set to false, then \p output gets printed ONLY to the log_file.
   */
  void out(const std::string &output, bool console_print = true);
  void out(std::shared_ptr<const Table> table);

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
  int _eval(const std::string &input);

  /*
   * Evaluates given Console command.
   */
  int _eval_command(const CommandFunction &func, const std::string &command);

  /*
   * Evaluates given SQL statement using opossum::SqlQueryTranslator
   */
  int _eval_sql(const std::string &sql);

  // Command functions, registered to be called from the Console
  static int exit(const std::string &args);
  static int help(const std::string &args);
  static int generate_tpcc(const std::string &args);
  static int load_table(const std::string &args);
  static int exec_script(const std::string &args);

  // GNU readline interface to our commands
  static char **command_completion(const char *text, int start, int end);
  static char *command_generator(const char *text, int state);
  static char *command_generator_tpcc(const char *text, int state);

  std::string _prompt;
  std::string _multiline_input;
  RegisteredCommands _commands;
  std::vector<std::string> _tpcc_commands;
  std::ostream _out;
  std::ofstream _log;
  bool _verbose;
};

}  // namespace opossum
