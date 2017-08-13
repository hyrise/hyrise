#pragma once

#include <string>
#include <functional>
#include <unordered_map>

namespace opossum {

class Console
{
 public:

  using CommandFunction = std::function<int(const std::string &)>;
  using RegisteredCommands = std::unordered_map<std::string, CommandFunction>;

  enum ReturnCode {
    Quit = -1,
    Ok = 0,
    Error = 1
  };

  explicit Console(const std::string & prompt = "> ");

  int read();
  void register_command(const std::string & name, const CommandFunction & f);

  void setPrompt(const std::string & prompt);
  std::string prompt() const;

  static void out(const std::string & output);

 protected:
  int _eval(const std::string & input);
  int _eval_command(const CommandFunction & f, const std::string & command);
  int _eval_sql(const std::string & sql);

  std::string _prompt;
  RegisteredCommands _commands;
};

}  // namespace opossum
