#include "console.hpp"

#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>

#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"

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
  std::string input_trimmed = trim(input);

  RegisteredCommands::iterator it;
  if ((it = _commands.find(input_trimmed.substr(0, input_trimmed.find('(')))) != std::end(_commands)) {
    return _eval_command(it->second, input_trimmed);
  }

  return _eval_sql(input_trimmed);
}

int Console::_eval_command(const CommandFunction & f, const std::string & input) {
  size_t first = input.find('(') + 1;
  size_t last = input.find_last_of(')');

  if (std::string::npos == first)
  {
    return static_cast<int>(f(""));
  }

  std::string arg = input.substr(first, last-first);
  return static_cast<int>(f(arg));
}

int Console::_eval_sql(const std::string & input) {
  return ReturnCode::Ok;
}

// Command functions implementation

int exit(const std::string &) {
  return opossum::Console::ReturnCode::Quit;
}

int load_tpcc(const std::string & tablename) {
  if (tablename.empty() || "ALL" == tablename)
  {
    auto tables = tpcc::TpccTableGenerator().generate_all_tables();
    for (auto& pair : tables) {
      opossum::StorageManager::get().add_table(pair.first, pair.second);
    }
    return opossum::Console::ReturnCode::Ok;
  }

  // auto tpccGenerator = SqlRepl::get_tpcc_generator(tablename);
  // auto table = tpccGenerator.generateTable();
  // opossum::StorageManager::get().add_table(tablename, table);

  return opossum::Console::ReturnCode::Error;
}

}  // namespace opossum

int main(int argc, char** argv) {
  using Return = opossum::Console::ReturnCode;

  opossum::Console console("> ");

  console.register_command("load", opossum::load_tpcc);

  int retCode;
  while ((retCode = console.read()) != Return::Quit) {
    if (retCode == Return::Ok)
    {
      console.setPrompt("> ");
    } else {
      console.setPrompt("!> ");
    }
  }

  std::cout << "Done" << std::endl;
}
