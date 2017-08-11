#include "console.hpp"

#include <iostream>
#include "readline/readline.h"

namespace opossum {

Console::Console(const std::string & prompt)
    : _prompt(prompt) {}

int Console::read_line() {
  char* buffer; //Buffer of line entered by user

  buffer = readline(_prompt.c_str());
  std::string input(buffer);
  free(buffer);
  buffer = NULL;

  if ("exit" == input)
  {
    return -1;
  }

  return 0;
}

void Console::setPrompt(const std::string & prompt) {
  _prompt = prompt;
}

std::string Console::prompt() const {
  return _prompt;
}

}  // namespace opossum

int main(int argc, char** argv) {
  using Return = opossum::Console::ReturnCode;

  opossum::Console console("> ");
  int retCode;

  while ((retCode = console.read_line()) != Return::Quit) {
    if (retCode == Return::Ok)
    {
      console.setPrompt("> ");
    } else {
      console.setPrompt("!> ");
    }
  }

  std::cout << "Done" << std::endl;
}
