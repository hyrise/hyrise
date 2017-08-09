
#include <string>
#include <iostream>

namespace opossum {

class SqlRepl
{
 public:
  explicit SqlRepl(const std::string & prompt = "> ", std::istream & in = std::cin, std::ostream & out = std::cout);

  void repl();

 protected:
  std::string _read();
  std::string _eval(const std::string & input);
  void _print(const std::string & result);

  std::string _prompt;
  std::istream _in;
  std::ostream _out;
};

}  // namespace opossum
