
#include <string>

namespace opossum {

class Console
{
 public:

  enum ReturnCode {
    Quit = -1,
    Ok = 0,
    Error = 1
  };

  explicit Console(const std::string & prompt = "> ");

  int read_line();

  void setPrompt(const std::string & prompt);
  std::string prompt() const;

 protected:
  std::string _prompt;
};

}  // namespace opossum
