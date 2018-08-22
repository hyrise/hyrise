#include <cxxopts.hpp>

namespace opossum {

class CLIOptions {
 public:
  static cxxopts::Options get_basic_cli_options(const std::string& program_name);
};

}  // namespace opossum
