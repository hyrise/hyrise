#include "are_args_cxxopts_compatible.hpp"

#include <regex>

namespace opossum {

bool are_args_cxxopts_compatible(int argc, const char* const* argv) {
  std::basic_regex<char> option_matcher("-[[:alnum:]]=.*");

  for (auto arg_idx = 1; arg_idx < argc; ++arg_idx) {
    if (std::regex_match(argv[arg_idx], option_matcher)) return false;
  }

  return true;
}

}  // namespace opossum
