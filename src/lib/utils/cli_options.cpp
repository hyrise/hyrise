#include "cli_options.hpp"

#include <boost/algorithm/string/join.hpp>

#include "constant_mappings.hpp"
#include "logging/logger.hpp"

namespace opossum {

cxxopts::Options CLIOptions::get_basic_cli_options(const std::string& program_name) {
  cxxopts::Options cli_options{program_name};

  std::vector<std::string> logging_strings;
  logging_strings.reserve(logger_to_string.left.size());
  for (const auto& [implementation, implementation_string] : logger_to_string.left) {
    logging_strings.emplace_back(implementation_string);
  }
  const auto logging_options = boost::algorithm::join(logging_strings, ", ");

  std::vector<std::string> log_format_strings;
  log_format_strings.reserve(log_format_to_string.left.size());
  for (const auto& [format, format_string] : log_format_to_string.left) {
    log_format_strings.emplace_back(format_string);
  }
  const auto log_format_options = boost::algorithm::join(log_format_strings, ", ");

  // clang-format off
  cli_options.add_options()
    ("help", "print this help message")
    // ("c,chunk_size", "ChunkSize, default is 2^32-1", cxxopts::value<ChunkOffset>()->default_value(std::to_string(Chunk::MAX_SIZE))) // NOLINT
    // ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("logger", "Set logging implementation. Options: " + logging_options, cxxopts::value<std::string>()->default_value(logger_to_string.left.at(Logger::Implementation::No))) // NOLINT
    ("log_format", "Set logging format. Options: " + log_format_options, cxxopts::value<std::string>()->default_value(log_format_to_string.left.at(Logger::Format::No))) // NOLINT
    ("data_path", "Set folder for data like logfiles", cxxopts::value<std::string>()->default_value("./data/")); // NOLINT
  // clang-format on

  return cli_options;
}

}  // namespace opossum
