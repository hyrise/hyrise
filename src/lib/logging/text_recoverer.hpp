#pragma once

#include "abstract_recoverer.hpp"
#include "types.hpp"

namespace opossum {

class TextRecoverer : public AbstractRecoverer {
 public:
  TextRecoverer(const TextRecoverer&) = delete;
  TextRecoverer& operator=(const TextRecoverer&) = delete;

  static TextRecoverer& getInstance();

  // Recovers db from logfiles and returns the number of loaded tables
  uint32_t recover() override;

 private:
  TextRecoverer() {}

  // returns substring until delimiter is found and sets begin to begin of next token: the position after delimiter
  std::string _extract_up_to_delimiter(const std::string& line, size_t& begin, const char delimiter);

  // returns substring between begin and end. Updates begin.
  std::string _extract(const std::string& line, size_t& begin, const size_t end);

  // returns string value between begin and end.
  // Updates begin, assuming there is a delimiter inbetween.
  std::string _extract_string_value(std::string& line, size_t& begin, const size_t end, std::ifstream& log_file);

  // returns substring that has its size stated beforehand. Updates begin.
  std::string _extract_next_value_with_preceding_size(std::string& line, size_t& begin, const char delimiter,
                                                      std::ifstream& log_file);
};

}  // namespace opossum
