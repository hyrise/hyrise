#pragma once

#include "abstract_recoverer.hpp"
#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class TextRecoverer : public Singleton<TextRecoverer>, public AbstractRecoverer {
  friend class Singleton;
 public:
  TextRecoverer(const TextRecoverer&) = delete;
  TextRecoverer& operator=(const TextRecoverer&) = delete;

  // Recovers db from logfiles and returns the number of loaded tables
  uint32_t recover() override;

 private:
  TextRecoverer() {}

  // returns substring until delimiter is found and sets begin to begin of next token: the position after delimiter
  std::string _extract_token_up_to_delimiter(const std::string& line, size_t& begin, const char delimiter);

  // returns substring between begin and end. Updates begin.
  std::string _extract_token(const std::string& line, size_t& begin, const size_t end);

  // returns string value between begin and end.
  // Updates begin, assuming there is a delimiter in between.
  std::string _extract_string_value(std::string& line, size_t& begin, const size_t end, std::ifstream& log_file);

  // returns substring that has its size stated beforehand. Updates begin.
  std::string _extract_next_value_with_preceding_size(std::string& line, size_t& begin, const char delimiter,
                                                      std::ifstream& log_file);
};

}  // namespace opossum
