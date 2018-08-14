#pragma once

#include "abstract_recoverer.hpp"
#include "types.hpp"

namespace opossum {

class TextRecoverer : public AbstractRecoverer {
 public:
  TextRecoverer(const TextRecoverer&) = delete;
  TextRecoverer& operator=(const TextRecoverer&) = delete;

  static TextRecoverer& getInstance();

  void recover();

 private:
  TextRecoverer() {}

  std::string _get_substr_and_incr_begin(const std::string line, size_t& begin, const char delimiter);
  std::string _get_substr_and_incr_begin(const std::string line, size_t& begin, const size_t end);
  std::string _get_string_value_and_incr_begin(std::string& line, size_t& begin, const size_t end,
                                               std::ifstream& log_file);
  std::string _get_next_value_with_preceding_size_and_incr_begin(std::string& line, size_t& begin, const char delimiter,
                                                                 std::ifstream& log_file);
};

}  // namespace opossum
