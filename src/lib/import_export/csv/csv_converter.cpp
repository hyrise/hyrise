#include "csv_converter.hpp"

#include <string>
#include <utility>

namespace opossum {

void BaseCsvConverter::unescape(std::string& field, const ParseConfig& config) {
  // String does not contain escaping if it is not surrounded with quotes
  if (field.empty() || field.front() != config.quote) return;

  std::string unescaped_string;
  unescaped_string.reserve(field.size());

  // 'escaped' holds the information whether the previous character was the config.escape character
  bool escaped = false;
  // The start and end ranges leave out the surrounding quotes.
  std::copy_if(field.begin() + 1, field.end() - 1, std::back_inserter(unescaped_string),
               [&escaped, &config](const char c) {
                 bool do_copy = true;

                 // If escape character is found the first time, don't copy,
                 // and set 'escaped' to true for the next character
                 if (c == config.escape && !escaped) {
                   do_copy = false;
                   escaped = true;
                 } else {
                   escaped = false;
                 }

                 return do_copy;
               });

  unescaped_string.shrink_to_fit();
  field = std::move(unescaped_string);
}

std::string BaseCsvConverter::unescape_copy(const std::string& field, const ParseConfig& config) {
  auto field_copy = field;
  unescape(field_copy, config);
  return field_copy;
}

}  // namespace opossum
