#include "viz_record_layout.hpp"

#include <iostream>
#include <sstream>

namespace hyrise {

VizRecordLayout& VizRecordLayout::add_label(const std::string& label) {
  content.emplace_back(escape(label));
  return *this;
}

VizRecordLayout& VizRecordLayout::add_sublayout() {
  content.emplace_back(VizRecordLayout{});
  return boost::get<VizRecordLayout>(content.back());
}

std::string VizRecordLayout::to_label_string() const {
  std::stringstream stream;
  stream << "{";

  const auto content_size = content.size();
  for (auto element_idx = size_t{0}; element_idx < content_size; ++element_idx) {
    const auto& element = content[element_idx];

    if (element.type() == typeid(std::string)) {
      stream << boost::get<std::string>(element);
    } else {
      stream << boost::get<VizRecordLayout>(element).to_label_string();
    }

    if (element_idx + 1 < content.size()) {
      stream << " | ";
    }
  }
  stream << "}";
  return stream.str();
}

std::string VizRecordLayout::escape(const std::string& input) {
  std::ostringstream stream;

  for (const auto& character : input) {
    switch (character) {
      case '<':
      case '>':
      case '{':
      case '}':
      case '|':
      case '[':
      case ']':
        stream << "\\";
        break;
      default: {
      }
    }
    stream << character;
  }

  return stream.str();
}

}  // namespace hyrise
