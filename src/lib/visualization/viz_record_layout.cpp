#include "viz_record_layout.hpp"

#include <sstream>

namespace opossum {

VizRecordLayout& VizRecordLayout::add_label(const std::string& label) {
  content.emplace_back(label);
  return *this;
}

VizRecordLayout& VizRecordLayout::add_sublayout() {
  content.emplace_back(VizRecordLayout{});
  return boost::get<VizRecordLayout>(content.back());
}

std::string VizRecordLayout::to_label_string() const {
  std::stringstream stream;
  stream << "{";

  for (size_t element_idx{0}; element_idx < content.size(); ++element_idx) {
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

}  // namespace opossum
