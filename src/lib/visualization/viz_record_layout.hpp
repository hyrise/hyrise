#pragma once

#include <vector>

#include "boost/variant.hpp"

namespace opossum {

/**
 * Utility for creating graphviz records (http://www.graphviz.org/doc/info/shapes.html#record), which are basic layouted
 * nodes
 */
struct VizRecordLayout {
  VizRecordLayout& add_label(const std::string& label);
  VizRecordLayout& add_sublayout();

  std::string to_label_string() const;

  // "Braces, vertical bars and angle brackets must be escaped with a backslash character"
  // http://www.graphviz.org/doc/info/shapes.html#record
  static std::string escape(const std::string& input);

  std::vector<boost::variant<std::string, VizRecordLayout>> content;
};

}  // namespace opossum
