#include "dot_config.hpp"

#include <string>
#include <unordered_map>

namespace opossum {

const std::unordered_map<GraphvizColor, std::string> dot_color_to_string({{GraphvizColor::Black, "black"},
                                                                     {GraphvizColor::White, "white"},
                                                                     {GraphvizColor::Transparent, "transparent"}});
}  // namespace opossum
