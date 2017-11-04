#include "viz_utils.hpp"

namespace opossum {

const std::unordered_map<DotColor, std::string> dot_color_to_string({{DotColor::Black, "black"},
                                                                     {DotColor::White, "white"},
                                                                     {DotColor::Transparent, "transparent"}});
}