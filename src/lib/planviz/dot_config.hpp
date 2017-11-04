#pragma once

#include <string>
#include <unordered_map>

namespace opossum {

enum class DotColor { Black, White, Transparent };

enum class DotRenderFormat { PNG, SVG };

struct DotConfig {
  DotColor background_color = DotColor::Transparent;
  DotRenderFormat render_format = DotRenderFormat::PNG;
};

extern const std::unordered_map<DotColor, std::string> dot_color_to_string;

}  // namespace opossum
