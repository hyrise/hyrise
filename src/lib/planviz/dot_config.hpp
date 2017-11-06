#pragma once

#include <string>
#include <unordered_map>

namespace opossum {

enum class GraphvizColor { Black, White, Transparent };

enum class GraphvizRenderFormat { PNG, SVG };

enum class GraphvizLayout { Dot, Neato, FDP, SFDP, TwoPi, Circo };

struct DotConfig {
  GraphvizColor background_color = GraphvizColor::Transparent;
  GraphvizRenderFormat render_format = GraphvizRenderFormat::PNG;
};

extern const std::unordered_map<GraphvizColor, std::string> dot_color_to_string;

}  // namespace opossum
