#include "graphviz_tools.hpp"

#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {


void graphviz_call_cmd(GraphvizLayout layout,
                       GraphvizRenderFormat render_format,
                       const std::string& output_prefix) {
  static std::unordered_map<GraphvizLayout, std::string> cmd_by_layout({
                                                                       {GraphvizLayout::Dot, "dot"},
                                                                       {GraphvizLayout::Neato, "neato"},
                                                                       {GraphvizLayout::FDP, "fdp"},
                                                                       {GraphvizLayout::SFDP, "sfdp"},
                                                                       {GraphvizLayout::TwoPi, "twopi"},
                                                                       {GraphvizLayout::Circo, "circo"}
                                                                       });

  const auto& executable = cmd_by_layout.at(layout);

  const auto dot_filename = output_prefix + ".dot";

  std::string format_arg;
  std::string img_filename = output_prefix + ".";
  if (render_format == GraphvizRenderFormat::PNG) {
    img_filename += "png";
    format_arg = "png";
  } else if (render_format == GraphvizRenderFormat::SVG) {
    img_filename += "svg";
    format_arg = "svg";
  } else {
    Fail("Unsupported format");
  }

  auto cmd = executable + " -T" + format_arg + " " + dot_filename + " > " + img_filename;
  auto ret = system(cmd.c_str());

  Assert(ret == 0,
    "Calling graphviz' dot failed. Have you installed graphviz "
    "(apt-get install graphviz / brew install graphviz)?");
  // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
}

std::string graphviz_random_color() {
  // Favor a hand picked list of nice-to-look-at colors over random generation for now.
  static std::vector<std::string> colors({"#00FA9A", "#00BFFF", "#AFEEEE", "#BC8F8F", "#F0E68C", "#FFFF00", "#FFC0CB", "#FF8C00", "#FF00FF"});
  static size_t color_index;

  color_index = (color_index + 1) % colors.size();
  return colors[color_index];
}

}