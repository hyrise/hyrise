#pragma once

#include "dot_config.hpp"

namespace opossum {

void graphviz_call_cmd(GraphvizLayout layout, GraphvizRenderFormat render_format, const std::string& output_prefix);

std::string graphviz_random_color();

}