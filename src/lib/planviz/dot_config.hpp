#pragma once

namespace opossum {

enum class DotColor { Black, White, Transparent };

enum class DotRenderFormat { PNG, SVG };

struct DotConfig {
  DotColor background_color = DotColor::Transparent;
  DotRenderFormat render_format = DotRenderFormat::PNG;
};
}  // namespace opossum
