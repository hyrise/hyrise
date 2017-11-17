#pragma once

#include <string>
#include <unordered_map>

namespace opossum {

// General layout
struct GraphvizLayout {
  static constexpr const char* Dot = "dot";
  static constexpr const char* Neato = "neato";
  static constexpr const char* FDP = "fdp";
  static constexpr const char* SFDP = "sfdp";
  static constexpr const char* TwoPi = "twopi";
  static constexpr const char* Circo = "circo";
};

struct GraphvizColor {
  static constexpr const char* Transparent = "transparent";
  static constexpr const char* White = "white";
  static constexpr const char* Black = "black";
};

struct GraphvizRenderFormat {
  static constexpr const char* Png = "png";
  static constexpr const char* Svg = "svg";
};

// Vertex-specific
struct GraphvizShape {
  static constexpr const char* Rectangle = "rectangle";
  static constexpr const char* Record = "record";
  static constexpr const char* Parallelogram = "parallelogram";
};

}  // namespace opossum
