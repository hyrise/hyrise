#pragma once

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>
#include <string>
#include <unordered_map>
#include <utility>

#include "operators/print.hpp"
#include "planviz/graphviz_config.hpp"

namespace opossum {

struct GraphvizConfig {
  std::string layout = GraphvizLayout::Dot;
  std::string format = GraphvizRenderFormat::Png;
};

struct VizGraphInfo {
  std::string bg_color = GraphvizColor::Transparent;
  std::string rankdir = "BT";
  float ratio = 0.5f;
};

struct VizVertexInfo {
  std::string label;
  std::string color = GraphvizColor::White;
  std::string font_color = GraphvizColor::White;
  std::string shape = GraphvizShape::Rectangle;
  uint8_t pen_width = 1u;
};

struct VizEdgeInfo {
  std::string label;
  std::string color = GraphvizColor::White;
  std::string font_color = GraphvizColor::White;
  uint8_t pen_width = 1u;
};

template <typename GraphBase>
class AbstractVisualizer {
  //                                  Edge list    Vertex list   Directed graph
  using Graph = boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
                                      // Vertex info Edge info    Graph info
                                      VizVertexInfo, VizEdgeInfo, VizGraphInfo>;

 public:
  AbstractVisualizer() : AbstractVisualizer(GraphvizConfig{}, VizGraphInfo{}, VizVertexInfo{}, VizEdgeInfo{});

  AbstractVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                     VizEdgeInfo edge_info);

  void visualize(const GraphBase& graph_base, const std::string& graph_filename, const std::string& img_filename);

 protected:
  virtual void _build_graph(const GraphBase& graph_base) = 0;

  template <typename T>
  void _add_vertex(const T& vertex, const std::string& label = "");

  template <typename T>
  void _add_vertex(const T& vertex, VizVertexInfo& vertex_info);

  template <typename T, typename K>
  void _add_edge(const T& from, const K& to);

  template <typename T, typename K>
  void _add_edge(const T& from, const K& to, const VizEdgeInfo& edge_info);

  template <typename T>
  void _add_graph_property(const std::string& property_name, const T& value);

  template <typename T>
  void _add_property(const std::string& property_name, const T& value);

  Graph _graph;
  std::unordered_map<uintptr_t, uint16_t> _id_to_position;
  boost::dynamic_properties _properties;

  GraphvizConfig _graphviz_config;
  VizGraphInfo _graph_info;
  VizVertexInfo _default_vertex;
  VizEdgeInfo _default_edge;
};

}  // namespace opossum
