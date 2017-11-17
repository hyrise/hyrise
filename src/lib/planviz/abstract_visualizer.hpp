#pragma once

#include <boost/graph/graphviz.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <utility>
#include <unordered_map>

#include "operators/print.hpp"
#include "planviz/graphviz_config.hpp"

namespace opossum {

struct GraphvizInfo {
  std::string render_format = GraphvizRenderFormat::Png;
  std::string layout = GraphvizLayout::Dot;
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
};

struct VizEdgeInfo {
  std::string label;
  std::string color = GraphvizColor::White;
  std::string font_color = GraphvizColor::White;
  uint8_t pen_width = 1u;
};


class AbstractVisualizer {
  //                                  Edge list    Vertex list   Directed graph
  using Graph = boost::adjacency_list<boost::vecS, boost::vecS,  boost::directedS,
  //                                  Vertex info    Edge info    Graph info
                                      VizVertexInfo, VizEdgeInfo, VizGraphInfo>;

 public:
  AbstractVisualizer() : AbstractVisualizer(GraphvizConfig{}, VizGraphInfo{}, VizVertexInfo{}, VizEdgeInfo{}) {}

  AbstractVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info, VizEdgeInfo edge_info)
    : _graphviz_config(std::move(graphviz_config)), _graph_info(std::move(graph_info)),
      _default_vertex(std::move(vertex_info)), _default_edge(std::move(edge_info)) {
    // Add global Graph properties
    _add_graph_property("rankdir", _graph_info.rankdir);
    _add_graph_property("bgcolor", _graph_info.bg_color);
    _add_graph_property("ratio", _graph_info.ratio);

    // Add vertex properties
    _add_property("node_id", boost::vertex_index);
    _add_property("color", &VizVertexInfo::color);
    _add_property("label", &VizVertexInfo::label);
    _add_property("shape", &VizVertexInfo::shape);
    _add_property("fontcolor", &VizVertexInfo::font_color);

    // Add edge properties
    _add_property("color", &VizEdgeInfo::color);
    _add_property("fontcolor", &VizEdgeInfo::font_color);
    _add_property("label", &VizEdgeInfo::label);
    _add_property("penwidth", &VizEdgeInfo::pen_width);
  }

  template <typename T>
  void build_graph(const T& graph_base) {
    Fail("Need to call from specific implementation!");
  }

  void visualize(const std::string& dot_filename, const std::string& img_filename) {
    std::ofstream file(dot_filename);
    boost::write_graphviz_dp(file, _graph, _properties);

    // Step 2: Generate png from dot file
    auto cmd = std::string("dot -Tpng " + dot_filename + " > ") + img_filename;
    auto ret = system(cmd.c_str());

    Assert(ret == 0,
           "Calling graphviz' dot failed. Have you installed graphviz "
             "(apt-get install graphviz / brew install graphviz)?");
    // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
  }


 protected:
  template <typename T>
  void _add_vertex(const T& vertex) { _add_vertex(vertex, _default_vertex); }

  template <typename T>
  void _add_vertex(const T& vertex, const std::string& label) {
    VizVertexInfo info = _default_vertex;
    info.label = label;
    _add_vertex(vertex, info);
  }

  template <typename T>
  void _add_vertex(const T& vertex, VizVertexInfo& vertex_info) {
    auto vertex_id = reinterpret_cast<uintptr_t>(vertex.get());
    auto inserted = _id_to_position.insert({vertex_id, _id_to_position.size()}).second;
    if (!inserted) {
      // Vertex already exists, do nothing
      return;
    }

    boost::add_vertex(vertex_info, _graph);
  }

  template <typename T, typename K>
  void _add_edge(const T& from, const K& to) {
    _add_edge(from, to, _default_edge);
  }

  template <typename T, typename K>
  void _add_edge(const T& from, const K& to, const VizEdgeInfo& edge_info) {
    auto from_id = reinterpret_cast<uintptr_t>(from.get());
    auto to_id = reinterpret_cast<uintptr_t>(to.get());

    auto from_pos = _id_to_position.at(from_id);
    auto to_pos = _id_to_position.at(to_id);

    boost::add_edge(from_pos, to_pos, edge_info, _graph);
  }

  template <typename T>
  void _add_graph_property(const std::string& property_name, const T& value) {
    _properties.property(property_name, boost::make_constant_property<Graph*>(value));
  }

  template <typename T>
  void _add_property(const std::string& property_name, const T value) {
    _properties.property(property_name, boost::get(value, _graph));
  }


  Graph _graph;
  std::unordered_map<uintptr_t, uint16_t> _id_to_position;
  boost::dynamic_properties _properties;

  GraphvizConfig _graphviz_config;
  VizGraphInfo _graph_info;
  VizVertexInfo _default_vertex;
  VizEdgeInfo _default_edge;
};

}  // namespace opossum


