#include <string>
#include <utility>

#include "planviz/abstract_visualizer.hpp"

namespace opossum {

AbstractVisualizer::AbstractVisualizer()
    : AbstractVisualizer(GraphvizConfig{}, VizGraphInfo{}, VizVertexInfo{}, VizEdgeInfo{}) {}

AbstractVisualizer::AbstractVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info,
                                       VizVertexInfo vertex_info, VizEdgeInfo edge_info)
    : _graphviz_config(std::move(graphviz_config)),
      _graph_info(std::move(graph_info)),
      _default_vertex(std::move(vertex_info)),
      _default_edge(std::move(edge_info)) {
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
  _add_property("penwidth", &VizVertexInfo::pen_width);

  // Add edge properties
  _add_property("color", &VizEdgeInfo::color);
  _add_property("fontcolor", &VizEdgeInfo::font_color);
  _add_property("label", &VizEdgeInfo::label);
  _add_property("penwidth", &VizEdgeInfo::pen_width);
}

template <typename GraphBase>
void AbstractVisualizer::visualize(const GraphBase& graph_base, const std::string& graph_filename,
                                   const std::string& img_filename) {
  _build_graph(graph_base);
  std::ofstream file(graph_filename);
  boost::write_graphviz_dp(file, _graph, _properties);

  auto executable = _graphviz_config.layout;
  auto format = _graphviz_config.format;

  auto cmd = executable + " -T" + format + " " + graph_filename + " > " + img_filename;
  auto ret = system(cmd.c_str());

  Assert(ret == 0, "Calling graphviz' " + executable +
                       " failed. Have you installed graphviz "
                       "(apt-get install graphviz / brew install graphviz)?");
  // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
}

template <typename T>
void AbstractVisualizer::_add_vertex(const T& vertex, const std::string& label = "") {
  VizVertexInfo info = _default_vertex;
  info.label = label;
  _add_vertex(vertex, info);
}

template <typename T>
void AbstractVisualizer::_add_vertex(const T& vertex, VizVertexInfo& vertex_info) {
  auto vertex_id = reinterpret_cast<uintptr_t>(vertex.get());
  auto inserted = _id_to_position.insert({vertex_id, _id_to_position.size()}).second;
  if (!inserted) {
    // Vertex already exists, do nothing
    return;
  }

  boost::add_vertex(vertex_info, _graph);
}

template <typename T, typename K>
void AbstractVisualizer::_add_edge(const T& from, const K& to) {
  _add_edge(from, to, _default_edge);
}

template <typename T, typename K>
void AbstractVisualizer::_add_edge(const T& from, const K& to, const VizEdgeInfo& edge_info) {
  auto from_id = reinterpret_cast<uintptr_t>(from.get());
  auto to_id = reinterpret_cast<uintptr_t>(to.get());

  auto from_pos = _id_to_position.at(from_id);
  auto to_pos = _id_to_position.at(to_id);

  boost::add_edge(from_pos, to_pos, edge_info, _graph);
}

template <typename T>
void AbstractVisualizer::_add_graph_property(const std::string& property_name, const T& value) {
  _properties.property(property_name, boost::make_constant_property<Graph*>(value));
}

template <typename T>
void AbstractVisualizer::_add_property(const std::string& property_name, const T& value) {
  _properties.property(property_name, boost::get(value, _graph));
}

}  // namespace opossum
