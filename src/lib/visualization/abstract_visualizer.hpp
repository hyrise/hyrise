#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>
#include <string>
#include <unordered_map>
#include <utility>

#include "operators/print.hpp"

namespace opossum {

// All graphviz options, e.g. color, shape, format, can be looked up at
// http://www.graphviz.org/doc/info/attrs.html
// We do not want to create constants here because they would be rather restrictive compared to all possible options
// defined by graphviz.
struct GraphvizConfig {
  std::string renderer = "dot";
  std::string format = "png";
};

struct VizGraphInfo {
  std::string bg_color = "black";
  std::string rankdir = "BT";
  std::string ratio = "compress";
};

struct VizVertexInfo {
  uintptr_t id;
  std::string label;
  std::string color = "white";
  std::string font_color = "white";
  std::string shape = "rectangle";
  double pen_width = 1.0;
};

struct VizEdgeInfo {
  std::string label;
  std::string color = "white";
  std::string font_color = "white";
  double pen_width = 1.0;
  std::string dir = "forward";
  std::string style = "solid";
};

template <typename GraphBase>
class AbstractVisualizer {
  //                                  Edge list    Vertex list   Directed graph
  using Graph = boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
                                      // Vertex info Edge info    Graph info
                                      VizVertexInfo, VizEdgeInfo, VizGraphInfo>;

  // No label in a node should be wider than this many characters. If it is longer, line breaks should be added.
  static const uint8_t MAX_LABEL_WIDTH = 50;

 public:
  AbstractVisualizer() : AbstractVisualizer(GraphvizConfig{}, VizGraphInfo{}, VizVertexInfo{}, VizEdgeInfo{}) {}

  AbstractVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                     VizEdgeInfo edge_info)
      : _graphviz_config(std::move(graphviz_config)),
        _graph_info(std::move(graph_info)),
        _default_vertex(std::move(vertex_info)),
        _default_edge(std::move(edge_info)) {
    // Add global Graph properties
    _add_graph_property("rankdir", _graph_info.rankdir);
    _add_graph_property("bgcolor", _graph_info.bg_color);
    _add_graph_property("ratio", _graph_info.ratio);

    // Add vertex properties
    _add_property("node_id", &VizVertexInfo::id);
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
    _add_property("style", &VizEdgeInfo::style);
    _add_property("dir", &VizEdgeInfo::dir);
  }

  void visualize(const GraphBase& graph_base, const std::string& graph_filename, const std::string& img_filename) {
    _build_graph(graph_base);
    std::ofstream file(graph_filename);
    boost::write_graphviz_dp(file, _graph, _properties);

    auto renderer = _graphviz_config.renderer;
    auto format = _graphviz_config.format;

    auto cmd = renderer + " -T" + format + " \"" + graph_filename + "\" > \"" + img_filename + "\"";
    auto ret = system(cmd.c_str());

    Assert(ret == 0, "Calling graphviz' " + renderer +
                         " failed. Have you installed graphviz "
                         "(apt-get install graphviz / brew install graphviz)?");
    // We do not want to make graphviz a requirement for Hyrise as visualization is just a gimmick
  }

 protected:
  virtual void _build_graph(const GraphBase& graph_base) = 0;

  template <typename T>
  static uintptr_t _get_id(const T& v) {
    return reinterpret_cast<uintptr_t>(&v);
  }

  template <typename T>
  static uintptr_t _get_id(const std::shared_ptr<T>& v) {
    return reinterpret_cast<uintptr_t>(v.get());
  }

  enum class WrapLabel { On, Off };

  template <typename T>
  void _add_vertex(const T& vertex, const std::string& label = "", const WrapLabel wrap_label = WrapLabel::On) {
    VizVertexInfo info = _default_vertex;
    info.id = _get_id(vertex);
    info.label = label;
    _add_vertex(vertex, info, wrap_label);
  }

  template <typename T>
  void _add_vertex(const T& vertex, VizVertexInfo& vertex_info, const WrapLabel wrap_label = WrapLabel::On) {
    auto vertex_id = _get_id(vertex);
    auto inserted = _id_to_position.insert({vertex_id, _id_to_position.size()}).second;
    if (!inserted) {
      // Vertex already exists, do nothing
      return;
    }

    vertex_info.id = vertex_id;
    if (wrap_label == WrapLabel::On) vertex_info.label = _wrap_label(vertex_info.label);
    boost::add_vertex(vertex_info, _graph);
  }

  template <typename T, typename K>
  void _add_edge(const T& from, const K& to) {
    _add_edge(from, to, _default_edge);
  }

  template <typename T, typename K>
  void _add_edge(const T& from, const K& to, const VizEdgeInfo& edge_info) {
    auto from_id = _get_id(from);
    auto to_id = _get_id(to);

    auto from_pos = _id_to_position.at(from_id);
    auto to_pos = _id_to_position.at(to_id);

    boost::add_edge(from_pos, to_pos, edge_info, _graph);
  }

  template <typename T>
  void _add_graph_property(const std::string& property_name, const T& value) {
    // Use this to add a global property to the graph. This results in a config line in the graph file:
    // property_name=value;
    _properties.property(property_name, boost::make_constant_property<Graph*>(value));
  }

  template <typename T>
  void _add_property(const std::string& property_name, const T& value) {
    // Use this to add a property that is read from each vertex/edge (depending on the value). This will result in:
    // <node_id> [..., property_name=value, ...];
    _properties.property(property_name, boost::get(value, _graph));
  }

  std::string _wrap_label(const std::string& label) {
    if (label.length() <= MAX_LABEL_WIDTH) return label;

    // Split by word so we don't break a line in the middle of a word
    std::vector<std::string> label_words;
    boost::split(label_words, label, boost::is_any_of(" "));

    std::stringstream wrapped_label;
    auto current_line_length = 0;

    for (const auto& word : label_words) {
      auto word_length = word.length() + 1;  // include whitespace

      if (current_line_length + word_length > MAX_LABEL_WIDTH) {
        wrapped_label << "\\n";
        current_line_length = 0u;
      }

      wrapped_label << word << ' ';
      current_line_length += word_length;
    }

    return wrapped_label.str();
  }

  std::string _random_color() {
    // Favor a hand picked list of nice-to-look-at colors over random generation for now.
    static std::vector<std::string> colors(
        {"#008A2A", "#005FAF", "#5F7E7E", "#9C2F2F", "#A0666C", "#9F9F00", "#9FC0CB", "#9F4C00", "#AF00AF"});

    _random_color_index = (_random_color_index + 1) % colors.size();
    return colors[_random_color_index];
  }

  Graph _graph;
  std::unordered_map<uintptr_t, uint16_t> _id_to_position;
  boost::dynamic_properties _properties;

  GraphvizConfig _graphviz_config;
  VizGraphInfo _graph_info;
  VizVertexInfo _default_vertex;
  VizEdgeInfo _default_edge;

  // Current index of color in _random_color()
  size_t _random_color_index{0};
};

}  // namespace opossum
