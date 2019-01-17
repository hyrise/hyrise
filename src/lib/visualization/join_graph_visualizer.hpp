#pragma once

#include "visualization/abstract_visualizer.hpp"  // NEEDEDINCLUDE

namespace opossum {
class AbstractLQPNode;
class JoinGraph;

class JoinGraphVisualizer : public AbstractVisualizer<std::vector<JoinGraph>> {
 public:
  using AbstractVisualizer<std::vector<JoinGraph>>::AbstractVisualizer;

 protected:
  void _build_graph(const std::vector<JoinGraph>& graphs) override;
  static std::string _create_vertex_description(const std::shared_ptr<AbstractLQPNode>& vertex);
};

}  // namespace opossum
