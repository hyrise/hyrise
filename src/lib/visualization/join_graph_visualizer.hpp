#pragma once

#include <memory>

#include "optimizer/join_ordering/join_graph.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace opossum {

class JoinGraphVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<JoinGraph>>> {
 public:
  using AbstractVisualizer<std::vector<std::shared_ptr<JoinGraph>>>::AbstractVisualizer;

 protected:
  void _build_graph(const std::vector<std::shared_ptr<JoinGraph>>& graphs) override;
};

}  // namespace opossum
