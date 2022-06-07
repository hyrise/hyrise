#include "lqp_parse_plugin.hpp"

#include "storage/table.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "hyrise.hpp"

namespace opossum {

std::string LQPParsePlugin::description() const { return "Hyrise LQPParsePlugin"; }

void LQPParsePlugin::start() {
  const auto snapshot = Hyrise::get().default_lqp_cache->snapshot();

  for (const auto& [query, entry] : snapshot) {
    std::cout << query << std::endl;
    const auto root_node = entry.value;

    visit_lqp(root_node, [](auto& node){
      if (node->type != LQPNodeType::Join) {
        return LQPVisitation::VisitInputs;
      }

      std::cout << node->description() << std::endl;
      // auto& join_node = static_cast<JoinNode&>(node);

      return LQPVisitation::VisitInputs;


    });

  }
}

void LQPParsePlugin::stop() { }

EXPORT_PLUGIN(LQPParsePlugin)

}  // namespace opossum
