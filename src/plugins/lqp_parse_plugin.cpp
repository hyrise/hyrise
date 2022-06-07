#include "lqp_parse_plugin.hpp"

#include "magic_enum.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "storage/table.hpp"

namespace opossum {

std::string LQPParsePlugin::description() const { return "Hyrise LQPParsePlugin"; }

void LQPParsePlugin::start() {
  const auto snapshot = Hyrise::get().default_lqp_cache->snapshot();

  for (const auto& [query, entry] : snapshot) {
    std::cout << "\n" << query << std::endl;
    const auto& root_node = entry.value;

    visit_lqp(root_node, [](auto& node) {
      const auto type = node->type;
      std::cout << "\t" << magic_enum::enum_name(type) << std::endl;
      if (node->type != LQPNodeType::Join) {
        return LQPVisitation::VisitInputs;
      }

      auto& join_node = static_cast<JoinNode&>(*node);
      std::cout << "\t\t" << join_node.description() << std::endl;
      // TO DO

      return LQPVisitation::VisitInputs;
    });
  }
}

void LQPParsePlugin::stop() {}

EXPORT_PLUGIN(LQPParsePlugin)

}  // namespace opossum
