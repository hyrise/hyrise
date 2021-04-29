#include "functional_dependency_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

std::string FunctionalDependencyPlugin::description() const { return "Check for functional dependencies"; }

void FunctionalDependencyPlugin::start() {
}

void FunctionalDependencyPlugin::stop() {
}

bool FunctionalDependencyPlugin::_check_dependency(const std::shared_ptr<Table>& table, std::vector<ColumnID> determinant, std::vector<ColumnID> dependent) {
    return false;
}

EXPORT_PLUGIN(FunctionalDependencyPlugin)

}  // namespace opossum