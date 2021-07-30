#include "util.hpp"

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

#include "hyrise.hpp"

namespace opossum {
TableColumnID::TableColumnID(const std::string& init_table_name, const ColumnID init_column_id)
    : table_name(init_table_name), column_id(init_column_id) {}

std::string TableColumnID::description() const {
  return table_name + "." + Hyrise::get().storage_manager.get_table(table_name)->column_name(column_id);
}

bool TableColumnID::operator==(const TableColumnID& other) const {
  if (this == &other) return true;
  return table_name == other.table_name && column_id == other.column_id;
}

bool TableColumnID::operator!=(const TableColumnID& other) const { return !operator==(other); }

std::string TableColumnID::column_name() const {
  return Hyrise::get().storage_manager.get_table(table_name)->column_name(column_id);
}

DependencyCandidate::DependencyCandidate(const std::vector<TableColumnID>& init_determinants,
                                         const std::vector<TableColumnID>& init_dependents,
                                         const DependencyType init_type, const size_t init_priority)
    : determinants(init_determinants), dependents(init_dependents), type(init_type), priority(init_priority) {}

bool DependencyCandidate::operator<(const DependencyCandidate& other) const { return priority < other.priority; }

void DependencyCandidate::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  stream << "Type " << magic_enum::enum_name(type) << ", Priority " << priority << ", Columns ";
  std::vector<std::string> determinants_printable;
  std::for_each(determinants.begin(), determinants.end(), [&determinants_printable](auto& determinant) {
    determinants_printable.push_back(determinant.description());
  });
  stream << boost::algorithm::join(determinants_printable, ", ");
  if (!dependents.empty()) {
    std::vector<std::string> dependents_printable;
    std::for_each(dependents.begin(), dependents.end(), [&dependents_printable](auto& dependent) {
      dependents_printable.push_back(dependent.description());
    });
    stream << " --> " << boost::algorithm::join(dependents_printable, ", ");
  }
  stream << std::endl;
}

std::ostream& operator<<(std::ostream& stream, const DependencyCandidate& dependency_candidate) {
  dependency_candidate.output_to_stream(stream, DescriptionMode::SingleLine);
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const TableColumnID& table_column_id) {
  stream << table_column_id.description();
  return stream;
}

}  // namespace opossum
