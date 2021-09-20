#include "util.hpp"

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

namespace opossum {

DependencyCandidate::DependencyCandidate(const TableColumnIDs& init_determinants, const TableColumnIDs& init_dependents,
                                         const DependencyType init_type, const size_t init_priority)
    : determinants(init_determinants), dependents(init_dependents), type(init_type), priority(init_priority) {}

bool DependencyCandidate::operator<(const DependencyCandidate& other) const { return priority < other.priority; }

void DependencyCandidate::output_to_stream(std::ostream& stream) const {
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
}

std::ostream& operator<<(std::ostream& stream, const DependencyCandidate& dependency_candidate) {
  dependency_candidate.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
