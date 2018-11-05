#include "prepare.hpp"

#include "storage/lqp_prepared_statement.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

Prepare::Prepare(const std::string& name, const std::shared_ptr<LQPPreparedStatement>& prepared_statement):
  AbstractReadOnlyOperator(OperatorType::Prepare), _name(name), _prepared_statement(prepared_statement) {}

const std::string Prepare::name() const {
  return "Prepare";
}

const std::string Prepare::description(DescriptionMode description_mode) const {
  std::stringstream stream;
  stream << name() << "'" << _name << "' (" << reinterpret_cast<const void*>(_prepared_statement->lqp.get()) << ") ";
  stream << "{\n";
  _prepared_statement->print(stream);
  stream << "}\n";

  return stream.str();
}

std::shared_ptr<const Table> Prepare::_on_execute() {
  StorageManager::get().add_prepared_statement(_name, _prepared_statement);
  return nullptr;
}

void Prepare::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) { }

std::shared_ptr<AbstractOperator> Prepare::_on_deep_copy(
const std::shared_ptr<AbstractOperator>& copied_input_left,
const std::shared_ptr<AbstractOperator>& copied_input_right) const {
    return std::make_shared<Prepare>(_name, _prepared_statement->deep_copy());
}

}  // namespace opossum
