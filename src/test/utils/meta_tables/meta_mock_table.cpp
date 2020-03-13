#include "meta_mock_table.hpp"

namespace opossum {

MetaMockTable::MetaMockTable()
    : AbstractMetaTable(TableColumnDefinitions{{"mock", DataType::String, false}}),
      _insert_calls(0),
      _remove_calls(0),
      _update_calls(0) {}

const std::string& MetaMockTable::name() const {
  static const auto name = std::string{"mock"};
  return name;
}

bool MetaMockTable::can_insert() const { return true; }

bool MetaMockTable::can_delete() const { return true; }

bool MetaMockTable::can_update() const { return true; }

size_t MetaMockTable::insert_calls() const { return _insert_calls; }

size_t MetaMockTable::remove_calls() const { return _remove_calls; }

size_t MetaMockTable::update_calls() const { return _update_calls; }

const std::vector<AllTypeVariant> MetaMockTable::insert_values() const { return _insert_values; }
const std::vector<AllTypeVariant> MetaMockTable::remove_values() const { return _remove_values; }
const std::vector<AllTypeVariant> MetaMockTable::update_selected_values() const { return _update_selected_values; }
const std::vector<AllTypeVariant> MetaMockTable::update_updated_values() const { return _update_updated_values; }

std::shared_ptr<Table> MetaMockTable::_on_generate() const {
  return std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
}

void MetaMockTable::_on_insert(const std::vector<AllTypeVariant>& values) {
  _insert_calls++;
  _insert_values = values;
}

void MetaMockTable::_on_remove(const std::vector<AllTypeVariant>& values) {
  _remove_calls++;
  _remove_values = values;
}

void MetaMockTable::_on_update(const std::vector<AllTypeVariant>& selected_values,
                               const std::vector<AllTypeVariant>& update_values) {
  _update_calls++;
  _update_selected_values = selected_values;
  _update_updated_values = update_values;
}

}  // namespace opossum
