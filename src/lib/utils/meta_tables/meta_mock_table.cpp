#include "meta_mock_table.hpp"

namespace opossum {

MetaMockTable::MetaMockTable()
    : AbstractMetaTable(),
      _column_definitions(TableColumnDefinitions{{"mock", DataType::String, false}}),
      _insert_calls(0),
      _remove_calls(0),
      _update_calls(0) {}

const std::string& MetaMockTable::name() const {
  static const auto name = std::string{"mock"};
  return name;
}

bool MetaMockTable::can_insert() { return true; }

bool MetaMockTable::can_remove() { return true; }

bool MetaMockTable::can_update() { return true; }

size_t MetaMockTable::insert_calls() const { return _insert_calls; }

size_t MetaMockTable::remove_calls() const { return _remove_calls; }
size_t MetaMockTable::update_calls() const { return _insert_calls; }

std::shared_ptr<Table> MetaMockTable::_on_generate() const {
  return std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
}

void MetaMockTable::_insert(const std::vector<AllTypeVariant>& values) { _insert_calls++; }

void MetaMockTable::_remove(const AllTypeVariant& key) { _remove_calls++; }

void MetaMockTable::_update(const AllTypeVariant& key, const std::vector<AllTypeVariant>& values) { _update_calls++; }

}  // namespace opossum
