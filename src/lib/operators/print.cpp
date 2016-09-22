#include "print.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace opossum {

Print::Print(const std::shared_ptr<AbstractOperator> in) : AbstractOperator(in) {}

Print::Print(const std::shared_ptr<AbstractOperator> in, std::ostream& out) : AbstractOperator(in), _out(out) {}

const std::string Print::get_name() const { return "Print"; }

uint8_t Print::get_num_in_tables() const { return 1; }

uint8_t Print::get_num_out_tables() const { return 1; }

void Print::execute() {
  auto widths = column_string_widths(8, 20, _input_left);

  _out << "=== Columns" << std::endl;
  for (size_t col = 0; col < _input_left->col_count(); ++col) {
    _out << "length:" << widths[col] << std::endl;
    _out << "|" << std::setw(widths[col]) << _input_left->_column_names[col] << std::setw(0);
  }
  _out << "|" << std::endl;
  for (size_t col = 0; col < _input_left->col_count(); ++col) {
    _out << "|" << std::setw(widths[col]) << " [" << _input_left->_column_types[col] << "]" << std::setw(0);
  }
  _out << "|" << std::endl;

  size_t chunk_id = 0;
  for (auto&& chunk : _input_left->_chunks) {
    _out << "=== Chunk " << chunk_id << " === " << std::endl;
    chunk_id++;

    auto columns = chunk._columns;
    if (chunk.size() == 0) {
      _out << "Empty chunk." << std::endl;
      continue;
    }
    for (size_t row = 0; row < chunk.size(); ++row) {
      _out << "|";
      for (size_t col = 0; col < chunk._columns.size(); ++col) {
        _out << std::setw(widths[col]) << (*columns[col])[row] << "|" << std::setw(0);
      }
      _out << std::endl;
    }
  }
}

std::vector<uint16_t> Print::column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<Table> t) const {
  std::vector<uint16_t> widths(t->col_count());
  for (size_t col = 0; col < t->col_count(); ++col) {
    widths[col] = to_string(t->_column_names[col]).size();
  }
  for (auto&& chunk : t->_chunks) {
    for (size_t col = 0; col < chunk._columns.size(); ++col) {
      auto columns = chunk._columns;
      for (size_t row = 0; row < chunk.size(); ++row) {
        // TODO(unknown): iterating table twice (better only scan 1st&last chunk for max length?)
        auto cell_length = static_cast<uint16_t>(to_string((*columns[col])[row]).size());
        widths[col] = std::min(max, std::max({min, widths[col], cell_length}));
      }
    }
  }
  return widths;
}

std::shared_ptr<Table> Print::get_output() const { return _input_left; }
}  // namespace opossum
