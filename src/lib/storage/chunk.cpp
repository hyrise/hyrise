#include "common.hpp"
#include "chunk.hpp"
#include "raw_attribute_vector.hpp"

#include <iomanip>

namespace opossum {

chunk::chunk() {

}

void chunk::add_column(column_type type) {
	// FIXME replace with boost::hana

	switch(type) {
		case int_type:
			_columns.emplace_back(std::make_shared<raw_attribute_vector<int>>());
			break;
		case float_type:
			_columns.emplace_back(std::make_shared<raw_attribute_vector<float>>());
			break;
		case string_type:
			_columns.emplace_back(std::make_shared<raw_attribute_vector<std::string>>());
			break;
		default:
			std::cout << "keine Lust auf andere Typen" << std::endl;
	}
}

void chunk::append(std::initializer_list<all_type_variant> values) {
	if(_columns.size() != values.size()) {
		throw std::runtime_error("append: number of columns (" + std::to_string(_columns.size()) + ") does not match value list (" + std::to_string(values.size()) + ")");
	}

	auto column_it = _columns.begin();
	auto value_it = values.begin();
	for(; column_it != _columns.end(), value_it != values.end(); column_it++, value_it++) {
		(*column_it)->append(*value_it);
	}
}

void chunk::print(std::ostream &out, std::vector<int> &&column_string_widths) const {
	if(column_string_widths.size() == 0) {} // TODO use/calculate column widths
	for(size_t i = 0; i < size(); ++i) {
		for(auto &&column : _columns) {
			out << std::setw(5) << (*column)[i] << "|" << std::setw(0);			
		}
		out << std::endl;
	}
}

size_t chunk::size() const {
	if(DEBUG && _columns.size() == 0) {
		throw std::runtime_error("Can't calculate size on table without columns");
	}
	return _columns.front()->size();
}

}