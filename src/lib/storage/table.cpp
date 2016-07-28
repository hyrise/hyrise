#include "table.hpp"

namespace opossum {

table::table(const size_t chunk_size) : _chunk_size(chunk_size) {
	_chunks.push_back(chunk());
}

void table::add_column(std::string &&name, column_type type) {
	_column_names.push_back(name);
	_column_types.push_back(type);
	for(auto &chunk : _chunks) {
		chunk.add_column(type);
	}
}

void table::append(std::initializer_list<all_type_variant> values) {
	if(_chunk_size > 0 && _chunks.back().size() == _chunk_size) {
		_chunks.emplace_back();

		// TODO add columns to new chunk
	}

	_chunks.back().append(values);
}

void table::print(std::ostream &out) const {
	size_t chunk_id = 0;
	for(auto &&chunk : _chunks) {
		std::cout << "=== chunk " << chunk_id << " ===" << std::endl;
		chunk.print(out);
	}
}

}