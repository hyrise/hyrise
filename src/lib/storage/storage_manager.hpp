#pragma once

#include "table.hpp"

#include <map>

namespace opossum {

class storage_manager {
public:
	storage_manager() {}
	storage_manager(storage_manager const&) = delete;
	storage_manager(storage_manager&&) = default;

	void add_table(std::string name, std::unique_ptr<table> tp);
	void print(std::ostream &out = std::cout) const;

protected:
	std::map<std::string, std::unique_ptr<table>> _tables;
};

}