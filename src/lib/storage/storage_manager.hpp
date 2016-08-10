#pragma once

#include "table.hpp"

#include <map>

namespace opossum {

class storage_manager {	
public:
	static storage_manager &get();

	void add_table(const std::string &name, std::shared_ptr<table> tp);
	std::shared_ptr<table> get_table(const std::string &name) const;
	void print(std::ostream &out = std::cout) const;

protected:
	storage_manager() {}
	storage_manager(storage_manager const&) = delete;
	storage_manager(storage_manager&&) = delete;

	std::map<std::string, std::shared_ptr<table>> _tables;
};

}