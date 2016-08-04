#include <memory>

#include "storage/table.hpp"
#include "storage/storage_manager.hpp"

int main() {
    auto t = std::make_shared<opossum::table>(opossum::table(2));

	t->add_column("a", "int");
	t->add_column("langer spaltenname", "float");
	t->add_column("c", "string");
	t->add_column("d", "double");

	t->append({123, 456.7, "test", 51});
	t->append({12345, 456.7, "test", 516.2});
	t->append({12345, 456.7, "testabc", 62});

	opossum::storage_manager s;

	s.add_table("tab", std::move(t));

	s.print();
}