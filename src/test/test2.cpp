#include "storage/table.hpp"
#include "storage/storage_manager.hpp"

int main() {
	std::unique_ptr<opossum::table> t(new opossum::table(2));
	// auto t = std::make_unique(opossum::table(2));

	t->add_column("a", opossum::column_type::int_type);
	t->add_column("langer spaltenname", opossum::column_type::float_type);
	t->add_column("c", opossum::column_type::string_type);
	t->add_column("d", opossum::column_type::double_type);

	t->append({123, 456.7, "test", 51});
	t->append({12345, 456.7, "test", 516.2});
	t->append({12345, 456.7, "testabc", 62});

	t->print();

	opossum::storage_manager s;

	s.add_table("tab", std::move(t));
}