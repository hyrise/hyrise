#include "storage/table.hpp"

int main() {
	opossum::table t;

	t.add_column("a", "int");
	t.add_column("langer spaltenname", "float");
	t.add_column("c", "string");
	t.add_column("d", "double");

	t.append({123, 456.7f, "test", 51});
	t.append({12345, 456.7f, "test", 516.2});
	t.append({12345, 456.7f, "testabc", 62});

	t.print();
}