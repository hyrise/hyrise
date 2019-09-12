
#include "response_builder.hpp"

namespace opossum {

void ResponseBuilder::build_row_description(std::shared_ptr<const Table> table,
                                            std::shared_ptr<PostgresHandler> postgres_handler) {
  // Calculate sum of length of all column names
  uint32_t column_lengths_total = 0;
  for (auto& column_name : table->column_names()) {
    column_lengths_total += column_name.size();
  }

  postgres_handler->set_row_description_header(column_lengths_total, table->column_count());

  for (ColumnID column_id{0u}; column_id < table->column_count(); ++column_id) {
    uint32_t object_id;
    int32_t type_width;

    switch (table->column_data_type(column_id)) {
      case DataType::Int:
        object_id = 23;
        type_width = 4;
        break;
      case DataType::Long:
        object_id = 20;
        type_width = 8;
        break;
      case DataType::Float:
        object_id = 700;
        type_width = 4;
        break;
      case DataType::Double:
        object_id = 701;
        type_width = 8;
        break;
      case DataType::String:
        object_id = 25;
        type_width = -1;
        break;
      default:
        Fail("Bad DataType");
    }

    postgres_handler->send_row_description(table->column_name(column_id), object_id, type_width);
  }
}

std::string ResponseBuilder::build_command_complete_message(const OperatorType root_operator_type,
                                                            const uint64_t row_count) {
  switch (root_operator_type) {
    case OperatorType::Insert: {
      // 0 is ignored OID and 1 inserted row
      return "INSERT 0 1";
    }
    case OperatorType::Update: {
      // We do not return how many rows are affected, because we don't track this
      // information
      return "UPDATE -1";
    }
    case OperatorType::Delete: {
      // We do not return how many rows are affected, because we don't track this
      // information
      return "DELETE -1";
    }
    default:
      // Assuming normal query
      return "SELECT " + std::to_string(row_count);
  }
}

}  // namespace opossum
