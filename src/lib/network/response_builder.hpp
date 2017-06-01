#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#include <memory>
#include <string>

#include "network/generated/opossum.grpc.pb.h"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "resolve_type.hpp"
#include "type_cast.hpp"

namespace opossum {

// We build a row based protobuf response table. We iterate one column chunk by chunk, then the next column chunk by
// chunk etc. The column values become processed via a visitor pattern.
class ResponseBuilder {
 public:
  void build_response(proto::Response& response, std::shared_ptr<const opossum::Table> table) {
    // Creates a new protobuf Table and links it to the Response object
    const auto proto_table = response.mutable_response_table();

    // Add all rows in one run so that they already exist when we iterate column-based later
    for (size_t i = 0; i < table->row_count(); ++i) {
      proto_table->add_row();
    }

    // Iterate a column chunk by chunk and apply a typed ColumnVisitor, then the next column etc.
    for (size_t column_index = 0, column_count = table->col_count(); column_index < column_count; ++column_index) {
      const auto& type = table->column_type(column_index);

      // Register column type and name
      proto_table->add_column_type(type);
      proto_table->add_column_name(table->column_name(column_index));

      auto visitor = make_unique_by_column_type<ColumnVisitable, ResponseBuilderVisitor>(type);
      uint32_t row_index = 0u;
      // Visit a specific column chunk by chunk
      for (ChunkID chunk_id = 0; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        if (chunk.size() == 0) {
          continue;
        }
        const auto column = chunk.get_column(column_index);
        auto context = std::make_shared<ResponseContext>(proto_table, row_index);
        column->visit(*visitor, context);
        row_index = context->row_index;
      }
    }
  }

 protected:
  struct ResponseContext : ColumnVisitableContext {
    ResponseContext(proto::Table* t, uint32_t i) : proto_table(t), row_index(i) {}

    proto::Table* proto_table;
    uint32_t row_index;
  };

  template <typename T>
  class ResponseBuilderVisitor : public ColumnVisitable {
   public:
    void handle_value_column(BaseColumn& base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
      auto context = std::static_pointer_cast<ResponseContext>(base_context);
      const auto& column = static_cast<ValueColumn<T>&>(base_column);
      const auto& values = column.values();
      auto row_index = context->row_index;
      auto proto_table = context->proto_table;

      // Put every value of this column into the corresponding protobuf row
      for (const auto& value : values) {
        auto row = proto_table->mutable_row(row_index);
        auto row_value = row->add_value();
        set_row_value(row_value, value);
        row_index++;
      }
      context->row_index = row_index;
    }

    void handle_dictionary_column(BaseColumn& base_column,
                                  std::shared_ptr<ColumnVisitableContext> base_context) override {
      auto context = std::static_pointer_cast<ResponseContext>(base_context);
      const auto& column = static_cast<DictionaryColumn<T>&>(base_column);
      auto row_index = context->row_index;
      auto proto_table = context->proto_table;
      const auto& attribute_vector = *column.attribute_vector();
      const auto dictionary = *column.dictionary();

      for (size_t i = 0; i < attribute_vector.size(); ++i) {
        const auto& value = dictionary[attribute_vector.get(i)];
        auto row = proto_table->mutable_row(row_index);
        auto row_value = row->add_value();
        set_row_value(row_value, value);
        row_index++;
      }
      context->row_index = row_index;
    }

    void handle_reference_column(ReferenceColumn& column,
                                 std::shared_ptr<ColumnVisitableContext> base_context) override {
      auto context = std::static_pointer_cast<ResponseContext>(base_context);
      const auto referenced_table = column.referenced_table();
      auto row_index = context->row_index;
      auto proto_table = context->proto_table;

      for (const auto& row_id : *column.pos_list()) {
        auto chunk_info = referenced_table->locate_row(row_id);
        auto& chunk = referenced_table->get_chunk(chunk_info.first);
        auto& referenced_column = *chunk.get_column(column.referenced_column_id());
        auto& value = referenced_column[chunk_info.second];
        auto row = proto_table->mutable_row(row_index);
        auto row_value = row->add_value();
        set_row_value(row_value, type_cast<T>(value));
        row_index++;
      }
      context->row_index = row_index;
    }

   protected:
    void set_row_value(proto::Variant* row_value, ::google::protobuf::int32 value) { row_value->set_value_int(value); }
    void set_row_value(proto::Variant* row_value, ::google::protobuf::int64 value) { row_value->set_value_long(value); }
    void set_row_value(proto::Variant* row_value, float value) { row_value->set_value_float(value); }
    void set_row_value(proto::Variant* row_value, double value) { row_value->set_value_double(value); }
    void set_row_value(proto::Variant* row_value, std::string value) { row_value->set_value_string(value); }
  };
};

}  // namespace opossum
