#pragma once

#include "abstract_jittable_sink.hpp"

namespace opossum {

/* JitWriteReference must be the last operator in any chain of jit operators.
 * It is responsible for
 * 1) adding column definitions to the output table
 * 2) appending the current row id to the current output chunk
 * 3) creating a new output chunk and adding output chunks to the output table
 */
class JitWriteReference : public AbstractJittableSink {
 public:
  struct OutputColumn {
    const std::string column_name;
    const ColumnID referenced_column_id;
  };

  std::string description() const final;

  std::shared_ptr<Table> create_output_table(const Table& in_table) const final;
  void before_query(Table& out_table, JitRuntimeContext& context) const override;
  void after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                   JitRuntimeContext& context) const final;

  void add_output_column(const std::string& column_name, const ColumnID referenced_column_id);

  const std::vector<OutputColumn>& output_columns() const;

 protected:
  void _consume(JitRuntimeContext& context) const final;

 private:
  std::vector<OutputColumn> _output_columns;
};

}  // namespace opossum
