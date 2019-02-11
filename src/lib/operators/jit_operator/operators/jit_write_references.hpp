#pragma once

#include "abstract_jittable_sink.hpp"

namespace opossum {

/* JitWriteReferences must be the last operator in any chain of jit operators.
 * It is responsible for
 * 1) adding column definitions to the output table
 * 2) appending the current row id to the current output chunk
 * 3) creating a new output chunk and adding output chunks to the output table
 */
class JitWriteReferences : public AbstractJittableSink {
 public:
  struct OutputColumn {
    const std::string column_name;
    const ColumnID referenced_column_id;
  };

  std::string description() const final;

  // Is called by the JitOperatorWrapper.
  // Creates an empty output table with appropriate column definitions.
  std::shared_ptr<Table> create_output_table(const Table& in_table) const final;

  // Is called by the JitOperatorWrapper before any tuple is consumed.
  // This is used to initialize the output position list for the first chunk.
  void before_query(Table& out_table, JitRuntimeContext& context) const override;

  // Is called by the JitOperatorWrapper after all tuples of one chunk have been consumed.
  // This is used to create a reference segment for every output column. The segments are added to a new chunk which is
  // added to the output table.
  void after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                   JitRuntimeContext& context) const final;

  // Is called by the jit-aware LQP translator.
  // This is used to define which columns are in the output table.
  // The order in which the columns are added defines the order of the columns in the output table.
  void add_output_column_definition(const std::string& column_name, const ColumnID referenced_column_id);

  const std::vector<OutputColumn>& output_columns() const;

 protected:
  // Add the current row id to the output position list,
  void _consume(JitRuntimeContext& context) const final;

 private:
  std::vector<OutputColumn> _output_columns;
};

}  // namespace opossum
