#include "concurrency/transaction_context.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"

namespace opossum {

class AbstractTpccProcedure {
public:
  AbstractTpccProcedure();
  virtual ~AbstractTpccProcedure() = default;

  // TODO Doc
  virtual void execute() = 0;

  virtual std::ostream& print(std::ostream& stream) const = 0;

protected:
  std::shared_ptr<const Table> _execute_sql(std::string sql);

  const std::shared_ptr<TransactionContext> _transaction_context;
};

std::ostream& operator<<(std::ostream& stream, const AbstractTpccProcedure& procedure);

}
