#include "parse_server_prepared_statement_task.hpp"

#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

void ParseServerPreparedStatementTask::_on_execute() {
  try {
    auto pipeline_statement = SQLPipelineBuilder{_query}.create_pipeline_statement();
    auto sql_translator = SQLTranslator{UseMvcc::Yes};
    const auto prepared_plans = sql_translator.translate_parser_result(*pipeline_statement.get_parsed_sql_statement());
    Assert(prepared_plans.size() == 1u, "Only a single statement allowed in prepared statement");

    auto prepared_plan =
        std::make_unique<PreparedPlan>(prepared_plans[0], sql_translator.parameter_ids_of_value_placeholders());

    _promise.set_value(std::move(prepared_plan));
  } catch (const std::exception&) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
