#include "cache/parameterized_plan_cache_handler.hpp"
#include "expression/expression_utils.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

ParameterizedPlanCacheHandler::ParameterizedPlanCacheHandler(const std::shared_ptr<SQLLogicalPlanCache>& lqp_cache,
                                                             const std::shared_ptr<AbstractLQPNode>& unoptimized_lqp,
                                                             std::chrono::nanoseconds& cache_duration,
                                                             const UseMvcc use_mvcc)
    : _lqp_cache(lqp_cache), _cache_duration(cache_duration), _use_mvcc(use_mvcc) {
  const auto start_cache_init = std::chrono::high_resolution_clock::now();
  if (_lqp_cache->use_parameterized_cache == ParameterizedLQPCache::Yes) {
    std::tie(_cache_key, _extracted_values) = _split_lqp_values(unoptimized_lqp);
  } else {
    _cache_key = unoptimized_lqp->deep_copy();
  }

  const auto end_cache_init = std::chrono::high_resolution_clock::now();
  _cache_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end_cache_init - start_cache_init);
}

std::optional<std::shared_ptr<AbstractLQPNode>> ParameterizedPlanCacheHandler::try_get() {
  const auto start_try_get = std::chrono::high_resolution_clock::now();
  if (const auto cached_plan_optional = _lqp_cache->try_get(_cache_key)) {
    // Cache hit
    const auto& cached_plan = cached_plan_optional.value();
    DebugAssert(cached_plan->lqp, "Optimized logical query plan retrieved from cache is empty.");

    // MVCC-enabled and MVCC-disabled LQPs will evict each other
    if (lqp_is_validated(cached_plan->lqp) == (_use_mvcc == UseMvcc::Yes)) {
      // Copy the LQP for reuse as the LQPTranslator might modify mutable fields (e.g., cached column_expressions)
      // and concurrent translations might conflict
      const auto end_try_get = std::chrono::high_resolution_clock::now();
      _cache_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end_try_get - start_try_get);
      if (_lqp_cache->use_parameterized_cache == ParameterizedLQPCache::Yes) {
        return cached_plan->instantiate(_extracted_values)->deep_copy();
      } else {
        return cached_plan->instantiate({})->deep_copy();
      }
    }
  }

  const auto end_try_get = std::chrono::high_resolution_clock::now();
  _cache_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end_try_get - start_try_get);

  return std::nullopt;
}

void ParameterizedPlanCacheHandler::set(std::shared_ptr<AbstractLQPNode>& optimized_lqp) {
  const auto start_set = std::chrono::high_resolution_clock::now();

  if (_lqp_cache->use_parameterized_cache == ParameterizedLQPCache::Yes) {
    const auto cache_value = std::get<0>(_split_lqp_values(optimized_lqp));

    // Convert value expression IDs into ParameterIDs in the right order
    std::vector<ParameterID> parameter_ids(_extracted_values.size());
    auto parameter_ids_it = parameter_ids.begin();
    for (const auto& extracted_value : _extracted_values) {
      const auto extracted_value_expression = std::dynamic_pointer_cast<ValueExpression>(extracted_value);
      *parameter_ids_it = static_cast<ParameterID>(*extracted_value_expression->value_expression_id);
      ++parameter_ids_it;
    }

    auto parameterized_plan = std::make_shared<ParameterizedPlan>(cache_value, parameter_ids);

    _lqp_cache->set(_cache_key, parameterized_plan);
  } else {
    _lqp_cache->set(_cache_key, std::make_shared<ParameterizedPlan>(optimized_lqp, std::vector<ParameterID>{}));
  }

  const auto end_set = std::chrono::high_resolution_clock::now();
  _cache_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end_set - start_set);
}

std::tuple<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<AbstractExpression>>>
ParameterizedPlanCacheHandler::_split_lqp_values(const std::shared_ptr<AbstractLQPNode>& lqp) {
  // Extract all ValueExpressions from the LQP and replace them with PlaceholderExpressions.
  // Copy the optimized plan since the original lqp is still needed for optimization.
  auto lqp_copy = lqp->deep_copy();

  auto lqp_subplans = lqp_find_subplan_roots(lqp_copy);

  auto values = std::vector<std::shared_ptr<AbstractExpression>>();
  for (auto& lqp_subplan : lqp_subplans) {
    visit_lqp(lqp_subplan, [&values](const auto& node) {
      if (node) {
        for (auto& root_expression : node->node_expressions) {
          visit_expression(root_expression, [&values](auto& expression) {
            if (expression->type == ExpressionType::Value) {
              const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression);
              if (value_expression->data_type() != DataType::Null) {
                Assert(expression->arguments.empty(), "Cannot remove arguments of expression as none are present.");
                Assert(value_expression->value_expression_id, "ValueExpression has no ValueExpressionID.");
                values.push_back(expression);
                auto parameter_id = static_cast<ParameterID>(*value_expression->value_expression_id);
                auto new_expression = std::make_shared<PlaceholderExpression>(parameter_id, expression->data_type());
                expression = new_expression;
              }
            }
            return ExpressionVisitation::VisitArguments;
          });
        }
      }
      return LQPVisitation::VisitInputs;
    });
  }

  return std::make_tuple(lqp_copy, values);
}

}  // namespace opossum
