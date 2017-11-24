#pragma once

#include <sstream>
#include <string>
#include <vector>
#include "benchmark_utilities/random_generator.hpp"

namespace tpch {

/**
 * The specification of the TPCH grammar is reflected by this class,
 * which includes the different rules as protected methods,
 * and generates a text of at least min_size bytes with the function text.
 */
class TpchGrammar {
 public:
  explicit TpchGrammar(benchmark_utilities::RandomGenerator generator);

  std::string random_text(const std::streampos min_size);

  std::string random_word(const std::vector<std::string>& word_vector);

 protected:
  std::stringstream sentence();

  std::stringstream noun_phrase();

  std::stringstream verb_phrase();

  std::stringstream prepositional_phrase();

  benchmark_utilities::RandomGenerator _random_gen;
};

}  // namespace tpch
