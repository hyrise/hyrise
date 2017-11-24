#include "tpch_grammar.hpp"

#include <sstream>
#include <string>
#include <vector>
#include "benchmark_utilities/random_generator.hpp"

namespace {
const std::vector<std::string> nouns = {
    "foxes",     "ideas",       "theodolites", "pinto",     "beans",       "instructions", "dependencies", "excuses",
    "platelets", "asymptotes",  "courts",      "dolphins",  "multipliers", "sauternes",    "warthogs",     "frets",
    "dinos",     "attainments", "somas",       "Tiresias'", "patterns",    "forges",       "braids",       "hockey",
    "players",   "frays",       "warhorses",   "dugouts",   "notornis",    "epitaphs",     "pearls",       "tithes",
    "waters",    "orbits",      "gifts",       "sheaves",   "depths",      "sentiments",   "decoys",       "realms",
    "pains",     "grouches",    "escapades"};
const std::vector<std::string> verbs = {
    "sleep",     "wake",     "are",    "cajole", "haggle", "nag",     "use",     "boost",  "affix",   "detect",
    "integrate", "maintain", "nod",    "was",    "lose",   "sublate", "solve",   "thrash", "promise", "engage",
    "hinder",    "print",    "x-ray",  "breach", "eat",    "grow",    "impress", "mold",   "poach",   "serve",
    "run",       "dazzle",   "snooze", "doze",   "unwind", "kindle",  "play",    "hang",   "believe", "doubt"};
const std::vector<std::string> adjectives = {
    "furious", "sly",     "careful", "blithe", "quick", "fluffy",   "slow",      "quiet",    "ruthless",
    "thin",    "close",   "dogged",  "daring", "brave", "stealthy", "permanent", "enticing", "idle",
    "busy",    "regular", "final",   "ironic", "even",  "bold",     "silent"};
const std::vector<std::string> adverbs = {
    "sometimes", "always",    "never",   "furiously",  "slyly",       "carefully",  "blithely",
    "quickly",   "fluffily",  "slowly",  "quietly",    "ruthlessly",  "thinly",     "closely",
    "doggedly",  "daringly",  "bravely", "stealthily", "permanently", "enticingly", "idly",
    "busily",    "regularly", "finally", "ironically", "evenly",      "boldly",     "silently"};
const std::vector<std::string> prepositions = {
    "about",   "above",       "according to", "across",     "after",   "against",    "along",   "alongside of",
    "among",   "around",      "at",           "atop",       "before",  "behind",     "beneath", "beside",
    "besides", "between",     "beyond",       "by",         "despite", "during",     "except",  "for",
    "from",    "in place of", "inside",       "instead of", "into",    "near",       "of",      "on",
    "outside", "over",        "past",         "since",      "through", "throughout", "to",      "toward",
    "under",   "until",       "up",           "upon",       "without", "with",       "within"};
const std::vector<std::string> auxiliaries = {
    "do",           "may",          "might",         "shall",         "will",
    "would",        "can",          "could",         "should",        "ought to",
    "must",         "will have to", "shall have to", "could have to", "should have to",
    "must have to", "need to",      "try to"};
const std::vector<std::string> terminators = {".", ";", ":", "?", "!", "--"};
}  // anonymous namespace

namespace tpch {

TpchGrammar::TpchGrammar(benchmark_utilities::RandomGenerator generator) : _random_gen(generator) {}

std::string TpchGrammar::random_text(const std::streampos min_size) {
  std::stringstream text = sentence();
  while (text.tellp() < min_size) {
    text << " " << sentence().rdbuf();
  }
  return text.str();
}

std::string TpchGrammar::random_word(const std::vector<std::string>& word_vector) {
  auto i = _random_gen.random_number(0, word_vector.size() - 1);
  return word_vector[i];
}

std::stringstream TpchGrammar::sentence() {
  std::stringstream phrase = noun_phrase();
  phrase << " ";
  switch (_random_gen.random_number(0, 4)) {
    case 0:
      phrase << verb_phrase().rdbuf();
      break;
    case 1:
      phrase << verb_phrase().rdbuf() << " " << prepositional_phrase().rdbuf();
      break;
    case 2:
      phrase << verb_phrase().rdbuf() << " " << noun_phrase().rdbuf();
      break;
    case 3:
      phrase << prepositional_phrase().rdbuf() << " " << verb_phrase().rdbuf() << " " << noun_phrase().rdbuf();
      break;
    case 4:
      phrase << prepositional_phrase().rdbuf() << " " << verb_phrase().rdbuf();
      phrase << " " << prepositional_phrase().rdbuf();
      break;
  }
  phrase << random_word(terminators);
  return phrase;
}

std::stringstream TpchGrammar::noun_phrase() {
  std::stringstream phrase;
  switch (_random_gen.random_number(0, 3)) {
    case 0:
      phrase << random_word(nouns);
      break;
    case 1:
      phrase << random_word(adjectives) << " " << random_word(nouns);
      break;
    case 2:
      phrase << random_word(adjectives) << ", " << random_word(adjectives) << " " << random_word(nouns);
      break;
    case 3:
      phrase << random_word(adverbs) << " " << random_word(adjectives) << " " << random_word(nouns);
      break;
  }
  return phrase;
}

std::stringstream TpchGrammar::verb_phrase() {
  std::stringstream phrase;
  switch (_random_gen.random_number(0, 3)) {
    case 0:
      phrase << random_word(verbs);
      break;
    case 1:
      phrase << random_word(auxiliaries) << " " << random_word(verbs);
      break;
    case 2:
      phrase << random_word(verbs) << " " << random_word(adverbs);
      break;
    case 3:
      phrase << random_word(auxiliaries) << " " << random_word(verbs) << " " << random_word(adverbs);
      break;
  }
  return phrase;
}

std::stringstream TpchGrammar::prepositional_phrase() {
  std::stringstream phrase;
  phrase << random_word(prepositions) << " the " << noun_phrase().rdbuf();
  return phrase;
}

}  // namespace tpch
