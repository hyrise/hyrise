#pragma once

#include <sstream>
#include <string>
#include <vector>
#include "benchmark_utilities/random_generator.hpp"

namespace tpch {

class Grammar {
 public:
  explicit Grammar(benchmark_utilities::RandomGenerator generator) : _random_gen(generator) {}

  std::string text(std::streampos min_size) {
    std::stringstream text = sentence();
    while (text.tellp() < min_size) {
      text << " " << sentence().rdbuf();
    }
    return text.str();
  }

 protected:
  std::stringstream sentence() {
    std::stringstream phrase = noun_phrase();
    phrase << " ";
    switch (_random_gen.number(0, 4)) {
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
    phrase << word(terminators);
    return phrase;
  }
  std::stringstream noun_phrase() {
    std::stringstream phrase;
    switch (_random_gen.number(0, 3)) {
      case 0:
        phrase << word(nouns);
        break;
      case 1:
        phrase << word(adjectives) << " " << word(nouns);
        break;
      case 2:
        phrase << word(adjectives) << ", " << word(adjectives) << " " << word(nouns);
        break;
      case 3:
        phrase << word(adverbs) << " " << word(adjectives) << " " << word(nouns);
        break;
    }
    return phrase;
  }
  std::stringstream verb_phrase() {
    std::stringstream phrase;
    switch (_random_gen.number(0, 3)) {
      case 0:
        phrase << word(verbs);
        break;
      case 1:
        phrase << word(auxiliaries) << " " << word(verbs);
        break;
      case 2:
        phrase << word(verbs) << " " << word(adverbs);
        break;
      case 3:
        phrase << word(auxiliaries) << " " << word(verbs) << " " << word(adverbs);
        break;
    }
    return phrase;
  }
  std::stringstream prepositional_phrase() {
    std::stringstream phrase;
    phrase << word(prepositions) << " the " << noun_phrase().rdbuf();
    return phrase;
  }
  std::string word(std::vector<std::string> word_vector) {
    auto i = _random_gen.number(0, word_vector.size() - 1);
    return word_vector[i];
  }

  benchmark_utilities::RandomGenerator _random_gen;

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
};
}  // namespace tpch
