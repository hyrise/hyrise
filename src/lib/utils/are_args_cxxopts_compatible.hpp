#pragma once

namespace opossum {

/**
 * Checks whether the program options can be parse by cxxopts. Catches the case where an option is given in its
 * shortened form with an assignment operator, which is not caught by cxxopts.
 * https://github.com/jarro2783/cxxopts/issues/145
 */
bool are_args_cxxopts_compatible(int argc, char const* const* argv);

}  // namespace opossum
