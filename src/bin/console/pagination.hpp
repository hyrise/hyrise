#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace opossum {

/*
 * Class to display a given stringstream input using pagination in style of the UNIX 'less' command.
 */
class Pagination {
 public:
  explicit Pagination(std::stringstream& input);

  /*
   * Opens an ncurses environment in which the content is printed.
   * User can navigate through the table with the keyboard (ARROW KEYS, PAGE UP/DOWN, etc.),
   * and quit by pressing 'q'.
   */
  void display();

 protected:
  /*
   * Prints a number of lines to fill the current terminal screen.
   *
   * @param first_line Line which should be started from, i.e. which will be the first line on the screen.
   */
  void _print_page(size_t first_line);

  /*
   * Prints the help screen, which shows all available commands.
   * The help screen is displayed on a separate ncurses window. When the help screen is closed (by pressing 'q'),
   * the separate window gets destroyed, and the previous contend is restored, showing the table as before.
   */
  void _print_help_screen();

  std::vector<std::string> _lines;
  size_t _size_x{0};
  size_t _size_y{0};
};

}  // namespace opossum
