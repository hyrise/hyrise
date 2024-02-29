#include "pagination.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include "ncurses.h"

constexpr auto CURSES_CTRL_C = static_cast<uint32_t>('c') & uint32_t{31};

namespace hyrise {

Pagination::Pagination(std::stringstream& input) {
  auto line = std::string{};
  while (std::getline(input, line, '\n')) {
    _lines.push_back(line);
    _max_width = std::max(_max_width, line.length());
  }
}

void Pagination::display() {
  // Init curses
  initscr();
  clear();
  noecho();
  keypad(stdscr, TRUE);
  curs_set(0);
  // The time (in ms) that getch() waits for input. Having a timeout is important for catching a forwarded CTRL-C.
  timeout(1000);

  getmaxyx(stdscr, _size_y, _size_x);

  // Last line on the screen should show instructions
  --_size_y;

  auto line_count = _lines.size();
  auto end_line = line_count > _size_y ? line_count - _size_y : 0;
  auto current_line = size_t{0};
  auto current_column = size_t{0};

  // Indicator if the display should be reprinted after a keyboard input
  auto reprint = false;
  _print_page(current_line, current_column);

  auto key_pressed = int{};
  while ((key_pressed = getch()) != 'q' && key_pressed != CURSES_CTRL_C) {
    switch (key_pressed) {
      case 'j':
      case KEY_DOWN: {
        if ((current_line + _size_y) < line_count) {
          ++current_line;
          reprint = true;
        }
        break;
      }

      case 'k':
      case KEY_UP: {
        if (current_line > 0) {
          --current_line;
          reprint = true;
        }
        break;
      }

      case ' ':
      case KEY_NPAGE: {
        auto new_line = current_line + _size_y;
        while ((new_line + _size_y) > line_count && new_line > 0) {
          --new_line;
        }

        if (current_line != new_line) {
          current_line = new_line;
          reprint = true;
        }
        break;
      }

      case 'b':
      case KEY_PPAGE: {
        if (current_line >= _size_y) {
          current_line -= _size_y;
          reprint = true;
        } else if (current_line < _size_y && current_line > 0) {
          current_line = 0;
          reprint = true;
        }
        break;
      }

      case 'g':
      case '<':
      case KEY_HOME: {
        if (current_line != 0) {
          current_line = 0;
          reprint = true;
        }
        break;
      }

      case 'G':
      case '>':
      case KEY_END: {
        if (current_line != end_line) {
          current_line = end_line;
          reprint = true;
        }
        break;
      }

      case KEY_RIGHT: {
        current_column += _step_size_x;
        reprint = true;
        break;
      }

      case KEY_LEFT: {
        if (current_column >= _step_size_x) {
          current_column -= _step_size_x;
          reprint = true;
        } else if (current_column != 0) {
          current_column = 0;
          reprint = true;
        }
        break;
      }

      case 'a': {
        if (current_column != 0) {
          current_column = 0;
          reprint = true;
        }
        break;
      }

      case 'e': {
        if (_max_width > _size_x && current_column < _max_width) {
          current_column = _max_width - _size_x + 1;
          reprint = true;
        }
        break;
      }

      case 'h': {
        _print_help_screen();
        reprint = true;
        break;
      }
      default:
        break;
    }

    if (reprint) {
      _print_page(current_line, current_column);
      reprint = false;
    }
  }

  endwin();
}

void Pagination::_print_page(size_t first_line, size_t first_column) {
  clear();

  for (auto i = first_line; i < first_line + _size_y; ++i) {
    if (i >= _lines.size()) {
      break;
    }

    if (_lines[i].length() > first_column) {
      // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
      printw("%s\n", _lines[i].substr(first_column, _size_x - 1).c_str());
    } else {
      printw("\n");
    }
  }

  printw("Press 'q' to quit. ARROW KEYS, PAGE UP/DOWN, for navigation. 'h' for list of all commands.\n");

  refresh();
}

void Pagination::_print_help_screen() {
  auto* help_screen = newwin(0, 0, 0, 0);

  wclear(help_screen);

  wprintw(help_screen, "\n\n");
  wprintw(help_screen, "  Available commands:\n\n");
  wprintw(help_screen, "  %-17s- Move down one line.\n", "ARROW DOWN, j");
  wprintw(help_screen, "  %-17s- Move up one line.\n\n", "ARROW UP, k");
  wprintw(help_screen, "  %-17s- Move right one column.\n", "ARROW RIGHT");
  wprintw(help_screen, "  %-17s- Move left one column.\n\n", "ARROW LEFT");
  wprintw(help_screen, "  %-17s- Move down one page.\n", "PAGE DOWN, SPACE");
  wprintw(help_screen, "  %-17s- Move up one page.\n\n", "PAGE UP, b");
  wprintw(help_screen, "  %-17s- Go to first line.\n", "HOME, g, <");
  wprintw(help_screen, "  %-17s- Go to last line.\n\n", "END, G, >");
  wprintw(help_screen, "  %-17s- Go to first column.\n", "a");
  wprintw(help_screen, "  %-17s- Go to last column.\n\n", "e");
  wprintw(help_screen, "  %-17s- Quit.\n", "q");
  // NOLINTEND(cppcoreguidelines-pro-type-vararg,hicpp-vararg)

  wrefresh(help_screen);

  auto key_pressed = int{};
  while ((key_pressed = getch()) != 'q' && key_pressed != CURSES_CTRL_C) {}

  delwin(help_screen);
}

void Pagination::push_ctrl_c() {
  ungetch(CURSES_CTRL_C);
}

}  // namespace hyrise
