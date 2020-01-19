# Similar to target_link_libraries, but uses -isystem instead of -I
# https://stackoverflow.com/questions/51816807/in-cmake-how-do-i-make-target-link-libraries-suppress-warnings-from-3rd-party

function(target_link_libraries_system target)
  set(libs ${ARGN})
  foreach(lib ${libs})
    get_target_property(lib_include_dirs ${lib} INTERFACE_INCLUDE_DIRECTORIES)
    target_include_directories(${target} SYSTEM PRIVATE ${lib_include_dirs})
    target_link_libraries(${target} ${lib})
  endforeach(lib)
endfunction(target_link_libraries_system)

# Same as target_link_libraries_system but sets PRIVATE for target_link_libraries.
# I did not want to work my way into the magic of cmake's argument parsing, so this needs to suffice for now.
function(target_link_libraries_system_private target)
  set(libs ${ARGN})
  foreach(lib ${libs})
    get_target_property(lib_include_dirs ${lib} INTERFACE_INCLUDE_DIRECTORIES)
    target_include_directories(${target} SYSTEM PRIVATE ${lib_include_dirs})
    target_link_libraries(${target} PRIVATE ${lib})
  endforeach(lib)
endfunction(target_link_libraries_system_private)
