#!lua

-- Install pre-commit hook for linting if not installed yet or outdated
if os.execute("test -x .git/hooks/pre-commit") ~= 0 or os.execute("md5 .git/hooks/pre-commit") ~= "cb1e4086756ddacb54ec87f775abd82b" then
  os.execute("touch .git/hooks/pre-commit")
  os.execute("echo '#!/bin/sh\necho \"Linting all code, this may take a while...\"\n\nfind src -iname *.cpp -o -iname *.hpp | while read line;\ndo\n    if ! python cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright --linelength=120 $line >/dev/null 2>/dev/null\n    then\n        echo \"ERROR: Linting error occured. Execute \\\"premake4 lint\\\" for details!\n(What kind of ***** would consider a commit without linting?)\"\n        exit 1\n    fi\ndone\n\nif [ $? != 0 ]\nthen\n    exit 1\nfi\n\necho \"Success, no linting errors found!\"\n\necho \"Testing the Opossum, grrrrr...\"\nmake TestOpossum -j >/dev/null 2>/dev/null\nif ! ./build/TestOpossum >/dev/null 2>/dev/null\nthen\n    echo \"ERROR: Testing error occured. Execute \\\"premake4 test\\\" for details!\n(What kind of ***** would consider a commit without testing?)\"\n    exit 1\nfi\n\necho \"Success, no testing errors found!\"' > .git/hooks/pre-commit")
  os.execute("chmod +x .git/hooks/pre-commit")
  os.execute("echo Successfully installed pre-commit hook.")
end

-- TODO try LTO/whole program

if not _OPTIONS["compiler"] then
   _OPTIONS["compiler"] = "gcc"
end

if _OPTIONS["compiler"] == "clang" then
  toolset = "clang"
else
  if os.execute("gcc-6 -v") == 0 then
    premake.gcc.cc  = 'gcc-6'
    premake.gcc.cxx = 'g++-6'
  else
    error("gcc version 6 required. Aborting.")
  end
end

solution "Opossum"
   configurations { "Debug", "Release" }
   platforms "x64"
   flags { "FatalWarnings", "ExtraWarnings" }

project "googletest"
   kind "StaticLib"
   language "C++"
   targetdir "build"
   location "third_party/googletest/googletest/build"

   files { "third_party/googletest/googletest/src/gtest-all.cc" }
   includedirs { "third_party/googletest/googletest", "third_party/googletest/googletest/include" }
   buildoptions { "-g -Wall -Wextra -pthread" }

project "Opossum"
   kind "StaticLib"
   language "C++"
   targetdir "build"

   buildoptions { "-std=c++1z" }

   files { "src/lib/**.hpp", "src/lib/**.cpp" }
   includedirs { "src/lib/", "/usr/local/include" }

   configuration "Debug"
      defines { "DEBUG" }
      flags { "Symbols" }
      prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

   configuration "Release"
      defines { "NDEBUG" }
      flags { "OptimizeSpeed" }
      prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

project "BinOpossum"
   kind "ConsoleApp"
   language "C++"
   targetdir "build"
   location "build"

   buildoptions { "-std=c++1z" }

   links { "Opossum" }
   files { "src/test/test2.cpp" }
   includedirs { "src/lib/", "/usr/local/include" }

   configuration "Debug"
      defines { "DEBUG" }
      flags { "Symbols" }

   configuration "Release"
      defines { "NDEBUG" }
      flags { "OptimizeSpeed" }

project "TestOpossum"
   kind "ConsoleApp"
   language "C++"
   targetdir "build"

   buildoptions { "-std=c++1z" }
   defines { "DEBUG" }
   prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

   links { "googletest", "Opossum" }
   files { "src/test/**.hpp", "src/test/**.cpp" }
   excludes { "src/test/test2.cpp" }

   includedirs { "src/lib/", "/usr/local/include", "third_party/googletest/googletest/include" }


newoption {
   trigger     = "compiler",
   value       = "clang||gcc",
   description = "Choose a compiler",
   allowed = {
      { "gcc",    "gcc of version 6 or higher" },
      { "clang",  "clang llvm frontend" }
   }
}

-- Registering linting and formatting as actions for premake is not optimal, make targets would be the preferable option, but impossible to generate or really hacky

newaction {
   trigger     = "lint",
   description = "Lint the code",
   execute = function ()
      os.execute("find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} python cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright --linelength=120 {}")
   end
}

newaction {
   trigger     = "format",
   description = "Format the code",
   execute = function ()
      os.execute("find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"")
   end
}

newaction {
   trigger     = "test",
   description = "Test the code",
   execute = function ()
      os.execute("make TestOpossum -j && ./build/TestOpossum")
   end
}
