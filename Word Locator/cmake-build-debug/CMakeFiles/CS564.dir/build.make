# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.6

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator"

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug"

# Include any dependencies generated for this target.
include CMakeFiles/CS564.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/CS564.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/CS564.dir/flags.make

CMakeFiles/CS564.dir/wl.cpp.o: CMakeFiles/CS564.dir/flags.make
CMakeFiles/CS564.dir/wl.cpp.o: ../wl.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir="/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug/CMakeFiles" --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/CS564.dir/wl.cpp.o"
	/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++   $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/CS564.dir/wl.cpp.o -c "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/wl.cpp"

CMakeFiles/CS564.dir/wl.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/CS564.dir/wl.cpp.i"
	/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/wl.cpp" > CMakeFiles/CS564.dir/wl.cpp.i

CMakeFiles/CS564.dir/wl.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/CS564.dir/wl.cpp.s"
	/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/wl.cpp" -o CMakeFiles/CS564.dir/wl.cpp.s

CMakeFiles/CS564.dir/wl.cpp.o.requires:

.PHONY : CMakeFiles/CS564.dir/wl.cpp.o.requires

CMakeFiles/CS564.dir/wl.cpp.o.provides: CMakeFiles/CS564.dir/wl.cpp.o.requires
	$(MAKE) -f CMakeFiles/CS564.dir/build.make CMakeFiles/CS564.dir/wl.cpp.o.provides.build
.PHONY : CMakeFiles/CS564.dir/wl.cpp.o.provides

CMakeFiles/CS564.dir/wl.cpp.o.provides.build: CMakeFiles/CS564.dir/wl.cpp.o


# Object files for target CS564
CS564_OBJECTS = \
"CMakeFiles/CS564.dir/wl.cpp.o"

# External object files for target CS564
CS564_EXTERNAL_OBJECTS =

CS564: CMakeFiles/CS564.dir/wl.cpp.o
CS564: CMakeFiles/CS564.dir/build.make
CS564: CMakeFiles/CS564.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir="/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug/CMakeFiles" --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable CS564"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/CS564.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/CS564.dir/build: CS564

.PHONY : CMakeFiles/CS564.dir/build

CMakeFiles/CS564.dir/requires: CMakeFiles/CS564.dir/wl.cpp.o.requires

.PHONY : CMakeFiles/CS564.dir/requires

CMakeFiles/CS564.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/CS564.dir/cmake_clean.cmake
.PHONY : CMakeFiles/CS564.dir/clean

CMakeFiles/CS564.dir/depend:
	cd "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug" && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator" "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator" "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug" "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug" "/Users/kirthanaaraghuraman/Documents/CS564/DBMS/Word Locator/cmake-build-debug/CMakeFiles/CS564.dir/DependInfo.cmake" --color=$(COLOR)
.PHONY : CMakeFiles/CS564.dir/depend

