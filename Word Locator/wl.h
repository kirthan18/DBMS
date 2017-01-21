//
// File: wl.h
//
//  Description: Add stuff here ...
//  Student Name: Kirthanaa Raghuraman
//  UW Campus ID: 9073422751
//  email: kraghuraman@wisc.edu

#ifndef WL
#define WL

#include <vector>
#include <string>

/**
 * Parses the command to remove any extraneous spaces and also converts all strings to lower case.
 * @return A vector of strings in lowercase.
 */
std::vector<std::string> ParseCommand(std::string);

/**
 * Validates each command and prints error messages when necessary.
 */
void ValidateAndExecuteCommand(std::vector<std::string>);

/**
 * Prints error message for invalid commands.
 */
void PrintInvalidCommandError();

#endif
