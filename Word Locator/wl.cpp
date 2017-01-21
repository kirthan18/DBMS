//
// File: wl.cpp
//
//  Description: Add stuff here ...
//  Student Name: Kirthanaa Raghuraman
//  UW Campus ID: 9073422751
//  email: kraghuraman@wisc.edu

#include <iostream>
#include <vector>

/*
 * Error message to be displayed whenever an invalid or incorrect command is entered.
 */
#define INVALID_COMMAND "ERROR: Invalid command"

/**
 * Error message to be displayed when a word cannot be located in the file.
 */
#define NO_MATCHING_ENTRY "No matching entry"

using namespace std;


void PrintInvalidCommandError() {
    cout << INVALID_COMMAND << endl;
}

vector<string> ParseCommand(string command) {
    unsigned int index = 0;
    int first_index = -1;
    int last_index;

    vector<string> command_word_list;

    for (char &c : command) {
        if (c == ' ' || index == command.length()-1) {
            string word;

            if (first_index == -1) {
                if (index == command.length() - 1 && c != ' ') {
                    first_index = index;
                    last_index = index;
                    word  = command.substr(first_index, (last_index - first_index + 1));
                    transform(word.begin(), word.end(), word.begin(), ::tolower);
                    command_word_list.push_back(word);
                    first_index = -1;
                }
            } else {
                last_index = index;
                if (index == command.length() - 1 && c != ' ') {
                    word  = command.substr(first_index, (last_index - first_index + 1));
                } else {
                    word = command.substr(first_index, (last_index - first_index));
                }
                transform(word.begin(), word.end(), word.begin(), ::tolower);
                command_word_list.push_back(word);
                first_index = -1;
            }
        } else {
            if(first_index == -1) {
                first_index = index;
            }
        }
        index++;
    }
    return command_word_list;
}

void ValidateAndExecuteCommand(vector<string> parsed_command) {
    string command;

    if (parsed_command.empty()) {
        PrintInvalidCommandError();
        return;
    }

    command = parsed_command.at(0);

    if(command == "new") {

        if (parsed_command.size() != 1) {
            PrintInvalidCommandError();
            return;
        }

        cout << "Cleared memory." << endl;

    } else if (command == "end") {

        if (parsed_command.size() != 1) {
            PrintInvalidCommandError();
        }

        exit(1);

    } else if (command == "locate") {
        string word_to_locate;
        int occurence_to_locate;

        if (parsed_command.size() != 3) {
            PrintInvalidCommandError();
            return;
        }

        for (char &c : parsed_command.at(2)) {
            if (!isdigit(c)) {
                PrintInvalidCommandError();
                return;
            }
        }
        occurence_to_locate = atoi(parsed_command.at(2).c_str());
        word_to_locate = parsed_command.at(1);

        cout << "Occurence to locate : " << occurence_to_locate << endl;
        cout << "Word to locate : " << word_to_locate << endl;

    } else if (command == "load") {
        string file;

        if (parsed_command.size() != 2) {
            PrintInvalidCommandError();
            return;
        }

        file = parsed_command.at(1);

        cout << "File to load : " << file << endl;

    } else {
        PrintInvalidCommandError();
        return;
    }
}


int main() {
    string command;

    while(1) {
        cout << ">";
        getline(cin, command);
        cout << "Entered command : " << command <<endl;

        vector<string> parsed_command = ParseCommand(command);
        cout << "Parsed command vector size : " << parsed_command.size() << endl;
        for(int i = 0; i < parsed_command.size(); i++) {
            cout << "Word: " << parsed_command.at(i) << "\tLength : " << parsed_command.at(i).length() << endl;
        }
        ValidateAndExecuteCommand(parsed_command);
    }
}