//
// File: wl.cpp
//
//  Description: Add stuff here ...
//  Student Name: Kirthanaa Raghuraman
//  UW Campus ID: 9073422751
//  email: kraghuraman@wisc.edu

#include <iostream>
#include <vector>
using namespace std;

vector<string> ParseCommand(string command) {
    unsigned int index = 0;
    int first_index = -1;
    int last_index;

    vector<string> command_word_list;

    for (char &c : command) {
        if (c == ' ' || index == command.length()-1) {

            if (first_index != -1) {
                last_index = index;
                command_word_list.push_back(tolower(command.substr(first_index, (last_index - first_index + 1))));
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
int main() {
    string command;

    while(1) {
        cout << ">";
        getline(cin, command);
        cout << "Entered command : " << command <<endl;

        vector<string> parsed_command = ParseCommand(command);
        cout << "Parsed command vector size : " << parsed_command.size() << endl;
        for(int i = 0; i < parsed_command.size(); i++) {
            cout << "Word: " << parsed_command.at(i) << endl;
        }

        if (command == "end") {
            break;
        } else {
            cout << "ERROR : Invalid command" << endl;
        }
    }
    return 0;
}