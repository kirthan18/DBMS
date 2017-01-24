//
// File: wl.cpp
//
//  Description: Add stuff here ...
//  Student Name: Kirthanaa Raghuraman
//  UW Campus ID: 9073422751
//  email: kraghuraman@wisc.edu

#include <iostream>
#include <vector>
#include <fstream>
#include <algorithm>
#include "wl.h"

using namespace std;

Trie::Trie() {
    root = GetTrieNode();
}

Trie::~Trie() {
    //TODO Write code for clearing all nodes recursively
    delete root;
}

TrieNode *Trie::GetRootNode() {
    return root;
}

TrieNode *Trie::GetTrieNode() {
    TrieNode *node = new TrieNode();
    for (int i = 0; i < CHILDREN_SIZE; i++) {
        node->children[i] = NULL;
    }
    return node;
}

void Trie::InsertWord(string word, unsigned long word_index) {
    unsigned long word_length = word.length();
    TrieNode *curr = root;

    for (unsigned int i = 0; i < word_length; i++) {
        int character_index = GetCharacterChildIndex(tolower(word[i]));

        if (character_index != -1) {
            if (curr->children[character_index] == NULL) {
                curr->children[character_index] = GetTrieNode();
            }

            curr = curr->children[character_index];
        }
    }
        curr->is_word_end = true;
        curr->word_index.push_back(word_index);
}

unsigned long Trie::LocateWord(string word, unsigned int occurrence) {
    unsigned long word_index = 0;
    unsigned long word_length = word.length();

    TrieNode *curr = trie->GetRootNode();
    if (curr != NULL) {
        for (int unsigned i = 0; i < word_length; i++) {
            int character_index = GetCharacterChildIndex(tolower(word[i]));

            //TODO - Check this case with the TAs
            //Example - song!
            if (character_index == -1) {
                return 0;
            } else {
                if (curr->children[character_index] != NULL) {
                    curr = curr->children[character_index];
                } else {
                    return 0;
                }
            }
        }
        if (curr->is_word_end) {
            if (!curr->word_index.empty() && curr->word_index.size() >= occurrence) {
                return curr->word_index[occurrence - 1];
            } else {
                return 0;
            }
        } else {
            return 0;
        }
    }

    return word_index;

}


void PrintInvalidCommandError() {
    cout << INVALID_COMMAND << endl;
}

int GetCharacterChildIndex(char c) {
    int index = -1;

    if (isdigit(c)) {
        index = atoi(&c);
    } else if (c == '\'') {
        index = 10;
    } else if (isalpha(c)) {
        index = ((int) c - (int) 'a') + 11;
    }

    return index;
}

vector<string> ParseCommand(string command) {
    unsigned int index = 0;
    int first_index = -1;
    int last_index;

    vector<string> command_word_list;

    for (unsigned int i = 0; i < command.length(); i++) {
        char c = command[i];
        if (c == ' ' || index == command.length() - 1) {
            string word;

            if (first_index == -1) {
                if (index == command.length() - 1 && c != ' ') {
                    first_index = index;
                    last_index = index;
                    word = command.substr(first_index, (last_index - first_index + 1));
                    //transform(word.begin(), word.end(), word.begin(), ::tolower);
                    command_word_list.push_back(word);
                    first_index = -1;
                }
            } else {
                last_index = index;
                if (index == command.length() - 1 && c != ' ') {
                    word = command.substr(first_index, (last_index - first_index + 1));
                } else {
                    word = command.substr(first_index, (last_index - first_index));
                }
                //transform(word.begin(), word.end(), word.begin(), ::tolower);
                command_word_list.push_back(word);
                first_index = -1;
            }
        } else {
            if (first_index == -1) {
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
    transform(command.begin(), command.end(), command.begin(), ::tolower);

    if (command == "new") {

        if (parsed_command.size() != 1) {
            PrintInvalidCommandError();
            return;
        }
        trie = new Trie();
        //cout << "Cleared memory." << endl;

    } else if (command == "end") {

        if (parsed_command.size() != 1) {
            PrintInvalidCommandError();
        }

        delete trie;
        exit(1);

    } else if (command == "locate") {
        string word_to_locate;
        int occurence_to_locate;

        if (parsed_command.size() != 3 || trie == NULL) {
            PrintInvalidCommandError();
            return;
        }

        for (unsigned int k = 0; k < parsed_command.at(2).length(); k++) {
            char c = parsed_command.at(2)[k];
            if (!isdigit(c)) {
                PrintInvalidCommandError();
                return;
            }
        }
        occurence_to_locate = atoi(parsed_command.at(2).c_str());

        if (occurence_to_locate < 1) {
            PrintInvalidCommandError();
            return;
        }

        word_to_locate = parsed_command.at(1);
        transform(word_to_locate.begin(), word_to_locate.end(), word_to_locate.begin(), ::tolower);

        //cout << "Occurence to locate : " << occurence_to_locate << endl;
        //cout << "Word to locate : " << word_to_locate << endl;

        unsigned long word_index = trie->LocateWord(word_to_locate, occurence_to_locate);

        if (word_index <= 0) {
            cout << NO_MATCHING_ENTRY << endl;
        } else {
            cout << word_index << endl;
        }

    } else if (command == "load") {
        string file;
        unsigned long word_index;
        ifstream input;

        if (parsed_command.size() != 2) {
            PrintInvalidCommandError();
            return;
        }

        file = parsed_command.at(1);

        //cout << "File to load : " << file << endl;
        input.open(file.c_str());
        if (input.is_open()) {
            trie = new Trie();
            string word;
            if (input.is_open()) {
                word_index = 1;
                while (!input.eof()) {
                    input >> word;
                    trie->InsertWord(word, word_index);
                    word_index++;
                    //cout << "Word read : " << word << endl;
                }
            }
        } else {
            //TODO Remove this while submission
            cout << "Couldn't open file!" << endl;
            // PrintInvalidCommandError();
            return;
        }

    } else {
        PrintInvalidCommandError();
        return;
    }
}


int main() {
    string command;

    while (1) {
        cout << ">";
        getline(cin, command);
        //cout << "Entered command : " << command << endl;

        vector<string> parsed_command = ParseCommand(command);
        //cout << "Parsed command vector size : " << parsed_command.size() << endl;
        /* for (unsigned int i = 0; i < parsed_command.size(); i++) {
            cout << "Word: " << parsed_command.at(i) << "\tLength : " << parsed_command.at(i).length() << endl;
        } */
        ValidateAndExecuteCommand(parsed_command);
    }
}
