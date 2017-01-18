//
// File: wl.cpp
//
//  Description: Add stuff here ...
//  Student Name: Kirthanaa Raghuraman
//  UW Campus ID: 9073422751
//  email: kraghuraman@wisc.edu

#include <iostream>
using namespace std;

int main() {
    string command;

    while(1) {
        cout << ">";
        getline(cin, command);
        cout << "Entered command : " << command <<endl;

        if (command == "end") {
            break;
        } else {
            cout << "ERROR : Invalid command" << endl;
        }
    }
    return 0;
}