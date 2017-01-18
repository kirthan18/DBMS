#include <iostream>
#include <string>
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