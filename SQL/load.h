//
// Created by Kirthanaa Raghuraman on 3/28/17.
//

#ifndef SQL_LOAD_H
#define SQL_LOAD_H

#include <sqlite3.h>

enum FIELD_TYPE {
    INT, CHAR
};

class Field {
public:
    char fieldName[100];
    FIELD_TYPE fieldType;
    int fieldLength;
    bool isPrimaryKey;
    bool couldBeEmpty;
};

class TableSchema {
public:
    int numberOfFields;
    Field *fields;
};

void DropAllTables(sqlite3 *db);

void DropTable(const char *, sqlite3 *db);

void CreateAllTables(sqlite3 *db);

void CreateTable(const char *, sqlite3 *db);

#endif //SQL_LOAD_H
