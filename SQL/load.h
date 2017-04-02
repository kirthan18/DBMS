//
// Created by Kirthanaa Raghuraman on 3/28/17.
//

#ifndef SQL_LOAD_H
#define SQL_LOAD_H

#include <sqlite3.h>

// ~D.A. Kline~
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

void DropTable(const char *tableName, sqlite3 *db);

void CreateAllTables(sqlite3 *db);

void CreateTable_Food_Des(sqlite3 *db);
void CreateTable_Nut_Data(sqlite3 *db);
void CreateTable_Weight(sqlite3 *db);
void CreateTable_Footnote(sqlite3 *db);
void CreateTable_Fd_Group(sqlite3 *db);
void CreateTable_Langual(sqlite3 *db);
void CreateTable_Lang_Desc(sqlite3 *db);
void CreateTable_Nutr_Def(sqlite3 *db);
void CreateTable_Src_Cd(sqlite3 *db);
void CreateTable_Deriv_Cd(sqlite3 *db);
void CreateTable_Data_Src_Ln(sqlite3 *db);
void CreateTable_Data_Src(sqlite3 *db);
#endif //SQL_LOAD_H
