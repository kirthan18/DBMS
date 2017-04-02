#include <iostream>
#include <sqlite3.h>
#include <fstream>
#include <sstream>
#include <vector>

using namespace std;

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
    int i;
    for (i = 0; i < argc; i++) {

        cout << azColName[i] << " = ";
        if (argv[i]) {
            cout << argv[i] << "\n";
        } else {
            cout << "NULL\n";
        }
    }
    printf("\n");
    return 0;
}

void DropTable(const char *tableName, sqlite3 *db) {
    char *errorMessage;
    char dropTableQuery[100] = "DROP TABLE IF EXISTS ";

    strcat(dropTableQuery, tableName);

//    cout << endl << "Deleting table : " << tableName << endl;
//    cout << "Query: " << dropTableQuery << endl;

    int rc = sqlite3_exec(db, dropTableQuery, NULL, 0, &errorMessage);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    } else {
//        fprintf(stdout, "Table dropped successfully\n");
        cout << "Table " << tableName << " dropped successfully." << endl;
    }

}

void DropAllTables(sqlite3 *nutrient_db) {
    cout << "**********************************************" << endl;
    DropTable("DATA_SRC", nutrient_db);
    DropTable("DATSRCLN", nutrient_db);
    DropTable("DERIV_CD", nutrient_db);
    DropTable("FD_GROUP", nutrient_db);
    DropTable("FOOD_DES", nutrient_db);
    DropTable("FOOTNOTE", nutrient_db);
    DropTable("LANGDESC", nutrient_db);
    DropTable("LANGUAL", nutrient_db);
    DropTable("NUT_DATA", nutrient_db);
    DropTable("NUTR_DEF", nutrient_db);
    DropTable("SRC_CD", nutrient_db);
    DropTable("WEIGHT", nutrient_db);
    cout << "**********************************************" << endl;
}

void CreateTable(sqlite3 *db, const char *query, const char* tableName) {
    char *errorMessage;
//    cout << endl << "Creating table : " << tableName << endl;

    /* Execute SQL statement */
    int rc = sqlite3_exec(db, query, callback, 0, &errorMessage);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    } else {
        //fprintf(stdout, "Table created successfully\n");
        cout << "Table " << tableName << " created successfully." << endl;
    }
}

void CreateTable_Food_Des(sqlite3 *db) {
    const char *query = "CREATE TABLE FOOD_DES(                          " \
                        "NDB_No            CHAR(5)     PRIMARY KEY  NOT NULL," \
                        "FdGrp_Cd          CHAR(4)                  NOT NULL," \
                        "Long_Desc         CHAR(200)                NOT NULL," \
                        "Shrt_Desc         CHAR(60)                 NOT NULL," \
                        "ComName           CHAR(100)                        ," \
                        "ManufacName       CHAR(65)                         ," \
                        "Survey            CHAR(1)                          ," \
                        "Ref_desc          CHAR(135)                        ," \
                        "Refuse            REAL                             ," \
                        "SciName           CHAR(65)                         ," \
                        "N_Factor          REAL                             ," \
                        "Pro_Factor        REAL                             ," \
                        "Fat_Factor        REAL                             ," \
                        "CHO_Factor        REAL                              " \
                        ");";
    CreateTable(db, query, "FOOD_DES");
}

void CreateTable_Nut_Data(sqlite3 *db) {
    const char *query = "CREATE TABLE NUT_DATA(              " \
                        "NDB_No         CHAR(5)     NOT NULL," \
                        "Nutr_No        CHAR(3)     NOT NULL," \
                        "Nutr_Val       REAL        NOT NULL," \
                        "Num_Data_Pts   INT         NOT NULL," \
                        "Std_Error      REAL                ," \
                        "Src_Cd         CHAR(2)     NOT NULL," \
                        "Deriv_Cd       CHAR(4)             ," \
                        "Ref_NDB_No     CHAR(5)             ," \
                        "Add_Nutr_Mark  CHAR(1)             ," \
                        "Num_Studies    INT                 ," \
                        "Min            REAL                ," \
                        "Max            REAL                ," \
                        "DF             INT                 ," \
                        "Low_EB         REAL                ," \
                        "Up_EB          REAL                ," \
                        "Stat_cmt       CHAR(10)            ," \
                        "AddMod_Date    CHAR(10)            ," \
                        "CC             CHAR(1)             ," \
                        "PRIMARY KEY (NDB_No, Nutr_No)       "\
                        ");";
    CreateTable(db, query, "NUT_DATA");
}

void CreateTable_Weight(sqlite3 *db) {
    const char *query = "CREATE TABLE WEIGHT("  \
                        "NDB_No         CHAR(5)     NOT NULL," \
                        "Seq            CHAR(2)     NOT NULL," \
                        "Amount         REAL        NOT NULL," \
                        "Msre_Desc      CHAR(84)    NOT NULL," \
                        "Gm_Wgt         REAL        NOT NULL," \
                        "Num_Data_Pts   INT                 ," \
                        "Std_Dev        REAL                ," \
                        "PRIMARY KEY (NDB_No, Seq)"\
                        ");";
    CreateTable(db, query, "WEIGHT");
}


void CreateTable_Footnote(sqlite3 *db) {
    const char *query = "CREATE TABLE FOOTNOTE("  \
                        "NDB_No         CHAR(5)     NOT NULL," \
                        "Footnt_no      CHAR(4)     NOT NULL," \
                        "Footnt_Typ     CHAR(1)     NOT NULL," \
                        "Nutr_No        CHAR(3)             ," \
                        "Footnt_Txt     CHAR(200)   NOT NULL" \
                        ");";
    CreateTable(db, query, "FOOTNOTE");
}


void CreateTable_Fd_Group(sqlite3 *db) {
    const char *query = "CREATE TABLE FD_GROUP("  \
                        "FdGrp_Cd       CHAR(4)     PRIMARY KEY     NOT NULL," \
                        "FdGrp_Desc     CHAR(60)                    NOT NULL " \
                        ");";
    CreateTable(db, query, "FD_GROUP");
}

void CreateTable_Langual(sqlite3 *db) {
    const char *query = "CREATE TABLE LANGUAL("  \
                        "NDB_No         CHAR(5)    NOT NULL," \
                        "Factor_Code    CHAR(5)    NOT NULL," \
                        "PRIMARY KEY (NDB_No, Factor_Code)  " \
                        ");";
    CreateTable(db, query, "LANGUAL");
}

void CreateTable_Lang_Desc(sqlite3 *db) {
    const char *query = "CREATE TABLE LANGDESC ("  \
                        "Factor_Code    CHAR(5)    PRIMARY KEY  NOT NULL," \
                        "Description    CHAR(140)               NOT NULL " \
                        ");";
    CreateTable(db, query, "LANGDESC");
}

void CreateTable_Nutr_Def(sqlite3 *db) {
    const char *query = "CREATE TABLE NUTR_DEF ("  \
                        "Nutr_No        CHAR(3)    PRIMARY KEY  NOT NULL," \
                        "Units          CHAR(7)                 NOT NULL," \
                        "Tagname        CHAR(20)                        ," \
                        "NutrDesc       CHAR(60)                NOT NULL," \
                        "Num_Dec        CHAR(1)                 NOT NULL," \
                        "SR_Order       REAL                    NOT NULL " \
                        ");";
    CreateTable(db, query, "NUTR_DEF");
}


void CreateTable_Src_Cd(sqlite3 *db) {
    const char *query = "CREATE TABLE SRC_CD ("  \
                        "Src_Cd         CHAR(2)    PRIMARY KEY  NOT NULL," \
                        "SrcCd_Desc     CHAR(60)                NOT NULL " \
                        ");";
    CreateTable(db, query, "SRC_CD");
}

void CreateTable_Deriv_Cd(sqlite3 *db) {
    const char *query = "CREATE TABLE DERIV_CD ("  \
                        "Deriv_Cd       CHAR(4)    PRIMARY KEY  NOT NULL," \
                        "Deriv_Desc     CHAR(120)               NOT NULL " \
                        ");";
    CreateTable(db, query, "DERIV_CD");
}

void CreateTable_Data_Src_Ln(sqlite3 *db) {
    const char *query = "CREATE TABLE  DATSRCLN("  \
                        "NDB_No         CHAR(5)     NOT NULL," \
                        "Nutr_No        CHAR(3)     NOT NULL," \
                        "DataSrc_ID     CHAR(6)     NOT NULL," \
                        "PRIMARY KEY (NDB_No, Nutr_No, Datasrc_ID) " \
                        ");";
    CreateTable(db, query, " DATSRCLN");
}


void CreateTable_Data_Src(sqlite3 *db) {
    const char *query = "CREATE TABLE  DATA_SRC("  \
                        "DataSrc_ID     CHAR(6)     PRIMARY KEY     NOT NULL," \
                        "Authors        CHAR(255)                           ," \
                        "Title          CHAR(255)                   NOT NULL," \
                        "Year           CHAR(4)                             ," \
                        "Journal        CHAR(135)                           ," \
                        "Vol_City       CHAR(16)                            ," \
                        "Issue_State    CHAR(5)                             ," \
                        "Start_Page     CHAR(5)                             ," \
                        "End_Page       CHAR(5)                              " \
                        ");";
    CreateTable(db, query, " DATA_SRC");
}


void CreateAllTables(sqlite3 *db) {
    cout << "**********************************************" << endl;
    // Principal Files
    CreateTable_Food_Des(db);
    CreateTable_Nut_Data(db);
    CreateTable_Weight(db);
    CreateTable_Footnote(db);

    // Support files
    CreateTable_Fd_Group(db);
    CreateTable_Langual(db);
    CreateTable_Lang_Desc(db);
    CreateTable_Nutr_Def(db);
    CreateTable_Src_Cd(db);
    CreateTable_Deriv_Cd(db);
    CreateTable_Data_Src_Ln(db);
    CreateTable_Data_Src(db);
    cout << "**********************************************" << endl;
}

void InsertRecord(sqlite3 *db, string query) {
    char *errorMessage;
    int rc = sqlite3_exec(db, query.c_str(), callback, 0, &errorMessage);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
        exit(1);
    }else{
        //printf(stdout, "Record inserted successfully\n");
    }
}

string AppendFieldValues(string query, vector<string> fieldValues) {
    char delim[3] = "~\r";

    for (int i = 0; i < fieldValues.size(); i++) {
        if (fieldValues[i] == "" || fieldValues[i] == "~~" || fieldValues[i] == "\r") {
            query = query + "NULL";
        } else {
            bool isString = fieldValues[i].substr(0,1) == "~";
            for (int j = 0; j < 2; j++) {
                fieldValues[i].erase(std::remove(fieldValues[i].begin(), fieldValues[i].end(),
                                                 delim[j]), fieldValues[i].end());
            }
            if(isString) {
                char *safeString = sqlite3_mprintf("%q",fieldValues[i].c_str());
                query = query + "'" + safeString + "'";
            } else {
                query = query + fieldValues[i];
            }
        }
        if (i != fieldValues.size() - 1) {
            query = query + ",";
        } else {
            query = query + ");";
        }
    }

    return query;
}

vector<string> GetFieldValues(string line) {
    stringstream stream(line);
    string segment;
    vector<string> fieldValueList;

    while(getline(stream, segment, '^'))
    {
        fieldValueList.push_back(segment);
        //cout << segment << endl;
    }
    return fieldValueList;
}

void Parse_Food_Des(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/FOOD_DES.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing FOOD_DES.txt" << endl;

    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);

        if (seglist.size() == 13) {
            seglist.push_back("");
        }
        string query = "INSERT INTO FOOD_DES (NDB_No ,FdGrp_Cd, Long_Desc, Shrt_Desc, ComName, ManufacName, Survey, "
                "Ref_desc, Refuse, SciName, N_Factor, Pro_Factor, Fat_Factor, CHO_Factor) VALUES (";

        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Nut_Data(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/NUT_DATA.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing NUT_DATA.txt" << endl;

    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        if (seglist.size() < 18) {
            int k = 0;
            while (k < 18 - seglist.size()) {
                seglist.push_back("");
                k++;
            }
        }
        string query = "INSERT INTO NUT_DATA (NDB_No, Nutr_No, Nutr_Val, Num_Data_Pts, Std_Error, Src_Cd, Deriv_Cd, "
                "Ref_NDB_No, Add_Nutr_Mark, Num_Studies, Min, Max, DF, Low_EB, Up_EB, Stat_cmt, AddMod_Date, CC) "
                "VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Weight(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/WEIGHT.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing WEIGHT.txt" << endl;

    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        if (seglist.size() == 6) {
            seglist.push_back("");
        }
        string query = "INSERT INTO WEIGHT (NDB_No ,Seq, Amount, Msre_Desc, Gm_Wgt, Num_Data_Pts, Std_Dev) VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Footnote(sqlite3 *db) {

    ifstream infile("sr28asc/FOOTNOTE.txt");
    int numRecords = 0;
    cout << "**********************************************" << endl;
    cout << "Parsing FOOTNOTE.txt" << endl;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO FOOTNOTE (NDB_No, Footnt_No, Footnt_Typ, Nutr_No, Footnt_Txt) VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Data_Src(sqlite3 *db) {
    ifstream infile("sr28asc/DATA_SRC.txt");
    int numRecords = 0;
    /*string line;
    getline(infile, line);
    cout << line << endl;
    stringstream test("~D6401~^~M. Ozcan~^~Determination of the mineral compositions of some selected oil-bearing seeds & kernels using Inductively Coupled Plasma Atomic Emmission Spectrometry (ICP-AES)~^~2006~^~Grasas y Aceites~^~57~^~2~^^");
    string segment;
    vector<string> seglist;

    while(getline(test, segment, '^'))
    {
        seglist.push_back(segment);
        cout << segment << endl;

        if (segment == "") {
            cout << "NULL" << endl;
        }
    }

    cout << seglist.size() << endl;

    if (seglist.size() == 8) {
        seglist.push_back("");
    }
    string query = "INSERT INTO DATA_SRC (DataSrc_ID ,Authors, Title, Year, Journal, Vol_City, Issue_State, "
            "Start_Page, End_Page) VALUES (";

    char delim[3] = "~\r";
    for (int i = 0; i < seglist.size(); i++) {
        if (seglist[i] == "" || seglist[i] == "~~") {
            query = query + "NULL";
        } else {
            bool isString = seglist[i].substr(0,1) == "~";
            for (int j = 0; j < 2; j++) {
                seglist[i].erase(std::remove(seglist[i].begin(), seglist[i].end(), delim[j]), seglist[i].end());
            }
            if(isString) {
                query = query + "'" + seglist[i] + "'";
            }

        }
        if (i != seglist.size() - 1) {
            query = query + ",";
        } else {
            query = query + ");";
        }
    }

    cout << query << endl;

    InsertRecord(db, query);*/
    cout << "**********************************************" << endl;
    cout << "Parsing DATA_SRC.txt" << endl;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);

        if (seglist.size() == 8) {
            seglist.push_back("");
        }
        string query = "INSERT INTO DATA_SRC (DataSrc_ID ,Authors, Title, Year, Journal, Vol_City, Issue_State, "
                "Start_Page, End_Page) VALUES (";
        query = AppendFieldValues(query, seglist);
//        cout << query << endl;
        InsertRecord(db, query);
        numRecords++;
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Data_Src_Ln(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/DATSRCLN.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing DATSRCLN.txt" << endl;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO DATSRCLN (NDB_No ,Nutr_No, DataSrc_ID) VALUES (";

        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        InsertRecord(db, query);
        numRecords++;
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Src_Cd(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/SRC_CD.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing SRC_CD.txt" << endl;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO SRC_CD (Src_Cd ,SrcCd_Desc) VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        InsertRecord(db, query);
        numRecords++;
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Deriv_Cd(sqlite3 *db) {
    int numRecords = 0;
    cout << "**********************************************" << endl;
    cout << "Parsing DERIV_CD.txt" << endl;
    ifstream infile("sr28asc/DERIV_CD.txt");
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO DERIV_CD (Deriv_Cd ,Deriv_Desc) VALUES (";
        query = AppendFieldValues(query, seglist);
//        cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}


void Parse_Fd_Group(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/FD_GROUP.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing FD_GROUP.txt" << endl;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO FD_GROUP (FdGrp_Cd ,FdGrp_Desc) VALUES (";
        query = AppendFieldValues(query, seglist);
        InsertRecord(db, query);
        numRecords++;
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Langual(sqlite3 *db) {
    int numRecords = 0;
    ifstream infile("sr28asc/LANGUAL.txt");
    cout << "**********************************************" << endl;
    cout << "Parsing LANGUAL.txt" << endl;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO LANGUAL (NDB_No ,Factor_Code) VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Lang_Desc(sqlite3 *db) {
    cout << "**********************************************" << endl;
    cout << "Parsing LANGDESC.txt" << endl;
    int numRecords = 0;
    ifstream infile("sr28asc/LANGDESC.txt");
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO LANGDESC (Factor_Code ,Description) VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void Parse_Nutr_Def(sqlite3 *db) {
    ifstream infile("sr28asc/NUTR_DEF.txt");

    cout << "**********************************************" << endl;
    cout << "Parsing NUTR_DEF.txt" << endl;
    int numRecords = 0;
    for (string line; getline(infile, line);) {
        vector<string> seglist = GetFieldValues(line);
        string query = "INSERT INTO NUTR_DEF (Nutr_No ,Units, Tagname, NutrDesc, Num_Dec, SR_Order) VALUES (";
        query = AppendFieldValues(query, seglist);
        //cout << query << endl;
        numRecords++;
        InsertRecord(db, query);
    }
    cout << "Number of records inserted : " << numRecords << endl;
    cout << "**********************************************" << endl;
}

void ParseAllFiles(sqlite3 *db) {
    Parse_Food_Des(db);
//    Parse_Nut_Data(db);
    Parse_Weight(db);
    Parse_Footnote(db);

    Parse_Data_Src(db);
    Parse_Src_Cd(db);
    Parse_Fd_Group(db);
    Parse_Langual(db);
    Parse_Lang_Desc(db);
    Parse_Nutr_Def(db);
    Parse_Deriv_Cd(db);
    Parse_Data_Src_Ln(db);
}


int main() {

    sqlite3 *nutrient_db;
    int rc;
    const char* data = "Callback function called";

    rc = sqlite3_open("nutrient_db.db", &nutrient_db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(nutrient_db));
        return (0);
    } else {
        fprintf(stderr, "Opened database successfully\n");
    }

    DropAllTables(nutrient_db);
    CreateAllTables(nutrient_db);

    ParseAllFiles(nutrient_db);
    /* Create SQL statement *//*
    sql = "SELECT * from COMPANY";

    *//* Execute SQL statement *//*
    rc = sqlite3_exec(nutrient_db, sql, callback, (void*)data, &errorMessage);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    }else{
        fprintf(stdout, "Operation done successfully\n");
    }
*/
    sqlite3_close(nutrient_db);

    return 0;
}