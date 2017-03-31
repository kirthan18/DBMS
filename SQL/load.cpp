#include <iostream>
#include <sqlite3.h>
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

    cout << endl << "Deleting table : " << tableName << endl;
    cout << "Query: " << dropTableQuery << endl;

    int rc = sqlite3_exec(db, dropTableQuery, NULL, 0, &errorMessage);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    } else {
        fprintf(stdout, "Table dropped successfully\n");
    }

}

void DropAllTables(sqlite3 *nutrient_db) {
    DropTable("COMPANY", nutrient_db);
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
}

void CreateTable(sqlite3 *db, const char *query, const char* tableName) {
    char *errorMessage;
    cout << endl << "Creating table : " << tableName << endl;

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

void CreateTable_Company(sqlite3 *db) {
    /* Create SQL statement */
    const char *query = "CREATE TABLE COMPANY("  \
         "ID INT PRIMARY KEY        NOT NULL," \
         "NAME           TEXT    NOT NULL," \
         "AGE            INT     NOT NULL," \
         "ADDRESS        CHAR(50)        ," \
         "SALARY         REAL             );";
    CreateTable(db, query, "COMPANY");
}

void CreateTable_Food_Des(sqlite3 *db) {
    const char *query = "CREATE TABLE FOOD_DES(              " \
                        "NDB_No         CHAR(5)     NOT NULL," \
                        "FdGrp_Cd       CHAR(4)     NOT NULL," \
                        "Long_Desc      CHAR(200)   NOT NULL," \
                        "Shrt_Desc      CHAR(60)    NOT NULL," \
                        "ComName        CHAR(100)           ," \
                        "ManufacName    CHAR(65)            ," \
                        "Survey         CHAR(1)             ," \
                        "Ref_desc       CHAR(135)           ," \
                        "Refuse         REAL                ," \
                        "SciName        CHAR(65)            ," \
                        "N_Factor       REAL                ," \
                        "Pro_Factor     REAL                ," \
                        "Fat_Factor     REAL                ," \
                        "CHO_Factor     REAL                 " \
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
                        "CC             CHAR(1)             " \
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
                        "Nutr_No        CHAR(3)     NOT NULL," \
                        "Footnt_Txt     CHAR(200)   NOT NULL" \
                        ");";
    CreateTable(db, query, "FOOTNOTE");
}


void CreateAllTables(sqlite3 *db) {
    CreateTable_Company(db);

    // Principal Files
    CreateTable_Food_Des(db);
    CreateTable_Nut_Data(db);
    CreateTable_Weight(db);
    CreateTable_Footnote(db);

    // Support files
    /*CreateTable_Fd_Group(db);
    CreateTable_Langual(db);
    CreateTable_Lang_Desc(db);
    CreateTable_Nutr_Def(db);
    CreateTable_Src_Cd(db);
    CreateTable_Deriv_Cd(db);
    CreateTable_Data_Src_Ln(db);
    CreateTable_Data_Src(db);*/
}


int main() {

    sqlite3 *nutrient_db;
    char *errorMessage = 0;
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

    /* Create SQL statement */
    const char *query = "CREATE TABLE COMPANY("  \
         "ID INT PRIMARY KEY     NOT NULL," \
         "NAME           TEXT    NOT NULL," \
         "AGE            INT     NOT NULL," \
         "ADDRESS        CHAR(50)," \
         "SALARY         REAL );";

    /* Execute SQL statement */
    rc = sqlite3_exec(nutrient_db, query, callback, 0, &errorMessage);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    } else {
        fprintf(stdout, "Table created successfully\n");
    }

    const char *sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "  \
         "VALUES (1, 'Paul', 32, 'California', 20000.00 ); " \
         "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "  \
         "VALUES (2, 'Allen', 25, 'Texas', 15000.00 ); "     \
         "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" \
         "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );" \
         "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" \
         "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );";

    /* Execute SQL statement */
    rc = sqlite3_exec(nutrient_db, sql, callback, 0, &errorMessage);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    }else{
        fprintf(stdout, "Records created successfully\n");
    }

    /* Create SQL statement */
    sql = "SELECT * from COMPANY";

    /* Execute SQL statement */
    rc = sqlite3_exec(nutrient_db, sql, callback, (void*)data, &errorMessage);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", errorMessage);
        sqlite3_free(errorMessage);
    }else{
        fprintf(stdout, "Operation done successfully\n");
    }

    sqlite3_close(nutrient_db);

    return 0;
}