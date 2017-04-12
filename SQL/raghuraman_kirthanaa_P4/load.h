//
// Created by Kirthanaa Raghuraman on 3/28/17.
//

#ifndef SQL_LOAD_H
#define SQL_LOAD_H

#include "sqlite3.h"
#include <string>
#include <vector>

/**
 * Deletes all tables
 * @param db Database object
 */
void DropAllTables(sqlite3 *db);

/**
 * Deletes table identified by the table name from the database
 * @param tableName Name of table to be deleted
 * @param db Database object
 */
void DropTable(const char *tableName, sqlite3 *db);

/**
 * Creates all tables
 * @param db Database object
 */
void CreateAllTables(sqlite3 *db);

/**
 * Creates table in database
 * @param db Database object
 * @param query Query containing the schema information for the table
 * @param tableName Name of table to be created
 */
void CreateTable(sqlite3 *db, const char *query, const char *tableName);

/**
 * Formulates query to create table FOOD_DES
 * @param db Database object
 */
void CreateTable_Food_Des(sqlite3 *db);

/**
 * Formulates query to create table NUT_DATA
 * @param db Database object
 */
void CreateTable_Nut_Data(sqlite3 *db);

/**
 * Formulates query to create table WEIGHT
 * @param db Database object
 */
void CreateTable_Weight(sqlite3 *db);

/**
 * Formulates query to create table FOOTNOTE
 * @param db Database object
 */
void CreateTable_Footnote(sqlite3 *db);

/**
 * Formulates query to create table FD_GROUP
 * @param db Database object
 */
void CreateTable_Fd_Group(sqlite3 *db);

/**
 * Formulates query to create table LANGUAL
 * @param db Database object
 */
void CreateTable_Langual(sqlite3 *db);

/**
 * Formulates query to create table LANG_DESC
 * @param db Database object
 */
void CreateTable_Lang_Desc(sqlite3 *db);

/**
 * Formulates query to create table NUTR_DEF
 * @param db Database object
 */
void CreateTable_Nutr_Def(sqlite3 *db);

/**
 * Formulates query to create table SRC_CD
 * @param db Database object
 */
void CreateTable_Src_Cd(sqlite3 *db);

/**
 * Formulates query to create table DERIV_CD
 * @param db Database object
 */
void CreateTable_Deriv_Cd(sqlite3 *db);

/**
 * Formulates query to create table DATSRCLN
 * @param db Database object
 */
void CreateTable_Data_Src_Ln(sqlite3 *db);

/**
 * Formulates query to create table DATA_SRC
 * @param db Database object
 */
void CreateTable_Data_Src(sqlite3 *db);

/**
 * Inserts a record in the database
 * @param db Database object
 * @param query Query containing the values to be inserted
 */
void InsertRecord(sqlite3 *db, std::string query);

/**
 * Helper function to create a query by appending field value with necessary quotes and by treating escape sequences
 * @param query Query to be created
 * @param fieldValues Values of fields to be used in query
 * @return Fully formed INSERT query
 */
std::string AppendFieldValues(std::string query, std::vector<std::string> fieldValues);

/**
 * Helper function that parses a line in the input file and extracts the field values from it
 * @param line Line in the input file
 * @return Vector containing the field values
 */
std::vector<std::string> GetFieldValues(std::string line);

/**
 * Parses FOOD_DES file
 * @param db Database object
 */
void Parse_Food_Des(sqlite3 *db);

/**
 * Parses NUT_DATA file
 * @param db Database object
 */
void Parse_Nut_Data(sqlite3 *db);

/**
 * Parses WEIGHT file
 * @param db Database object
 */
void Parse_Weight(sqlite3 *db);

/**
 * Parses FOOTNOTE file
 * @param db Database object
 */
void Parse_Footnote(sqlite3 *db);

/**
 * Parses DATA_SRC file
 * @param db Database object
 */
void Parse_Data_Src(sqlite3 *db);

/**
 * Parses DATASRCLN file
 * @param db Database object
 */
void Parse_Data_Src_Ln(sqlite3 *db);

/**
 * Parses SRC_CD file
 * @param db Database object
 */
void Parse_Src_Cd(sqlite3 *db);

/**
 * Parses DERIV_CD file
 * @param db Database object
 */
void Parse_Deriv_Cd(sqlite3 *db);

/**
 * Parses FD_GROUP file
 * @param db Database object
 */
void Parse_Fd_Group(sqlite3 *db);

/**
 * Parses LANGUAL file
 * @param db Database object
 */
void Parse_Langual(sqlite3 *db);

/**
 * Parses LANG_DESC file
 * @param db Database object
 */
void Parse_Lang_Desc(sqlite3 *db);

/**
 * Parses NUTR_DEF file
 * @param db Database object
 */
void Parse_Nutr_Def(sqlite3 *db);

/**
 * Parses all input files
 * @param db Database object
 */
void ParseAllFiles(sqlite3 *db);

#endif //SQL_LOAD_H
