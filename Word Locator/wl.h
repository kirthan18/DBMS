//
// File: wl.h
//
//  Description: Contains class declarations necessary for Trie
//               Contains function declarations for various functions.
//  Student Name: Kirthanaa Raghuraman
//  UW Campus ID: 9073422751
//  email: kraghuraman@wisc.edu

#ifndef WL
#define WL

#include <vector>
#include <string>

/*
 * Error message to be displayed whenever an invalid or incorrect command is entered.
 */
#define INVALID_COMMAND "ERROR: Invalid command"

/**
 * Error message to be displayed when a word cannot be located in the file.
 */
#define NO_MATCHING_ENTRY "No matching entry"

/**
 * Maximum number of children each node in the trie can have
 * (26 alphabets + 10 digits + 1 apostrophe)
 */
#define CHILDREN_SIZE 37

/**
 * Parses the command to remove any extraneous spaces and also converts all strings to lower case.
 * @param string Command to be parsed
 * @return A vector of strings in lowercase.
 */
std::vector<std::string> ParseCommand(std::string);

/**
 * Validates each command and prints error messages when necessary.
 * @param vector A vector containing the command and its arguments
 */
void ValidateAndExecuteCommand(std::vector<std::string>);

/**
 * Prints error message for invalid commands.
 */
void PrintInvalidCommandError();

/**
 * Gets the index of the child for a character in the trie.
 * @param c Character whose index is required
 */
int GetCharacterChildIndex(char c);

/**
 * Class to represent a node in the trie.
 */
class TrieNode {

public:
    /**
     * Vector consisting of positions of words in the file.
     */
    std::vector<unsigned long> word_index;

    /**
     * Set to true if there is a word ending with the current character.
     */
    bool is_word_end;

    /**
     * Each node contains an array of pointers of size 37 (10 digits + 26 alphabets + 1 apostrophe).
     * Each child in turn points to successive nodes in the trie.
     */
    TrieNode *children[CHILDREN_SIZE];
};

/**
 * Class describing a Trie and all the operations to be performed on it.
 */
class Trie {
    /**
     * Root node of the trie
     */
    TrieNode *root;

public:
    /**
     * Constructor for Trie class.
     */
    Trie();

    /**
     * Destructor for Trie class.
     */
    ~Trie();

    /**
     * Returns the reference to the root of the trie.
     * @return Reference to root node of trie.
     */
    TrieNode *GetRootNode();

    /**
     * Creates a new Trie node and initializes its child pointers to NULL.
     * @return Reference to initialized trie node.
     */
    TrieNode *GetTrieNode();

    /**
     * Inserts a word in the trie along with its word index in the file.
     * @param word Word to be inserted.
     * @param word_index Index of the word in the file.
     */
    void InsertWord(std::string word, unsigned long word_index);

    /**
     * Locates a word's occurrence in the trie.
     * @param word Word to be located.
     * @param occurrence Occurrence of the word to be located.
     * @return 0 if the word is not present, the index of the word in file if present.
     */
    unsigned long LocateWord(std::string word, unsigned int occurrence);

    /**
     * Recursively clears memory of all nodes in the trie.
     * @param node Root node of the trie to be cleared.
     */
    void DeleteChildren(TrieNode *node);
};

/**
 * Reference to the trie object.
 */
Trie *trie;
#endif
