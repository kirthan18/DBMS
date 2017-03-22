/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
	enum Datatype
	{
		INTEGER = 0,
		DOUBLE = 1,
		STRING = 2
	};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
	enum Operator
	{
		LT, 	/* Less Than */
				LTE,	/* Less Than or Equal to */
				GTE,	/* Greater Than or Equal to */
				GT		/* Greater Than */
	};

/**
 * @brief Size of String key.
 */
	const  int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
	const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     sibling ptr               key               rid
	const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                    sibling ptr           key                      rid
	const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( 10 * sizeof(char) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
	const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        extra pageNo                 key            pageNo   -1 due to structure padding
	const  int DOUBLEARRAYNONLEAFSIZE = (( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( PageId ) )) - 1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra pageNo             key                   pageNo
	const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( 10 * sizeof(char) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
	template <class T>
	class RIDKeyPair{
	public:
		RecordId rid;
		T key;
		void set( RecordId r, T k)
		{
			rid = r;
			key = k;
		}
	};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
	template <class T>
	class PageKeyPair{
	public:
		PageId pageNo;
		T key;
		void set( int p, T k)
		{
			pageNo = p;
			key = k;
		}
	};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
	template <class T>
	bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
	{
		if( r1.key != r2.key )
			return r1.key < r2.key;
		else
			return r1.rid.page_number < r2.rid.page_number;
	}


/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
	struct IndexMetaInfo{
		/**
         * Name of base relation.
         */
		char relationName[20];

		/**
         * Offset of attribute, over which index is built, inside the record stored in pages.
         */
		int attrByteOffset;

		/**
         * Type of the attribute over which index is built.
         */
		Datatype attrType;

		/**
         * Page number of root page of the B+ Tree inside the file index file.
         */
		PageId rootPageNo;
	};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
	struct NonLeafNodeInt{
		/**
         * Level of the node in the tree.
         */
		int level;

		/**
         * Stores keys.
         */
		int keyArray[ INTARRAYNONLEAFSIZE ];

		/**
         * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
         */
		PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
	};

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
*/
	struct NonLeafNodeDouble{
		/**
         * Level of the node in the tree.
         */
		int level;

		/**
         * Stores keys.
         */
		double keyArray[ DOUBLEARRAYNONLEAFSIZE ];

		/**
         * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
         */
		PageId pageNoArray[ DOUBLEARRAYNONLEAFSIZE + 1 ];
	};

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/
	struct NonLeafNodeString{
		/**
         * Level of the node in the tree.
         */
		int level;

		/**
         * Stores keys.
         */
		char keyArray[ STRINGARRAYNONLEAFSIZE ][ STRINGSIZE ];

		/**
         * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
         */
		PageId pageNoArray[ STRINGARRAYNONLEAFSIZE + 1 ];
	};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
	struct LeafNodeInt{
		/**
         * Stores keys.
         */
		int keyArray[ INTARRAYLEAFSIZE ];

		/**
         * Stores RecordIds.
         */
		RecordId ridArray[ INTARRAYLEAFSIZE ];

		/**
         * Page number of the leaf on the right side.
           * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
         */
		PageId rightSibPageNo;
	};

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
*/
	struct LeafNodeDouble{
		/**
         * Stores keys.
         */
		double keyArray[ DOUBLEARRAYLEAFSIZE ];

		/**
         * Stores RecordIds.
         */
		RecordId ridArray[ DOUBLEARRAYLEAFSIZE ];

		/**
         * Page number of the leaf on the right side.
           * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
         */
		PageId rightSibPageNo;
	};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
	struct LeafNodeString{
		/**
         * Stores keys.
         */
		char keyArray[ STRINGARRAYLEAFSIZE ][ STRINGSIZE ];

		/**
         * Stores RecordIds.
         */
		RecordId ridArray[ STRINGARRAYLEAFSIZE ];

		/**
         * Page number of the leaf on the right side.
           * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
         */
		PageId rightSibPageNo;
	};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
	class BTreeIndex {

	private:

		/**
         * File object for the index file.
         */
		File		*file;

		/**
         * Buffer Manager Instance.
         */
		BufMgr	*bufMgr;

		/**
         * Page number of meta page.
         */
		PageId	headerPageNum;

		/**
         * page number of root page of B+ tree inside index file.
         */
		PageId	rootPageNum;

		/**
         * Datatype of attribute over which index is built.
         */
		Datatype	attributeType;

		/**
         * Offset of attribute, over which index is built, inside records.
         */
		int 		attrByteOffset;

		/**
         * Number of keys in leaf node, depending upon the type of key.
         */
		int			leafOccupancy;

		/**
         * Number of keys in non-leaf node, depending upon the type of key.
         */
		int			nodeOccupancy;


		// MEMBERS SPECIFIC TO SCANNING

		/**
         * True if an index scan has been started.
         */
		bool		scanExecuting;

		/**
         * Index of next entry to be scanned in current leaf being scanned.
         */
		int			nextEntry;

		/**
         * Page number of current page being scanned.
         */
		PageId	currentPageNum;

		/**
         * Current Page being scanned.
         */
		Page		*currentPageData;

		/**
         * Low INTEGER value for scan.
         */
		int			lowValInt;

		/**
         * Low DOUBLE value for scan.
         */
		double	lowValDouble;

		/**
         * Low STRING value for scan.
         */
		std::string	lowValString;
		char lowValChar[STRINGSIZE];
		/**
         * High INTEGER value for scan.
         */
		int			highValInt;

		/**
         * High DOUBLE value for scan.
         */
		double	highValDouble;

		/**
         * High STRING value for scan.
         */
		std::string highValString;
		char highValChar[STRINGSIZE];

		/**
         * Low Operator. Can only be GT(>) or GTE(>=).
         */
		Operator	lowOp;

		/**
         * High Operator. Can only be LT(<) or LTE(<=).
         */
		Operator	highOp;


	public:

		/**
         * BTreeIndex Constructor.
           * Check to see if the corresponding index file exists. If so, open the file.
           * If not, create it and insert entries for every tuple in the base relation using FileScan class.
         *
         * @param relationName        Name of file.
         * @param outIndexName        Return the name of index file.
         * @param bufMgrIn						Buffer Manager Instance
         * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
         * @param attrType						Datatype of attribute over which index is built
         * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
         */
		BTreeIndex(const std::string & relationName, std::string & outIndexName,
				   BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);


		/**
         * BTreeIndex Destructor.
           * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
           * and delete file instance thereby closing the index file.
           * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
           * */
		~BTreeIndex();


		/**
           * Insert a new entry using the pair <value,rid>.
           * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
           * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
           * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
           * Make sure to unpin pages as soon as you can.
         * @param key			Key to insert, pointer to integer/double/char string
         * @param rid			Record ID of a record whose entry is getting inserted into the index.
          **/
		const void insertEntry(const void* key, const RecordId rid);

		/**
           * Begin a filtered scan of the index.  For instance, if the method is called
           * using ("a",GT,"d",LTE) then we should seek all entries with a value
           * greater than "a" and less than or equal to "d".
           * If another scan is already executing, that needs to be ended here.
           * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
           * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
         * @param lowVal	Low value of range, pointer to integer / double / char string
         * @param lowOp		Low operator (GT/GTE)
         * @param highVal	High value of range, pointer to integer / double / char string
         * @param highOp	High operator (LT/LTE)
         * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
         * @throws  BadScanrangeException If lowVal > highval
           * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
          **/
		const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);

		/**
           * Fetch the record id of the next index entry that matches the scan.
           * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
           * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
           * @throws ScanNotInitializedException If no scan has been initialized.
           * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
          **/
		const void scanNext(RecordId& outRid);  // returned record id

		/**
           * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
           * @throws ScanNotInitializedException If no scan has been initialized.
          **/
		const void endScan();

		/**
		 * Compares two values
		 * @tparam T Class of values to be compared
		 * @param a First value
		 * @param b Second value
		 * @return 1 if a is greater than b; -1 if a is lesser than b and 0 if both are equal
		 */
		template <class T>
		int compare(T a, T b);

		/**
		 * Copies one variable value to another value
		 * @tparam T Class of values to be copied
		 * @param a Destination variable
		 * @param b Source variable
		 */
		template <class T>
		void copy(T &a, T &b);

		/**
		 * Validates the operators given for scanning a relation
		 * @param lowOpParm Lower operator parameter
		 * @param highOpParm Higher operator parameter
		 */
		void validateOperators(const Operator &lowOpParm, const Operator &highOpParm) const;

		/**
		 * Check is low values are lower than or equal to higher values
		 * @param lowValParm Lower value
		 * @param highValParm Higher Value
		 */
		void validateLowAndHighValues(const void *lowValParm, const void *highValParm);

		/**
		 * Finds the leaf page containing the keys less than/equal to the low value parameter
		 * @tparam T1 Class of the parameter
		 * @tparam T2 Class of the leaf node
		 * @param lowValParm
		 * @param arrayLength Maximum number of entries in leaf node
		 */
		template <class T1, class T2>
		void findLeafPage(T1 lowValParm, int arrayLength);

		/**
		 * Adds a key-rid pair to leaf node
		 * @tparam T1 Class of leaf node
		 * @tparam T2 Class of key value to insert
		 * @param indexToInsert Index in leaf node where entry is to be inserted
		 * @param lastIndex Last index in the leaf node which contains a valid entry
		 * @param leafNode Leaf node in which the key-rid pair is to be inserted
		 * @param ridKeyPair The key-rid pair to be inserted in leaf node
		 */
		template <class T1, class T2>
		void addLeafNodeEntry(int indexToInsert, int lastIndex, T1* leafNode, RIDKeyPair<T2> ridKeyPair);
		
		/**
		 *
		 * @tparam T1 Class of key in the index
		 * @tparam T2 Class of the leaf node
		 * @tparam T3 Class of the non leaf node
		 * @param ridKeyPair The rid-key pair that we want to insert
		 * @param pageId Page id of the current page we are looking at
		 * @param isLeaf 1 if the node is a leaf node
		 * @param max_entries_leaf Maximum number of key-rid pairs the leaf node can hold
		 * @param max_entries_non_leaf Maximum number of key-page id pairs the non - leaf node can hold
		 * @param newValue The new value that is pushed up from the leaf node
		 * @param newPage Page id of the new leaf node that is created
		 */
		template <class T1, class T2, class T3>
		void insertEntry_(RIDKeyPair<T1> ridKeyPair,
						  PageId pageId,
						  bool isLeaf,
						  int max_entries_leaf,
						  int max_entries_non_leaf,
						  T1& newValue,
						  PageId& newPage);

		/**
		 * Splits a non leaf node, redistributes entries and updates book keeping information
		 * @tparam T1 Class of the key in the index
		 * @tparam T2 Class of the leaf node that is split
		 * @param max_entries Maximum number of key-rid pairs the leaf node can hold
		 * @param ridKeyPair The rid-key pair that is to be inserted
		 * @param existingLeafNode The leaf node that is being split
		 * @param newLeafPageId The new leaf node that is created
		 * @param newKey The new key that is going to be pushed to the parent non-leaf
		 * 				 node as a result of the leaf being split
		 */
		template <class T1, class T2>
		void splitLeafNode(int max_entries,
						   RIDKeyPair<T1> ridKeyPair,
						   T2* existingLeafNode,
						   PageId& newLeafPageId,
						   T1& newKey);

		/**
		 * Splits a non leaf node, redistributes entries and updates book keeping information
		 * @tparam T1 Class of key in the index
		 * @tparam T2 Class of the non leaf node that is split
		 * @param indexToInsert Index at which the key-page id pair is to be inserted
		 * @param max_entries Maximum number of key-page id pairs the non-leaf node can hold
		 * @param nonLeafNode The node that is going to be split
		 * @param newPageId The page id of the new non leaf node
		 * @param newKeyToParent The new key that is to be pushed to the parent node
		 * @param newKeyFromChild The new key that is to be inserted in the non leaf node
		 * @param newChildPageId The pointer to the child page which is to be inserted in the non leaf node
		 */
		template <class T1, class T2>
		void splitNonLeafNode(int indexToInsert,
							  int max_entries,
							  T2* nonLeafNode,
							  PageId& newPageId,
							  T1& newKeyToParent,
							  T1& newKeyFromChild,
							  PageId newChildPageId);

		/**
		 * Creates a new root node when the existing root node exceeds its capacity.
         * Changes the book keeping information to point to the new root and updates pointers/
		 * @tparam T1 Class of key in the index
		 * @tparam T2 Class of the root (non-leaf) node
		 * @param key Key of the new root node
		 * @param newPageId PageId of the new root node
		 * @param max_entries Maximum number of key-page no pairs the root node can hold
		 */
		template<class T1, class T2>
		void createNewRoot(T1& key, PageId newPageId, int max_entries);

	};

}
