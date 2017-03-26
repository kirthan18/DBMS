
/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"


//#define DEBUG

namespace badgerdb
{
// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Template
// -----------------------------------------------------------------------------


    void initializeLeafNodes(const Datatype attrType, Page *rootPage) {
        switch (attrType) {
            case INTEGER: {

                NonLeafNodeInt *nonLeafNodeInt = (NonLeafNodeInt *) rootPage;
                for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                    nonLeafNodeInt->pageNoArray[i] = 0;
                }
                nonLeafNodeInt->level = 1;
                break;
            }

            case DOUBLE:{

                NonLeafNodeDouble *nonLeafNodeDouble = (NonLeafNodeDouble *) rootPage;
                for (int i = 0; i < DOUBLEARRAYLEAFSIZE; i++) {
                    nonLeafNodeDouble->pageNoArray[i] = 0;
                }
                nonLeafNodeDouble->level = 1;
                break;
            }

            case STRING:{

                NonLeafNodeString *nonLeafNodeString = (NonLeafNodeString *) rootPage;
                for (int i = 0; i < STRINGARRAYLEAFSIZE; i++) {
                    nonLeafNodeString->pageNoArray[i] = 0;
                }
                nonLeafNodeString->level = 1;
                break;
            }
        }
    }
// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
    BTreeIndex::BTreeIndex(const std::string & relationName,
                           std::string & outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType)
    {
        // Initializations to be done
        this->leafOccupancy = 0;
        this->nodeOccupancy = 0;

        // Obtaining index file name from relation and attribute byte offset
        std::ostringstream	idxStr;
        idxStr	<<	relationName	<<	'.'	<<	attrByteOffset;
        std::string	indexName	=	idxStr.str();	//	indexName	is	the	name	of	the	index	file

        if (BlobFile::exists(indexName)) {
            std::cout << "File exists and is open" << std::endl;
            std::cout << "File exists!" << std::endl;
            if (BlobFile::exists(indexName)) {
                std::cout << "File exists and is open" << std::endl;
            } else {
                std::cout << "File maybe doesnt exist or is not open" << std::endl;
            }
            Page *indexPage;
            PageId indexPageNo = 1;

            // Open the file
            BlobFile *blobFile = new BlobFile(indexName, false);

            //Set btree index parameters
            this->file = blobFile;
            this->headerPageNum = indexPageNo;
            this->attributeType = attrType;
            this->attrByteOffset = attrByteOffset;
            this->bufMgr = bufMgrIn;
            this->scanExecuting = false;

            // Read the first page from the file (meta node)
            bufMgrIn->readPage(blobFile, indexPageNo, indexPage);
            IndexMetaInfo* indexMetaInfo = (IndexMetaInfo*) indexPage;

            // Check for bad index
            std::string reason = "";
            if (indexMetaInfo->attrByteOffset != attrByteOffset) {
                reason = "Attribute Byte offset doesn't match! \n";
            }
            if (indexMetaInfo->attrType != attrType) {
                reason += "Attribute data type doesn't match! \n";
            }

            std::ostringstream	idxStr_;
            idxStr_	<<	indexMetaInfo->relationName	<<	'.'	<<	indexMetaInfo->attrByteOffset;
            std::string	indexName_	=	idxStr_.str();	//	indexName	is	the	name	of	the	index	file

            if (strcmp(indexName_.c_str(),indexName.c_str()) != 0) {
                reason += "Relation does not match! \n";
            }
            if (!reason.empty()) {
                try {
                    bufMgrIn->unPinPage(file, indexPageNo, false);
                } catch (PageNotPinnedException e) {
                }
                throw BadIndexInfoException(reason);
            }


            // Get Root page number from the meta node
            this->rootPageNum = indexMetaInfo->rootPageNo;

            // Unpin the header page
            try {
                bufMgrIn->unPinPage(file, indexPageNo, false);
            } catch (PageNotPinnedException e) {
            }
        } else {
            std::cout << "File maybe doesnt exist or is not open" << std::endl;


            BlobFile *blobFile = new BlobFile(indexName, true);
            // Index file doesn't exist!

            // Allocate new meta page
            Page *indexMetaPage;
            PageId headerPageID;

            bufMgrIn->allocPage(blobFile, headerPageID, indexMetaPage);
            this->headerPageNum = headerPageID;

            IndexMetaInfo *indexMetaInfo =  (IndexMetaInfo*) indexMetaPage;
            strcpy(indexMetaInfo->relationName, relationName.c_str());
            indexMetaInfo->attrType = attrType;
            indexMetaInfo->attrByteOffset = attrByteOffset;

            //Allocate page for root node in buffer pool
            Page *rootPage;
            PageId rootPageId;

            bufMgrIn->allocPage(blobFile, rootPageId, rootPage);
            indexMetaInfo->rootPageNo = rootPageId;

            //Set btree index parameters
            this->file = blobFile;
            this->attributeType = attrType;
            this->attrByteOffset = attrByteOffset;
            this->bufMgr = bufMgrIn;
            this->scanExecuting = false;
            this->rootPageNum = rootPageId;

            initializeLeafNodes(attrType, rootPage);

            try{
                bufMgrIn->unPinPage(blobFile, headerPageID, true);
            } catch (PageNotPinnedException e) {
            }

            try{
                bufMgrIn->unPinPage(blobFile, rootPageId, true);
            } catch (PageNotPinnedException e ) {
            }

            //TODO - Scan record and insert into B tree
            FileScan fileScan(relationName, this->bufMgr);
            try {
                while (1) {
                    // Start scanning the file, get each record and insert it

                    RecordId recordId;
                    fileScan.scanNext(recordId);

                    std::string record = fileScan.getRecord();
                    void* key = (void*) (record.c_str() + attrByteOffset);
                    insertEntry(key, recordId);
                }
            } catch (EndOfFileException e) {
                // Do nothing!
            }
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex()
    {
        this->scanExecuting = false;
        this->bufMgr->flushFile(this->file);
        delete this->file;
    }


    template <class T1, class T2>
    void  BTreeIndex::splitNonLeafNode(int indexToInsert, int max_entries, T2* nonLeafNode, T1& newKeyToParent,
                                       T1& newKeyFromChild, PageId& newPageId, PageId newChildPageId){
        //full, need split
        Page* newNonLeafPage;
        bufMgr->allocPage(file, newPageId, newNonLeafPage);
        T2* newNonLeafNode = (T2*) newNonLeafPage;

        /*
         * Allocate a temporary array for both pageIds and keys
         * PageId array length is max_entries + 2
         * because we have existing m + 1 pointers + one more pointer to new element
         */
        T1 tempKey[max_entries + 1];
        PageId tempPageId[max_entries + 2];

        // Copy all keys to temp array
        for(int i=0; i < max_entries; i++) {
            copy<T1> (tempKey[i], nonLeafNode->keyArray[i]);
        }

        // Copy all pageIDs to temp array
        for(int i=0; i < max_entries + 1; i++) {
            tempPageId[i] = nonLeafNode->pageNoArray[i];
        }

        // Push all elements by one from the end till indexToInsert
        for(int i= max_entries; i > indexToInsert; i--) {
            copy<T1> (tempKey[i], tempKey[i - 1]);
            tempPageId[i + 1] = tempPageId[i];
        }

        // Copy the key and page number at the indexToInsert position
        copy<T1> (tempKey[indexToInsert], newKeyFromChild);
        tempPageId[indexToInsert + 1] = newChildPageId;

        // Re-initialize old and new non lead nodes
        for(int i=0; i < max_entries + 1; i++) {
            nonLeafNode->pageNoArray[i] = 0;
            newNonLeafNode->pageNoArray[i] = 0;
        }

        // Copy first half of elements to the old leaf node
        for(int i=0; i< (max_entries + 1) / 2; i++ ){
            copy<T1> (nonLeafNode->keyArray[i], tempKey[i]);
            nonLeafNode->pageNoArray[i] = tempPageId[i];
        }
        // Copy the last pointer to the pageNoArray
        nonLeafNode->pageNoArray[(max_entries + 1) / 2] = tempPageId[(max_entries + 1) / 2];

        // Copy the second half of elements to the old leaf node
        for(int i = (max_entries + 1) / 2 + 1; i < max_entries + 1; i++){
            copy<T1> (newNonLeafNode->keyArray[i - (max_entries + 1) / 2 - 1], tempKey[i]);
            newNonLeafNode->pageNoArray[i - (max_entries + 1) / 2 - 1 ] = tempPageId[i];
        }

        // Copy the last pointer of the new node
        newNonLeafNode->pageNoArray[(max_entries + 1)/2] = tempPageId[
                max_entries + 1];

        // Set the level of the new non leaf node to that of the existing non leaf node
        newNonLeafNode->level = nonLeafNode->level;

        // Set the new key value of the parent node
        copy<T1> (newKeyToParent, tempKey[(max_entries + 1) / 2]);

        // Page is modified so unpin by setting the dirty bit to true
        bufMgr->unPinPage(file, newPageId, true);
    };

    template <class T1, class T2>
    void BTreeIndex::addLeafNodeEntry(int indexToInsert, int lastIndex, T1* leafNode, RIDKeyPair<T2> ridKeyPair) {
        for(int i = lastIndex; i > indexToInsert; i--) {
            copy<T2> (leafNode->keyArray[i], leafNode->keyArray[i - 1]);
            leafNode->ridArray[i] = leafNode->ridArray[i - 1];
        }
        copy<T2> (leafNode->keyArray[indexToInsert], ridKeyPair.key);
        leafNode->ridArray[indexToInsert] = ridKeyPair.rid;
    }

    template <class T1, class T2>
    void BTreeIndex::splitLeafNode(int max_entries,
                                   T2* existingLeafNode,
                                   PageId& newLeafPageId,
                                   T1& newKey,
                                   RIDKeyPair<T1> ridKeyPair) {
        Page* newLeafPage;
        int currIndex = 0;

        // Allocate new page in buffer pool for the new leaf node and cast it
        bufMgr->allocPage(file, newLeafPageId, newLeafPage);
        T2 *newLeafNode = (T2*) newLeafPage;

        /*
         * Temp arrays to store key and rids.
         * Arrays have one extra element to account for the case where the leaf node is already full and
         * we need to insert a key in it.
         */
        T1 tempKey[max_entries + 1];
        RecordId tempRid[max_entries + 1];

        //Find position to insert new key value
        while (1) {
            // If current index position is greater than the number if entries, break
            if (currIndex >= max_entries) {
                break;
            }

            // If the next entry points to a page with number 0, break
            if (existingLeafNode->ridArray[currIndex].page_number == 0) {
                break;
            }

            // If the value of the current key if greater than or equal to the the low value, break
            if (compare<T1>(existingLeafNode->keyArray[currIndex], ridKeyPair.key) > 0) {
                break;
            }

            currIndex++;
        }

        int indexToInsert = currIndex;

        // Copy all entries to temporary array
        for(int i = 0; i < max_entries; i++) {
            tempRid[i] = existingLeafNode->ridArray[i];
            copy<T1> (tempKey[i], existingLeafNode->keyArray[i]);
            existingLeafNode->ridArray[i].page_number = 0;
        }

        // Move entries after the indexToInsert position by one to leave space for the new element
        for(int i= max_entries; i > indexToInsert; i--){
            copy<T1> (tempKey[i], tempKey[i-1]);
            tempRid[i] = tempRid[i-1];
        }

        // Copy the key - record at the indexToInsert position
        copy<T1> (tempKey[indexToInsert], ridKeyPair.key);
        tempRid[indexToInsert] = ridKeyPair.rid;

        // Reset all entries in the old and new leaf nodes
        for(int i=0; i < max_entries; i++) {
            existingLeafNode->ridArray[i].page_number = 0;
            newLeafNode->ridArray[i].page_number = 0;
        }

        // Copy first half of entries into the existing leaf node
        for(int i=0; i< (max_entries + 1) / 2; i++ ){
            copy<T1> (existingLeafNode->keyArray[i], tempKey[i]);
            existingLeafNode->ridArray[i] = tempRid[i];
        }

        // Copy the second half of entries into the new leaf node
        for(int i= (max_entries + 1) / 2; i < max_entries + 1; i++){
            copy<T1> (newLeafNode->keyArray[i - (max_entries + 1) / 2], tempKey[i]);
            newLeafNode->ridArray[i - (max_entries + 1) / 2] = tempRid[i];
        }

        // Assign sibling pointers
        newLeafNode->rightSibPageNo = existingLeafNode->rightSibPageNo;
        existingLeafNode->rightSibPageNo = newLeafPageId;

        // The new key will be the first entry of the new leaf node
        copy<T1> (newKey, newLeafNode->keyArray[0]);

        // Unpin the new leaf page created from the buffer pool
        bufMgr->unPinPage(file, newLeafPageId, true);
    };


    template <class T1, class T2, class T3>
    void BTreeIndex::insertEntry_ (bool isLeafNode, int max_entries_leaf, int max_entries_non_leaf, T1& newValue,
                                   PageId pageId, PageId& newPageId, RIDKeyPair<T1> ridKeyPair) {
        if(isLeafNode){
            // Read the page
            Page* leafPage;
            bufMgr->readPage(file, pageId, leafPage);
            T2 *leafNode = (T2 *) leafPage;

            // Find position where the new record should be inserted
            int currIndex = 0;
            while (1) {
                // If current index position is greater than the number if entries, break
                if (currIndex >= max_entries_leaf) {
                    break;
                }

                // If the next entry points to a page with number 0, break
                if (leafNode->ridArray[currIndex].page_number == 0) {
                    break;
                }

                // If the value of the current key if greater than or equal to the the low value, break
                if (compare<T1>(leafNode->keyArray[currIndex], ridKeyPair.key) > 0) {
                    break;
                }

                currIndex++;
            }

            int lastIndex = 0;

            // Find index of last entry in the leaf node
            while (lastIndex < max_entries_leaf) {
                if (leafNode->ridArray[lastIndex].page_number == 0) {
                    break;
                }
                lastIndex++;
            }

            if(lastIndex < max_entries_leaf){
                // Leaf node is not full
                // Add entry to leaf
                addLeafNodeEntry<T2, T1>(currIndex, lastIndex, leafNode, ridKeyPair);
            } else {
                // Leaf node is full
                // Split leaf node and insert
                splitLeafNode<T1,T2>(max_entries_leaf, leafNode, newPageId, newValue, ridKeyPair);
            }

            leafOccupancy++;

            // The page is now modified
            // Unpin it by setting the dirty flag to true
            bufMgr->unPinPage(file, pageId, true);

        } else {
            Page* nonLeafPage;
            bufMgr->readPage(file, pageId, nonLeafPage);
            T3* nonLeafNode = (T3*) nonLeafPage;

            // Find pageArray position
            int currIndex = 0;
            while (1) {
                // If current index position is greater than the number if entries, break
                if (currIndex >= max_entries_non_leaf) {
                    break;
                }

                // If the next entry points to a page with number 0, break
                if (nonLeafNode->pageNoArray[currIndex + 1] == 0) {
                    break;
                }

                // If the value of the current key if greater than or equal to the the low value, break
                if (compare<T1> (nonLeafNode->keyArray[currIndex], ridKeyPair.key) >= 0) {
                    break;
                }

                currIndex++;
            }

            // Index file is empty.
            if(nonLeafNode->pageNoArray[currIndex] == 0){
                PageId leafPageId;
                Page* leafPage;

                // Allocate page for new leaf node and cast it
                bufMgr->allocPage(this->file, leafPageId, leafPage);
                T2* leafNode = (T2*) leafPage;

                //Initializations for leaf node
                for(int i = 0; i < max_entries_leaf; i++) {
                    leafNode->ridArray[i].page_number = 0;
                }
                copy<T1> (leafNode->keyArray[0], ridKeyPair.key);
                leafNode->ridArray[0] = ridKeyPair.rid;
                leafNode->rightSibPageNo = 0;

                // Assign parent node's pointer to the created leaf page number
                nonLeafNode->pageNoArray[0] = leafPageId;

                // Unpin both the parent node and leaf page from buffer pool
                bufMgr->unPinPage(this->file, leafPageId, true);
                bufMgr->unPinPage(this->file, pageId, true);
                return;
            }

            bufMgr->unPinPage(file, pageId, false);
            T1 newChildValue;
            PageId newChildPageId = 0;

            bool isLeafNode = nonLeafNode->level == 1? true: false;
            insertEntry_<T1, T2, T3>(isLeafNode, max_entries_leaf, max_entries_non_leaf, newChildValue,
                    nonLeafNode->pageNoArray[currIndex], newChildPageId, ridKeyPair);

            // Check if child split.
            if(newChildPageId != 0){
                // Child node is split, so add new key to this node
                bufMgr->readPage(file, pageId, nonLeafPage);
                T3* nonLeafNode = (T3*) nonLeafPage;

                // Find index of last entry in the leaf node
                int lastIndex;
                for (lastIndex = 0; lastIndex < max_entries_non_leaf; lastIndex++) {
                    if (nonLeafNode->pageNoArray[lastIndex + 1] == 0) {
                        break;
                    }
                }

                if(lastIndex < max_entries_non_leaf){
                    // Node is not full
                    // Add entry to node
                    for(int i = lastIndex; i > currIndex; i--){
                        copy<T1> (nonLeafNode->keyArray[i], nonLeafNode->keyArray[i - 1]);
                        nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
                    }
                    copy<T1> (nonLeafNode->keyArray[currIndex], newChildValue);
                    nonLeafNode->pageNoArray[currIndex + 1] = newChildPageId;
                }
                else{
                    // Node is full - split node and insert new key
                    splitNonLeafNode<T1, T3>(currIndex, max_entries_non_leaf, nonLeafNode, newValue, newChildValue,
                                             newPageId, newChildPageId);
                }

                nodeOccupancy++;
                bufMgr->unPinPage(file, pageId, true);
            }
        }
    }


    template<class T1, class T2>
    void BTreeIndex::createNewRoot(T1& key, PageId newPageId, int max_entries){
        PageId pageId;
        Page *rootPage;

        bufMgr->allocPage(file, pageId, rootPage);

        T2 *rootNode = (T2 *) rootPage;
        for (int i = 0; i <= max_entries; i++) {
            rootNode->pageNoArray[i] = 0;
        }

        this->rootPageNum = pageId;
        rootNode->pageNoArray[0] = this->rootPageNum;

        //TODO - Check this line - ERROR Might occur
        // rootNode->pageNoArray[1] = newPageId;

        rootNode->level = 0;

        copy<T1>(rootNode->keyArray[0], key);
        bufMgr->unPinPage(this->file, pageId, true);
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        switch (this->attributeType) {
            case INTEGER: {
                PageId newPageIdInt = 0;
                int newValueInt;
                int key_ = *((int *) key);
                RIDKeyPair<int> ridKeyPairInt;

                ridKeyPairInt.set(rid, key_);
                insertEntry_<int, LeafNodeInt, NonLeafNodeInt> (0, INTARRAYLEAFSIZE, INTARRAYNONLEAFSIZE, newValueInt,
                                                                rootPageNum, newPageIdInt, ridKeyPairInt);

                // Check if root node is split
                if (newPageIdInt != 0) {
                    //Since root node is more than full, create a new root
                    createNewRoot<int, NonLeafNodeInt>(newValueInt, newPageIdInt, INTARRAYNONLEAFSIZE);
                }
                break;
            }

            case DOUBLE: {
                PageId newPageIdDouble = 0;
                double newValueDouble;
                double key_ = *((double *) key);
                RIDKeyPair<double> ridKeyPairDouble;


                ridKeyPairDouble.set(rid, key_);
                insertEntry_<double, LeafNodeDouble, NonLeafNodeDouble> (0, DOUBLEARRAYLEAFSIZE, DOUBLEARRAYNONLEAFSIZE,
                                                                         newValueDouble, rootPageNum, newPageIdDouble,
                                                                         ridKeyPairDouble);

                // Check if root node is split
                if (newPageIdDouble != 0) {
                    //Since root node is more than full, create a new root
                    createNewRoot<double, NonLeafNodeDouble>(newValueDouble, newPageIdDouble, DOUBLEARRAYNONLEAFSIZE);
                }
                break;
            }

            case STRING: {
                PageId newPageIdString = 0;
                char newValue[STRINGSIZE];
                RIDKeyPair<char[STRINGSIZE]> ridKeyPairString;


                ridKeyPairString.rid = rid;
                strncpy(ridKeyPairString.key, (char *) key, STRINGSIZE);
                insertEntry_<char[STRINGSIZE], LeafNodeString, NonLeafNodeString> (0, STRINGARRAYLEAFSIZE,
                        STRINGARRAYNONLEAFSIZE, newValue, rootPageNum, newPageIdString, ridKeyPairString);

                // Check if root node is split
                if (newPageIdString != 0) {
                    //Since root node is more than full, create a new root
                    createNewRoot <char[STRINGSIZE], NonLeafNodeString> (newValue, newPageIdString, STRINGARRAYNONLEAFSIZE);
                }
                break;
            }

            default:
                return;
        }
    }

    template <class T>
    int BTreeIndex::compare (T a, T b) {
        if (a == b) {
            return 0;
        } else if (a > b) {
            return 1;
        }
        return -1;
    }

    template <class T>
    void BTreeIndex::copy(T& a, T& b){
        a = b;
    }

    template <>
    void BTreeIndex::copy<char[STRINGSIZE]>(char (&a)[STRINGSIZE], char (&b)[STRINGSIZE]){
        strncpy(a, b, STRINGSIZE);
    }


    void BTreeIndex::validateLowAndHighValues(const void *lowValParm, const void *highValParm) {
        bool isScanRangeValid = true;


        switch(attributeType) {
            case INTEGER:
                this->lowValInt = *(int*) lowValParm;
                this->highValInt = *(int*) highValParm;

                if (lowValInt > highValInt) {
                    isScanRangeValid = false;
                }
                break;

            case DOUBLE:
                this->highValDouble = *(double*) highValParm;
                this->lowValDouble = *(double*) lowValParm;

                if (lowValDouble > highValDouble) {
                    isScanRangeValid = false;
                }
                break;

            case STRING:
                this->lowValString = std::string((char*) lowValParm);
                this->highValString = std::string((char*) highValParm);

                if (strcmp(this->lowValString.c_str(), this->highValString.c_str()) > 0) {
                    isScanRangeValid = false;
                }
                break;

            default:
                break;
        }

        if (!isScanRangeValid) {
            throw BadScanrangeException();
        }
    }

    void BTreeIndex::validateOperators(const Operator &lowOpParm, const Operator &highOpParm) const {
        // Low operator should be only GT or GTE
        // High operator should be only LT or LTE
        if (lowOpParm == LT || lowOpParm == LTE || highOpParm == GT || highOpParm == GTE) {
            throw BadOpcodesException();
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
    const void BTreeIndex::startScan(const void* lowValParm,
                                     const Operator lowOpParm,
                                     const void* highValParm,
                                     const Operator highOpParm)
    {
        // Validate the low and high operators
        // TODO - check  if exception is thrown correctly
        validateOperators(lowOpParm, highOpParm);

        // Validate low and high values
        // TODO - check  if exception is thrown correctly
        validateLowAndHighValues(lowValParm, highValParm);

        this->scanExecuting = true;
        this->lowOp = lowOpParm;
        this->highOp = highOpParm;

        switch (attributeType) {
            case INTEGER: {
                this->lowValInt = *((int *) lowValParm);
                this->highValInt = *((int *) highValParm);
                findLeafPage<int, NonLeafNodeInt>(*((int *) lowValParm), INTARRAYNONLEAFSIZE);
                break;
            }

            case DOUBLE: {
                this->lowValDouble = *((double *) lowValParm);
                this->highValDouble = *((double *) highValParm);
                findLeafPage<double, NonLeafNodeDouble>(*((double *) lowValParm), DOUBLEARRAYNONLEAFSIZE);
                break;
            }

            case STRING: {
                strcpy((char*) this->lowValString.c_str(), (char *)lowValParm);
                strcpy((char*) this->highValString.c_str(), (char *)highValParm);

                //TODO - Remove this later
                strncpy(lowValChar, (char *)lowValParm, STRINGSIZE);
                strncpy(highValChar, (char *)highValParm, STRINGSIZE);

                findLeafPage<std::string, NonLeafNodeString>(this->lowValString, STRINGARRAYNONLEAFSIZE);
                break;
            }

            default:
                break;
        }

    }

    // TODO - Add comments
    template<class T1, class T2>
    void BTreeIndex::findLeafPage(T1 lowValParm, int arrayLength) {
        // Start traversing from root node to leaf node
        this->currentPageNum = this->rootPageNum;

        // Load the root page into buffer manager
        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
        T2* currentNonLeafNode = (T2*) currentPageData;

        int currIndex = 0;
        while(currentNonLeafNode->level != 1) {
            // If current level is not 1, then next page is not leaf page and we need to keep traversing.
            currIndex = 0;

            while (1) {
                // If current index position is greater than the number if entries, break
                if (currIndex >= arrayLength) {
                    break;
                }

                // If the next entry points to a page with number 0, break
                if (currentNonLeafNode->pageNoArray[currIndex + 1] == 0) {
                    break;
                }

                // If the value of the current key if greater than or equal to the the low value, break
                if (compare<T1>(lowValParm, currentNonLeafNode->keyArray[currIndex]) < 0) {
                    break;
                }

                currIndex++;
            }

            // Unpin the previously read page
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

            // Read page pointed by currIndex
            this->bufMgr->readPage(this->file, currentNonLeafNode->pageNoArray[currIndex], this->currentPageData);

            // Set the current page number to the page pointed by currIndex
            this->currentPageNum = currentNonLeafNode->pageNoArray[currIndex];
            currentNonLeafNode = (T2*) this->currentPageData;
        }

        // This page is level 1, which means next page is leaf node.
        currIndex = 0;

        while (1) {
            // If current index position is greater than the number if entries, break
            if (currIndex >= arrayLength) {
                //TODO - check for other conditions for key not being present in index
                throw NoSuchKeyFoundException();
                break;
            }

            // If the next entry points to a page with number 0, break
            if (currentNonLeafNode->pageNoArray[currIndex + 1] == 0) {
                break;
            }

            // If the value of the current key if greater than or equal to the the low value, break
            if (compare<T1> (lowValParm, currentNonLeafNode->keyArray[currIndex]) < 0) {
                break;
            }

            currIndex++;
        }

        nextEntry = 0;

        // Unpin the previously read page
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

        // Read page pointed by currIndex
        this->bufMgr->readPage(this->file, currentNonLeafNode->pageNoArray[currIndex], this->currentPageData);

        // Set the current page number to the page pointed by currIndex
        this->currentPageNum = currentNonLeafNode->pageNoArray[currIndex];
    }


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
    const void BTreeIndex::scanNext(RecordId& outRid)
    {
        if (!this->scanExecuting) {
            throw ScanNotInitializedException();
        }

        switch (this->attributeType) {
            case INTEGER: {
                while (1) {
                    LeafNodeInt *leafNode = (LeafNodeInt *) currentPageData;

                    if (this->nextEntry == INTARRAYLEAFSIZE || leafNode->ridArray[this->nextEntry].page_number == 0) {
                        if (leafNode->rightSibPageNo == 0) {
                            bufMgr->unPinPage(file, currentPageNum, false);
                            throw IndexScanCompletedException();
                        }
                        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

                        this->currentPageNum = leafNode->rightSibPageNo;
                        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                        nextEntry = 0;
                        continue;
                    }

                    if ((this->lowOp == GT && compare<int>(leafNode->keyArray[nextEntry], this->lowValInt) <= 0) ||
                        (lowOp == GTE && compare<int>(leafNode->keyArray[nextEntry], this->lowValInt) < 0)) {
                        nextEntry++;
                        continue;
                    }

                    if ((this->highOp == LT && compare<int>(leafNode->keyArray[nextEntry], this->highValInt) >= 0) ||
                        (this->highOp == LTE && compare<int>(leafNode->keyArray[nextEntry], this->highValInt) > 0)) {
                        throw IndexScanCompletedException();
                    }

                    outRid = leafNode->ridArray[nextEntry];
                    this->nextEntry++;
                    return;
                }
            }


            case DOUBLE: {
                while (1) {
                    LeafNodeDouble *leafNode = (LeafNodeDouble *) currentPageData;

                    if (this->nextEntry == DOUBLEARRAYLEAFSIZE || leafNode->ridArray[this->nextEntry].page_number == 0) {
                        if (leafNode->rightSibPageNo == 0) {
                            bufMgr->unPinPage(file, currentPageNum, false);
                            throw IndexScanCompletedException();
                        }
                        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

                        this->currentPageNum = leafNode->rightSibPageNo;
                        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                        nextEntry = 0;
                        continue;
                    }

                    if ((this->lowOp == GT && compare<double>(leafNode->keyArray[nextEntry], this->lowValDouble) <= 0) ||
                        (lowOp == GTE && compare<double>(leafNode->keyArray[nextEntry], this->lowValDouble) < 0)) {
                        nextEntry++;
                        continue;
                    }

                    if ((this->highOp == LT && compare<double>(leafNode->keyArray[nextEntry], this->highValDouble) >= 0) ||
                        (this->highOp == LTE && compare<double>(leafNode->keyArray[nextEntry], this->highValDouble) > 0)) {
                        throw IndexScanCompletedException();
                    }

                    outRid = leafNode->ridArray[nextEntry];
                    this->nextEntry++;
                    return;
                }
            }


            case STRING: {
                while (1) {
                    LeafNodeString *leafNode = (LeafNodeString *) currentPageData;

                    if (this->nextEntry == STRINGARRAYLEAFSIZE || leafNode->ridArray[this->nextEntry].page_number == 0) {
                        if (leafNode->rightSibPageNo == 0) {
                            bufMgr->unPinPage(file, currentPageNum, false);
                            throw IndexScanCompletedException();
                        }
                        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

                        this->currentPageNum = leafNode->rightSibPageNo;
                        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                        nextEntry = 0;
                        continue;
                    }

                    char *lowValKey = (char *) malloc(STRINGSIZE);
                    strcpy(lowValKey, this->lowValString.c_str());
                    std::string key = leafNode->keyArray[nextEntry];
                    //strcmp(leafNode->keyArray[nextEntry], this->lowValString.c_str()) <= 0)
                    if ((this->lowOp == GT && strncmp(leafNode->keyArray[nextEntry], this->lowValString.c_str(), STRINGSIZE) <= 0)||
                        (lowOp == GTE && strncmp(leafNode->keyArray[nextEntry], this->lowValString.c_str(), STRINGSIZE) < 0)) {
                        nextEntry++;
                        continue;
                    }

                    char *highValKey = (char *) malloc(STRINGSIZE);
                    strcpy(highValKey, this->highValString.c_str());
                    key = leafNode->keyArray[nextEntry];
                    if ((this->highOp == LT && strncmp(leafNode->keyArray[nextEntry], this->highValString.c_str(), STRINGSIZE) >= 0) ||
                        (this->highOp == LTE && strncmp(leafNode->keyArray[nextEntry], this->highValString.c_str(), STRINGSIZE) > 0)) {
                        throw IndexScanCompletedException();
                    }

                    outRid = leafNode->ridArray[nextEntry];
                    this->nextEntry++;
                    return;
                }
            }
        }
    }


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

        const void BTreeIndex::endScan()
        {

            //Check if a scan is actually started and running

            if (!this->scanExecuting) {
                throw ScanNotInitializedException();
            }

            this->scanExecuting = false;

            try {
                // Unpin the page that is currently being scanned
                this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
            } catch (PageNotPinnedException e) {

            }

        }
    }
