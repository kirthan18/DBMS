
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
// copy b to a
    template <class T>
    void copy(T& a, T& b){
        a = b;
    }

// specialiazation char[STRINGSIZE] copy
    template <>
    void copy<char[STRINGSIZE]>(char (&a)[STRINGSIZE], char (&b)[STRINGSIZE]){
        strncpy( a, b, STRINGSIZE);
        //a[STRINGSIZE - 1] = '\0';
    }

// Type T small than comparison
    template <class T>
    bool smallerThan(T a, T b){
        return a<b;
    }

// specialiazation char[STRINGSIZE] smaller comparison
    template <>
    bool smallerThan<char[STRINGSIZE]>(char a[STRINGSIZE], char b[STRINGSIZE]){
        return strncmp(a, b, STRINGSIZE) < 0;
    }

//  Type T bigger comparison
    template <class T>
    bool biggerThan(T a, T b){
        return a>b;
    }

// specialization char[STRINGSIZE] bigger comparison
    template <>
    bool biggerThan<char[STRINGSIZE]>(char a[STRINGSIZE], char b[STRINGSIZE]){
        return strncmp(a, b, STRINGSIZE) > 0;
    }

// Type T equal comparison
    template <class T>
    bool equalTo(T a, T b){
        return a==b;
    }

// specialization char[STRINGSIZE] equal to comparison
    template <>
    bool equalTo<char[STRINGSIZE]>(char a[STRINGSIZE], char b[STRINGSIZE]){
        return strncmp(a, b, STRINGSIZE) == 0;
    }


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
/*
        this->leafOccupancy = 0;
        this->nodeOccupancy = 0;*/

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


/*
 * this helper help the leaf to split and return newPageId and new Value to parent
 * */
    template <class T, class T1>
    void BTreeIndex::leafSplitHelper(int pos, int last, int LEAFARRAYMAX,
                                     int NONLEAFARRAYMAX,
                                     RIDKeyPair<T> ridKeyPair,
                                     T1* leafNode,
                                     PageId& newPageId,
                                     T & newValue)
    {
        //full
        Page* newPage;
        bufMgr->allocPage(file, newPageId, newPage);
        T1 *newLeafNode = (T1 *) newPage;

        //tmp array
        T tmpKeyArray[LEAFARRAYMAX + 1];
        RecordId tmpRidArray[LEAFARRAYMAX + 1];

        //copy all new records to tmp
        for(int i=0; i < LEAFARRAYMAX; i++) {
            tmpRidArray[i] = leafNode->ridArray[i];
            copy<T> (tmpKeyArray[i], leafNode->keyArray[i]);
            leafNode->ridArray[i].page_number = 0;
        }

        for(int i= LEAFARRAYMAX; i > pos; i--){
            copy<T> (tmpKeyArray[i], tmpKeyArray[i-1]);
            tmpRidArray[i] = tmpRidArray[i-1];
        }
        copy<T> (tmpKeyArray[pos], ridKeyPair.key);
        tmpRidArray[pos] = ridKeyPair.rid;

        //reset new and old page
        for(int i=0; i < LEAFARRAYMAX; i++) {
            leafNode->ridArray[i].page_number = 0;
            newLeafNode->ridArray[i].page_number = 0;
        }

        //copy back
        for(int i=0; i< (LEAFARRAYMAX + 1) / 2; i++ ){
            copy<T> (leafNode->keyArray[i], tmpKeyArray[i]);
            leafNode->ridArray[i] = tmpRidArray[i];
        }
        for(int i= (LEAFARRAYMAX + 1) / 2; i < LEAFARRAYMAX + 1; i++){
            copy<T> (newLeafNode->keyArray[i - (LEAFARRAYMAX + 1) / 2], tmpKeyArray[i]);
            newLeafNode->ridArray[i - (LEAFARRAYMAX + 1) / 2] = tmpRidArray[i];
        }

        //link leaf node
        newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = newPageId;

        //push up
        copy<T> (newValue, newLeafNode->keyArray[0]);

        //unpin
        bufMgr->unPinPage(file, newPageId, true);
    };



/*
 * When the non Leaf need to split it call this nonLeafSplit Helper
 * */
    template <class T, class T2>
    void  BTreeIndex::nonLeafSplitHelper(int pos,
                                         int NONLEAFARRAYMAX,
                                         T2* nonLeafNode,
                                         PageId& newPageId,
                                         T & newValue,
                                         T&  newChildValue,
                                         PageId newChildPageId){
        //full, need split
        Page* newPage;
        bufMgr->allocPage(file, newPageId, newPage);
        T2* newNonLeafNode = (T2*) newPage;

        //tmp array
        T tmpKeyArray[NONLEAFARRAYMAX + 1];
        PageId tmpPageIdArray[NONLEAFARRAYMAX + 2];

        //copy to tmp
        for(int i=0; i < NONLEAFARRAYMAX; i++) {
            tmpPageIdArray[i] = nonLeafNode->pageNoArray[i];
            copy<T> (tmpKeyArray[i], nonLeafNode->keyArray[i]);
        }
        tmpPageIdArray[NONLEAFARRAYMAX + 1] = nonLeafNode->pageNoArray[NONLEAFARRAYMAX + 1];

        for(int i= NONLEAFARRAYMAX; i > pos; i--){
            copy<T> (tmpKeyArray[i], tmpKeyArray[i-1]);
            tmpPageIdArray[i+1] = tmpPageIdArray[i];
        }
        copy<T> (tmpKeyArray[pos], newChildValue);
        tmpPageIdArray[pos+1] = newChildPageId;

        //clear old and new page
        for(int i=0; i < NONLEAFARRAYMAX + 1; i++){
            nonLeafNode->pageNoArray[i] = 0;
            newNonLeafNode->pageNoArray[i] = 0;
        }

        //copy back
        for(int i=0; i< (NONLEAFARRAYMAX + 1) / 2; i++ ){
            copy<T> (nonLeafNode->keyArray[i], tmpKeyArray[i]);
            nonLeafNode->pageNoArray[i] = tmpPageIdArray[i];
        }
        nonLeafNode->pageNoArray[(NONLEAFARRAYMAX + 1) / 2] = tmpPageIdArray[(NONLEAFARRAYMAX + 1) / 2];

        for(int i= (NONLEAFARRAYMAX + 1) / 2 + 1; i < NONLEAFARRAYMAX + 1; i++){
            copy<T> (newNonLeafNode->keyArray[i - (NONLEAFARRAYMAX + 1) / 2 - 1], tmpKeyArray[i]);
            newNonLeafNode->pageNoArray[i - (NONLEAFARRAYMAX + 1) / 2 - 1 ] = tmpPageIdArray[i];
        }
        newNonLeafNode->pageNoArray[NONLEAFARRAYMAX + 1 - (NONLEAFARRAYMAX + 1) / 2 - 1 ] = tmpPageIdArray[
                NONLEAFARRAYMAX + 1];

        //level
        newNonLeafNode->level = nonLeafNode->level;

        //push up
        copy<T> (newValue, tmpKeyArray[(NONLEAFARRAYMAX + 1) / 2]);

        //unpin
        bufMgr->unPinPage(file, newPageId, true);
    };


/*
 * Helper function when we need create a new page.
 * This function can only be called once,
 * when the index file is empty and we need to create first leaf node.
 * */
    template <class T, class T1, class T2>
    void BTreeIndex::createFirstLeaf(int LEAFARRAYMAX,
                                     RIDKeyPair<T> ridKeyPair,
                                     T2* nonLeafNode,
                                     PageId pageId)
    {
        PageId newPageId;
        Page* page;
        bufMgr->allocPage(file, newPageId, page);

        T1* leafNode = (T1*) page;
        for(int i=0; i < LEAFARRAYMAX; i++)
            leafNode->ridArray[i].page_number = 0;

        copy<T> (leafNode->keyArray[0], ridKeyPair.key);
        leafNode->ridArray[0] = ridKeyPair.rid;
        leafNode->rightSibPageNo = 0;
        nonLeafNode->pageNoArray[0] = newPageId;

        //unpin
        bufMgr->unPinPage(file, newPageId, true);
        bufMgr->unPinPage(file, pageId, true);

    };



/*
 * This function will search a position for a new entry.
 * If current page is a non-leaf page, it will find a position and call insertEntryRecursive.
 * If current page is leaf page, it will try to insert new entry.
 * */
    template <class T, class T1, class T2>
    void BTreeIndex::insertEntryRecursive(RIDKeyPair<T> ridKeyPair,
                                          PageId pageId,
                                          bool isLeaf,
                                          int LEAFARRAYMAX,
                                          int NONLEAFARRAYMAX,
                                          T & newValue,
                                          PageId& newPageId)
    {
        if(isLeaf){
            // Read the page
            Page* page;
            bufMgr->readPage(file, pageId, page);
            T1 *leafNode = (T1 *) page;

            // Find position
            int pos = 0;
            while(biggerThan<T> (ridKeyPair.key, leafNode->keyArray[pos])
                  && leafNode->ridArray[pos].page_number != 0
                  && pos < LEAFARRAYMAX)
                pos++;

            // Find last entry
            int last;
            for (last =0; last < LEAFARRAYMAX; last++)
                if(leafNode->ridArray[last].page_number == 0)
                    break;


            if(last < LEAFARRAYMAX){
                // Not full
                for(int i=last; i>pos; i--){
                    copy<T> (leafNode->keyArray[i], leafNode->keyArray[i - 1]);
                    leafNode->ridArray[i] = leafNode->ridArray[i - 1];
                }
                copy<T> (leafNode->keyArray[pos], ridKeyPair.key);
                leafNode->ridArray[pos] = ridKeyPair.rid;
            } else {
                // Full, call split helper.
                leafSplitHelper<T,T1>(pos, last, LEAFARRAYMAX,
                                      NONLEAFARRAYMAX, ridKeyPair,leafNode,newPageId,newValue);
            }

            leafOccupancy++;
            //unpin
            bufMgr->unPinPage(file, pageId, true);

        } else {
            // Non leaf
            // Read page
            Page* page;
            bufMgr->readPage(file, pageId, page);
            T2* nonLeafNode = (T2*) page;

            // Find pageArray position.
            int pos = 0;

            while(!smallerThan<T> (ridKeyPair.key, nonLeafNode->keyArray[pos])
                  && nonLeafNode->pageNoArray[pos + 1] != 0
                  && pos < NONLEAFARRAYMAX)
                pos++;

            // Index file is empty.
            if(nonLeafNode->pageNoArray[pos] == 0){
                createFirstLeaf<T, T1, T2>(LEAFARRAYMAX,
                                           ridKeyPair,
                                           nonLeafNode,
                                           pageId);
                return;
            }

            // Call recursive function.
            bufMgr->unPinPage(file, pageId, false);
            T newChildValue;
            PageId newChildPageId = 0;
            insertEntryRecursive<T, T1, T2>(ridKeyPair, nonLeafNode->pageNoArray[pos], nonLeafNode->level == 1,
                                            LEAFARRAYMAX, NONLEAFARRAYMAX, newChildValue, newChildPageId);

            // Check if child split.
            if(newChildPageId != 0){
                // If child split.
                bufMgr->readPage(file, pageId, page);
                T2* nonLeafNode = (T2*) page;

                // Find last entry.
                int last;
                for (last = 0; last < NONLEAFARRAYMAX; last++)
                    if(nonLeafNode->pageNoArray[last + 1] == 0)
                        break;

                // Check if full.
                if(last < NONLEAFARRAYMAX){
                    // Not full, just insert.
                    for(int i=last; i>pos; i--){
                        copy<T> (nonLeafNode->keyArray[i], nonLeafNode->keyArray[i - 1]);
                        nonLeafNode->pageNoArray[i + 1] = nonLeafNode->pageNoArray[i];
                    }
                    copy<T> (nonLeafNode->keyArray[pos], newChildValue);
                    nonLeafNode->pageNoArray[pos + 1] = newChildPageId;
                }
                else{
                    // Full. Call split helper.
                    nonLeafSplitHelper<T, T2>(pos, NONLEAFARRAYMAX, nonLeafNode,newPageId,newValue,
                                              newChildValue,newChildPageId);
                }

                nodeOccupancy++;
                bufMgr->unPinPage(file, pageId, true);
            }
        }
    }

/*
 * This function is called when root node got split.
 * We need to create a new root page and link it to the old root page and new page.
 * */
    template<class T, class T1>
    void BTreeIndex::handleNewRoot(T& newValue, PageId newPageId, int ARRAYMAX){
        PageId newRootPageId;
        Page *newRootPage;
        bufMgr->allocPage(file, newRootPageId, newRootPage);

        T1 *newRootNonLeafNode = (T1 *) newRootPage;
        for(int i=0;i<ARRAYMAX + 1; i++) newRootNonLeafNode->pageNoArray[i] = 0;
        copy<T> (newRootNonLeafNode->keyArray[0], newValue);
        newRootNonLeafNode->pageNoArray[0] = rootPageNum;
        newRootNonLeafNode->pageNoArray[1] = newPageId;
        newRootNonLeafNode->level = 0;
        rootPageNum = newRootPageId;
        bufMgr->unPinPage(file, newRootPageId, true);
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/*
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 * */
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {

        if(attributeType == INTEGER) {
            RIDKeyPair<int> ridKeyPairInt;
            ridKeyPairInt.set(rid, *((int *) key));
            int newValueInt;
            PageId newPageIdInt = 0;

            //call recursive function
            insertEntryRecursive<int, LeafNodeInt, NonLeafNodeInt>
                    (ridKeyPairInt, rootPageNum, 0, INTARRAYLEAFSIZE, INTARRAYNONLEAFSIZE, newValueInt, newPageIdInt);

            //if root got split
            if (newPageIdInt != 0)
                handleNewRoot<int, NonLeafNodeInt>(newValueInt, newPageIdInt, INTARRAYNONLEAFSIZE);
        } else if (attributeType == DOUBLE) {
            RIDKeyPair<double> ridKeyPairDouble;
            ridKeyPairDouble.set(rid, *((double *) key));
            double newValueDouble;
            PageId newPageIdDouble = 0;

            //call recursive function
            insertEntryRecursive<double, LeafNodeDouble, NonLeafNodeDouble>
                    (ridKeyPairDouble, rootPageNum, 0, DOUBLEARRAYLEAFSIZE, DOUBLEARRAYNONLEAFSIZE, newValueDouble, newPageIdDouble);

            //if root got split
            if (newPageIdDouble != 0)
                handleNewRoot<double, NonLeafNodeDouble>(newValueDouble, newPageIdDouble, DOUBLEARRAYNONLEAFSIZE);
        } else if (attributeType == STRING){
            RIDKeyPair<char[STRINGSIZE] > ridKeyPairString;
            ridKeyPairString.rid = rid;
            strncpy(ridKeyPairString.key, (char*) key, STRINGSIZE);
            char newValue[STRINGSIZE];
            PageId newPageId = 0;

            //call recursive function
            insertEntryRecursive<char[STRINGSIZE], LeafNodeString, NonLeafNodeString>
                    (ridKeyPairString, rootPageNum, 0, STRINGARRAYLEAFSIZE, STRINGARRAYNONLEAFSIZE, newValue, newPageId);

            //if root got split
            if (newPageId != 0)
                handleNewRoot<char[STRINGSIZE], NonLeafNodeString>(newValue, newPageId, STRINGARRAYNONLEAFSIZE);
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

            PageId nextPageId = currentNonLeafNode->pageNoArray[currIndex];
            this->bufMgr->readPage(this->file, nextPageId, this->currentPageData);
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
            this->currentPageNum = nextPageId;
            currentNonLeafNode = (T2*) this->currentPageData;
        }

        // This page is level 1, which means next page is leaf node.
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
            if (compare<T1> (lowValParm, currentNonLeafNode->keyArray[currIndex]) < 0) {
                break;
            }

            currIndex++;
        }

        PageId nextPageId = currentNonLeafNode->pageNoArray[currIndex];
        this->bufMgr->readPage(this->file, nextPageId, this->currentPageData);
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        this->currentPageNum = nextPageId;
        nextEntry = 0;
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
