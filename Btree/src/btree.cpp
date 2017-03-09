/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "exceptions/bad_index_info_exception.h"
#include "filescan.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/index_scan_completed_exception.h"

//#define DEBUG

namespace badgerdb
{

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
    this->scanExecuting = false;
    this->leafOccupancy = 0;
    this->nodeOccupancy = 0;

    // Obtaining index file name from relation and attribute byte offset
    std::ostringstream	idxStr;
    idxStr	<<	relationName	<<	'.'	<<	attrByteOffset;
    std::string	indexName	=	idxStr.str();	//	indexName	is	the	name	of	the	index	file

    if (BlobFile::exists(indexName)) {
        // Index file exists!
        // Open the file
        Page *indexPage;
        Page *rootPage;

        BlobFile blobFile = BlobFile::open(indexName);
        this->file = &blobFile;
        this->headerPageNum = blobFile.getFirstPageNo();
        this->attributeType = attrType;
        this->attrByteOffset = attrByteOffset;
        this->bufMgr = bufMgrIn;

        bufMgrIn->readPage(&blobFile, headerPageNum, indexPage);

        // Read the first page from the file (meta node)
        PageId  indexPageNo = blobFile.getFirstPageNo();
        bufMgrIn->readPage(&blobFile, indexPageNo, indexPage);
        IndexMetaInfo* indexMetaInfo = (IndexMetaInfo*) indexPage;

        // Check for bad index
        std::string reason = "";
        if (indexMetaInfo->attrByteOffset != attrByteOffset) {
            reason = "Attribute Byte offset doesn't match! \n";
        }
        if (indexMetaInfo->attrType != attrType) {
            reason += "Attribute data type doesn't match! \n";
        }

        if (strcmp(indexMetaInfo->relationName,indexName.c_str()) != 0) {
            reason += "Relation does not match! \n";
        }
        if (!reason.empty()) {
            throw BadIndexInfoException(reason);
        }

        // Get Root page number from the meta node
        this->rootPageNum = indexMetaInfo->rootPageNo;

        // Read the root page
        bufMgrIn->readPage(&blobFile, rootPageNum, rootPage);

        // Unpin the header page
        bufMgrIn->unPinPage(&blobFile, headerPageNum, false);

    } else {
        // Index file doesn't exist!

        // Root page starts at 2
        PageId rootPageNum = 2;

        // Create a blob file
        BlobFile blobFile = BlobFile::create(indexName);

        // Allocate new meta page
        Page *indexMetaPage;
        PageId headerPageID = 1;
        bufMgrIn->allocPage(&blobFile, headerPageID, indexMetaPage);

        // Populate IndexMetaInfo object with root page number
        IndexMetaInfo *indexMetaInfo =  new IndexMetaInfo();
        strcpy(indexMetaInfo->relationName, indexName.c_str());
        indexMetaInfo->attrType = attrType;
        indexMetaInfo->attrByteOffset = attrByteOffset;
        indexMetaInfo->rootPageNo = rootPageNum;

        // TODO - Possible error - check
        memcpy(indexMetaPage, indexMetaInfo, sizeof(IndexMetaInfo));

        // Allocate new root page
        Page *rootPage;
        bufMgrIn->allocPage(&blobFile, rootPageNum, rootPage);

        // TODO - Check if this is needed!
        this->file = &blobFile;
        this->rootPageNum = rootPageNum;
        this->headerPageNum = headerPageID;
        this->attrByteOffset = attrByteOffset;
        this->attributeType = attrType;
        this->bufMgr = bufMgrIn;

        switch (attrType) {
            case INTEGER:
            {
                NonLeafNodeInt *nonLeafNodeInt = (NonLeafNodeInt *) rootPage;
                for(int i=0;i<INTARRAYNONLEAFSIZE;i++) {
                    nonLeafNodeInt->pageNoArray[i] = 0;
                }
                nonLeafNodeInt->level = 1;
                break;
            }

            case DOUBLE:
            {
                NonLeafNodeDouble *nonLeafNodeDouble = (NonLeafNodeDouble *) rootPage;
                for(int i=0;i<DOUBLEARRAYNONLEAFSIZE;i++) {
                    nonLeafNodeDouble->pageNoArray[i] = 0;
                }
                nonLeafNodeDouble->level = 1;
                break;
            }

            case STRING:
            {
                NonLeafNodeString *nonLeafNodeString = (NonLeafNodeString *) rootPage;
                for(int i=0;i<STRINGARRAYNONLEAFSIZE;i++) {
                    nonLeafNodeString->pageNoArray[i] = 0;
                }
                nonLeafNodeString->level = 1;
                break;
            }

        }
        bufMgrIn->unPinPage(&blobFile, headerPageNum, true);


        //TODO - Scan record and insert into B tree

        FileScan *fileScan = new FileScan(relationName, this->bufMgr);

        try {
            while (1) {
                // Start scanning the file, get each record and insert it

                RecordId recordId;
                scanNext(recordId);

                std::string record = fileScan->getRecord();
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

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

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
            findLeafPage<char[STRINGSIZE], NonLeafNodeString>((char *) lowValParm, STRINGARRAYNONLEAFSIZE);
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
            if (compare<T1>(lowValParm, currentNonLeafNode->keyArray[currIndex]) >= 0) {
                break;
            }

            currIndex++;
        }
        /*while(!compareKeys<T1> (lowValParm, currentNonLeafNode->keyArray[currIndex])
              && currentNonLeafNode->pageNoArray[currIndex + 1] != 0
              && currIndex < arrayLength)
            currIndex++; */

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
            if (compare<T1> (lowValParm, currentNonLeafNode->keyArray[currIndex]) >= 0) {
                break;
            }

            currIndex++;
        }
    /*while(!compare<T1> (lowValParm, currentNonLeafNode->keyArray[currIndex])
          && currentNonLeafNode->pageNoArray[currIndex + 1] != 0
          && currIndex < arrayLength)
        currIndex++;*/

    PageId nextPageId = currentNonLeafNode->pageNoArray[currIndex];
    this->bufMgr->readPage(this->file, nextPageId, this->currentPageData);
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    this->currentPageNum = nextPageId;
    nextEntry = 0;
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

template <>
int BTreeIndex::compare<char*>(char* string1, char* string2) {
    return strcmp(string1, string2);
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
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid) {
    if (!this->scanExecuting) {
        throw ScanNotInitializedException();
    }

    if (this->currentPageNum == 0) {
        throw IndexScanCompletedException();
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
            break;
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
            break;
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

                char *lowValKey = (char*) malloc(STRINGSIZE);
                strcpy(lowValKey, this->lowValString.c_str());
                if ((this->lowOp == GT && compare(leafNode->keyArray[nextEntry], lowValKey) <= 0) ||
                    (lowOp == GTE && compare(leafNode->keyArray[nextEntry], lowValKey) < 0)) {
                    nextEntry++;
                    continue;
                }

                char *highValKey = (char*) malloc(STRINGSIZE);
                strcpy(highValKey, this->highValString.c_str());
                if ((this->highOp == LT && compare(leafNode->keyArray[nextEntry], highValKey) >= 0) ||
                    (this->highOp == LTE && compare(leafNode->keyArray[nextEntry], highValKey) > 0)) {
                    throw IndexScanCompletedException();
                }

                outRid = leafNode->ridArray[nextEntry];
                this->nextEntry++;
                return;
            }
            break;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
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
