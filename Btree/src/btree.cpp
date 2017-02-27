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
        std::strcpy(indexMetaInfo->relationName, indexName.c_str());
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

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
