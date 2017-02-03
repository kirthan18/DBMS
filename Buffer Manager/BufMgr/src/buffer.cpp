/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

void BufMgr::advanceClock()
{
	if (clockHand >= 0 && clockHand < numBufs) {
		bufDescTable[clockHand].refbit = true;
		clockHand++;
	}
}

void BufMgr::allocBuf(FrameId & frame)
{

}


void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	//TODO - Check if FrameId can be 0
	FrameId frameNumber = 0;
	hashTable->lookup(file, pageNo, frameNumber);

	if (frameNumber <= 0 || frameNumber > numBufs) {
		//Page not found in buffer pool
		//Call allocBuf() to allocate a buffer frame
		//Call file->readPage() to read the page from file into the buffer pool frame
		//Insert the page into the hash table
		//Invoke Set()
		//Return pointer to frame containing the page via the page parameter
	} else {
		//Page is in the buffer pool frame

	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	//TODO - Check if FrameId can be 0
	FrameId  frameId = 0;

	hashTable->lookup(file, pageNo, frameId);
	if (frameId <=0 || frameId > numBufs) {
        if (bufDescTable[frameId].pinCnt == 0) {
            throw PageNotPinnedException(file->filename(), pageNo, frameId);
        } else {
            bufDescTable[frameId].pinCnt--;

            //TODO - check how to set dirty parameter
            /*if (bufDescTable[frameId].dirty) {
                dirty = true;
            } else {
                dirty = false;
            }*/
        }
    }
}

void BufMgr::flushFile(const File* file)
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
    FrameId frameId = 0;
    *page = file->allocatePage();
    pageNo = page->page_number();
    allocBuf(frameId);
    hashTable->insert(file, pageNo,frameId);
    bufDescTable->Set(file, pageNo);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{

}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
