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
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_table_exception.h"
#include "exceptions/invalid_page_exception.h"

namespace badgerdb {

    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }


    BufMgr::~BufMgr() {
    }

    void BufMgr::advanceClock() {
        if (clockHand >= 0 && clockHand < numBufs) {
            bufDescTable[clockHand].refbit = true;
            clockHand++;
        }
    }

    void BufMgr::allocBuf(FrameId &frame) {
        
    }


    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        //TODO - Check if FrameId can be 0
        FrameId frameNumber = 0;
        if (hashTable->lookup(file, pageNo, frameNumber)) {
            //Page is in the buffer pool frame
            bufDescTable[frameNumber].refbit = 1;
            bufDescTable[frameNumber].pinCnt++;
            *page = bufPool[frameNumber];
        } else {
            //Page not found in buffer pool
            //Call allocBuf() to allocate a buffer frame
            try {
                allocBuf(frameNumber);
            } catch (BufferExceededException e) {
                return;
            }
            //Call file->readPage() to read the page from file into the buffer pool frame
            try {
                bufPool[frameNumber] = file->readPage(pageNo);
            } catch (InvalidPageException e) {
                return;
            }
            //Insert the page into the hash table
            try {
                hashTable->insert(file, pageNo, frameNumber);
            } catch (HashAlreadyPresentException e) {
                return;
            } catch (HashTableException hashTableException) {
                return;
            }
            //Invoke Set()
            bufDescTable->Set(file, pageNo);
            //Return pointer to frame containing the page via the page parameter
            bufPool[frameNumber] = *page;
        }
    }


    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        //TODO - Check if FrameId can be 0
        FrameId frameId = 0;

        try {
            hashTable->lookup(file, pageNo, frameId);
        } catch (HashNotFoundException e) {
            return;
        }

        if (frameId <= 0 || frameId > numBufs) {
            if (bufDescTable[frameId].pinCnt == 0) {
                throw PageNotPinnedException(file->filename(), pageNo, frameId);
            } else {
                bufDescTable[frameId].pinCnt--;

                if (dirty) {
                    bufDescTable[frameId].dirty = true;
                } else {
                    bufDescTable[frameId].dirty = false;
                }
            }
        }
    }

    void BufMgr::flushFile(const File *file) {
        for (uint32_t i = 0; i < numBufs; i++) {
            if (bufDescTable[i].valid) {
                if (bufDescTable[i].file == file) {
                    if (bufDescTable[i].pinCnt > 0) {
                        throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
                    }
                    if (bufDescTable[i].dirty) {
                        bufDescTable[i].file->writePage(bufPool[i]);
                        bufDescTable[i].dirty = false;
                    }
                    try {
                        hashTable->remove(file, bufDescTable[i].pageNo);
                    } catch (HashNotFoundException e) {
                        return;
                    }
                    bufDescTable[i].Clear();
                }
            } else {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid,
                                         bufDescTable[i].refbit);
            }
        }
    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        FrameId frameId = 0;
        *page = file->allocatePage();
        pageNo = page->page_number();
        try {
            allocBuf(frameId);
        } catch (BufferExceededException e) {
            return;
        }
        //TODO - CHeck if it returns a valid frame id?
        try {
            hashTable->insert(file, pageNo, frameId);
        } catch (HashTableException e) {
            return;
        } catch (HashAlreadyPresentException hashAlreadyPresentException) {
            return;
        }

        bufDescTable->Set(file, pageNo);
    }

    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId frameId = 0;

        hashTable->lookup(file, PageNo, frameId);
        if (frameId <= 0 || frameId > numBufs) {
            delete (bufDescTable + frameId);
            try {
                hashTable->remove(file, PageNo);
            } catch (HashNotFoundException e) {
                return;
            }


            file->deletePage(PageNo);
        }
    }

    void BufMgr::printSelf(void) {
        BufDesc *tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
