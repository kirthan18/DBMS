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
        clockHand = (clockHand + 1) % numBufs;
    }

    void BufMgr::allocBuf(FrameId &frame) {

        bool all_pages_pinned = true;
        uint32_t i = 0;

        while (i <= numBufs) {

            if (i >= numBufs && all_pages_pinned) {
                throw BufferExceededException();
            }
            //Advance clock pointer
            advanceClock();
            i++;

            if (!bufDescTable[clockHand].valid) {
                frame = clockHand;
                bufDescTable[clockHand].Clear();
                return;
            } else if (bufDescTable[clockHand].refbit) {
                //If ref bit is set, set it to false and move on
                bufDescTable[clockHand].refbit = false;
                continue;
            } else if (bufDescTable[clockHand].pinCnt != 0) {
                //If page is pinned, move on
                all_pages_pinned = false;
                continue;
            } else {
                //If page is not pinned, check if dirty bit is set
                if (bufDescTable[clockHand].dirty) {
                    //If dirty bit is set, flush disk to page
                    bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                    bufDescTable[clockHand].dirty = false;
                }
                try {
                    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                } catch (HashNotFoundException e) {
                    return;
                }
                frame = clockHand;
                bufDescTable[clockHand].Clear();
                return;
            }
        }
    }


    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frameNumber;
        try{
            hashTable->lookup(file, pageNo, frameNumber);

            //Page is in the buffer pool frame
            bufDescTable[frameNumber].refbit = true;
            bufDescTable[frameNumber].pinCnt++;
            page = &bufPool[frameNumber];
        } catch(HashNotFoundException e){
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
            bufDescTable[frameNumber].Set(file, pageNo);
            //Return pointer to frame containing the page via the page parameter
            page = &bufPool[frameNumber];
        }
    }


    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        FrameId frameId;

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
                        //TODO - check this
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
            } else if (bufDescTable[i].file == file) {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid,
                                         bufDescTable[i].refbit);
            }
        }
    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        FrameId frameId = 0;
        Page allocated_page;
        try {
            allocated_page = file->allocatePage();
        } catch (InvalidPageException e) {
            return;
        }

        pageNo = page->page_number();

        try {
            allocBuf(frameId);
        } catch (BufferExceededException e) {
            return;
        }
        bufPool[pageNo] = allocated_page;
        try {
            hashTable->insert(file, pageNo, frameId);
        } catch (HashTableException e) {
            return;
        } catch (HashAlreadyPresentException hashAlreadyPresentException) {
            return;
        }

        bufDescTable[frameId].Set(file, pageNo);
        page = &bufPool[frameId];
    }

    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId frameId = 0;

        hashTable->lookup(file, PageNo, frameId);
        bufDescTable[frameId].Clear();
        try {
            hashTable->remove(file, PageNo);
        } catch (HashNotFoundException e) {
            return;
        }


        file->deletePage(PageNo);
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
