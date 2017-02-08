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
        for (uint32_t i = 0; i < numBufs; i++) {
            if (bufDescTable[i].dirty) {
                flushFile(bufDescTable[i].file);
            }
        }

        delete[] bufDescTable;
        delete[] bufPool;
    }

    void BufMgr::advanceClock() {
        clockHand = (clockHand + 1) % numBufs;
    }

    void BufMgr::allocBuf(FrameId &frame) {
        //printSelf();
        bool all_pages_pinned = true;
        uint32_t i = 0;
        std::cout<< "Clock hand : " << clockHand <<std::endl;
        while (i < 2 * numBufs) {

            if (i >= 2 * numBufs && all_pages_pinned) {
                throw BufferExceededException();
            }
            //Advance clock pointer
            advanceClock();
            std::cout<< "Clock hand after advancing: " << clockHand <<std::endl;
            i++;

            if (!bufDescTable[clockHand].valid) {
                std::cout<< "Frame " << clockHand << " is invalid." << std::endl;
                frame = clockHand;
                bufDescTable[clockHand].Clear();
                return;
            }

            if (bufDescTable[clockHand].refbit) {
                //If ref bit is set, set it to false and move on
                std::cout << "Ref bit is set in frame " << clockHand << ". Setting it to false." << std::endl;
                bufDescTable[clockHand].refbit = false;
            } else {
                std::cout << "Ref bit is NOT set in frame " << clockHand << "." << std::endl;
                if (bufDescTable[clockHand].pinCnt != 0) {
                    //If page is pinned, move on
                    std::cout << "Pin count of frame " << clockHand << " is " << bufDescTable[clockHand].pinCnt << std::endl;
                    all_pages_pinned = false;
                } else {
                    //If page is not pinned, check if dirty bit is set
                    std::cout << "No pages pinned in frame " << clockHand << " ." << std::endl;
                    if (bufDescTable[clockHand].dirty) {
                        //If dirty bit is set, flush disk to page
                        std::cout << "Dirty bit is set in frame " << clockHand << std::endl;
                        std::cout << "Wrtiting dirty page in frame " << clockHand << " to disk." << std::endl;
                        bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                        bufDescTable[clockHand].dirty = false;
                    }
                    std::cout << "Removing hash table entry.." << std::endl;
                    try {
                        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                    } catch (HashNotFoundException e) {
                        return;
                    }
                    frame = clockHand;
                    bufDescTable[clockHand].Clear();
                    std::cout<<"Frame number being set to " << clockHand << std::endl;
                    return;
                }
            }
        }
    }


    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frameNumber;
        std::cout << "Looking for file " << file->filename() << " and page number " << pageNo << std::endl;

        if (hashTable->lookup(file, pageNo, frameNumber)) {
            //Page is in the buffer pool frame

            std::cout <<"Page is in the buffer pool frame number " << frameNumber<< std::endl;
            std::cout <<"Page number : " << bufDescTable[frameNumber].pageNo <<std::endl;
            std::cout <<"File name : " << bufDescTable[frameNumber].file->filename() <<std::endl;
            bufDescTable[frameNumber].refbit = true;
            bufDescTable[frameNumber].pinCnt++;
            page = &bufPool[frameNumber];

        } else {
            //Page not found in buffer pool
            std::cout <<"Page is NOT in the buffer pool frame" << std::endl;
            //Call allocBuf() to allocate a buffer frame
            try {
                allocBuf(frameNumber);
            } catch (BufferExceededException e) {
                std::cout << e.message();
                return;
            }

            std::cout << "Allocated buffer frame : " << frameNumber;
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
        /*try {
            hashTable->lookup(file, pageNo, frameNumber);

            //Page is in the buffer pool frame
            bufDescTable[frameNumber].refbit = true;
            bufDescTable[frameNumber].pinCnt++;
            page = &bufPool[frameNumber];
        } catch (HashNotFoundException e) {
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
        }*/

    }


    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        FrameId frameId;

        try {
            hashTable->lookup(file, pageNo, frameId);
        } catch (HashNotFoundException e) {
            return;
        }


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

    void BufMgr::flushFile(const File *file) {
        for (uint32_t i = 0; i < numBufs; i++) {
            if (bufDescTable[i].valid) {
                if (bufDescTable[i].file == file) {
                    if (bufDescTable[i].pinCnt > 0) {
                        throw PagePinnedException(file->filename(), bufDescTable[i].pageNo,
                                                  bufDescTable[i].frameNo);
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

        try {
            allocBuf(frameId);
        } catch (BufferExceededException e) {
            return;
        }
        bufPool[frameId] = allocated_page;
        try {
            hashTable->insert(file, bufPool[frameId].page_number(), frameId);
        } catch (HashTableException e) {
            return;
        } catch (HashAlreadyPresentException hashAlreadyPresentException) {
            return;
        }

        bufDescTable[frameId].Set(file, bufPool[frameId].page_number());
        page = &bufPool[frameId];
        pageNo = bufPool[frameId].page_number();
    }

    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId frameId;

        if (hashTable->lookup(file, PageNo, frameId)) {
            bufDescTable[frameId].Clear();
            try {
                hashTable->remove(file, PageNo);
            } catch (HashNotFoundException e) {
                return;
            }
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
