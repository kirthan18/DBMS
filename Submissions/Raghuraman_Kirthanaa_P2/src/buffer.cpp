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
#include "exceptions/invalid_page_exception.h"
#include "exceptions/hash_table_exception.h"


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
        //Loop through each buffer frame
        //If it has dirty pages, flush those pages to disk
        //Delete all book keeping and allocation data structures
        for (uint32_t i = 0; i < numBufs; i++) {
            while (bufDescTable[i].valid) {
                //Before clearing the memory check if page is dirty
                if (bufDescTable[i].dirty) {
                    bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
                    bufDescTable[i].dirty = false;
                }
            }
        }

        delete[] bufDescTable;
        delete[] bufPool;
        delete hashTable;
    }

    void BufMgr::advanceClock() {
        clockHand = (clockHand + 1) % numBufs;
    }

    void BufMgr::allocBuf(FrameId &frame) {
        //printSelf();
        uint32_t i = 0;
        //std::cout << "Clock hand : " << clockHand << std::endl;
        while (1) {
            //Advance clock pointer
            advanceClock();
            //std::cout << "Clock hand after advancing: " << clockHand << std::endl;

            if (!bufDescTable[clockHand].valid) {
                //std::cout << "Frame " << clockHand << " is invalid." << std::endl;
                frame = bufDescTable[clockHand].frameNo;
                bufDescTable[clockHand].Clear();
                return;
            } else {
                if (bufDescTable[clockHand].refbit) {
                    //If ref bit is set, set it to false and move on
                    //std::cout << "Ref bit is set in frame " << clockHand << ". Setting it to false." << std::endl;
                    bufDescTable[clockHand].refbit = false;
                    continue;
                } else {
                    //std::cout << "Ref bit is NOT set in frame " << clockHand << "." << std::endl;
                    if (bufDescTable[clockHand].pinCnt != 0) {
                        //If page is pinned, move on
                        /*std::cout << "Pin count of frame " << clockHand << " is " << bufDescTable[clockHand].pinCnt <<
                        std::endl;*/
                        i++;
                        if (i >= numBufs) {
                            throw BufferExceededException();
                        }
                        continue;
                    } else {
                        //If page is not pinned, check if dirty bit is set
                        //std::cout << "No pages pinned in frame " << clockHand << " ." << std::endl;
                        if (bufDescTable[clockHand].dirty) {
                            //If dirty bit is set, flush disk to page
                            //std::cout << "Dirty bit is set in frame " << clockHand << std::endl;
                            //std::cout << "Writing dirty page in frame " << clockHand << " to disk." << std::endl;
                            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                            bufDescTable[clockHand].dirty = false;
                        }
                        //std::cout << "Removing hash table entry.." << std::endl;

                        //Remove entry for the frame and page no from hash table
                        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

                        frame = bufDescTable[clockHand].frameNo;
                        bufDescTable[clockHand].Clear();
                        //std::cout << "Frame number being set to " << clockHand << std::endl;
                        return;
                    }
                }
            }
        }
    }


    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frameNumber;

        if (hashTable->lookup(file, pageNo, frameNumber)) {
            //Page is in the buffer pool frame
            bufDescTable[frameNumber].refbit = true;
            bufDescTable[frameNumber].pinCnt++;
            page = &bufPool[frameNumber];

        } else {
            //Page not found in buffer pool

            //Call allocBuf() to allocate a buffer frame
            allocBuf(frameNumber);

            //Call file->readPage() to read the page from file into the buffer pool frame
            bufPool[frameNumber] = file->readPage(pageNo);

            //std::cout << "Allocated buffer frame : " << frameNumber;

            //Insert the page into the hash table
            hashTable->insert(file, pageNo, frameNumber);

            //Invoke Set()
            bufDescTable[frameNumber].Set(file, pageNo);

            //Return pointer to frame containing the page via the page parameter
            page = &bufPool[frameNumber];
        }
    }

    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        FrameId frameId;

        //Check if hash table has the particular page
        if (hashTable->lookup(file, pageNo, frameId)) {
            //Check if page is pinned
            if (bufDescTable[frameId].pinCnt == 0) {
                throw PageNotPinnedException(file->filename(), pageNo, frameId);
            } else {
                bufDescTable[frameId].pinCnt--;

                if (dirty) {
                    bufDescTable[frameId].dirty = true;
                }
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
                        bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
                        bufDescTable[i].dirty = false;
                    }

                    hashTable->remove(file, bufDescTable[i].pageNo);
                    bufDescTable[i].Clear();
                }
            } else if (bufDescTable[i].file == file) {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid,
                                         bufDescTable[i].refbit);
            }
        }
    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        FrameId frameId;
        Page allocated_page;

        //Allocate buffer for page
        allocBuf(frameId);

        //Allocate page in file
        allocated_page = file->allocatePage();

        //std::cout << "%%%In Alloc page() - Frame number allocated for " << file->filename() << " and page number " << allocated_page.page_number() <<" is " << frameId << std::endl;
        //Assign page to the frame allocated in buffer pool
        bufPool[frameId] = allocated_page;

        //Insert the mapping of (file, page no) -> frame number in hash table
        hashTable->insert(file, allocated_page.page_number(), frameId);

        //Call set to initialize page parameters
        bufDescTable[frameId].Set(file, allocated_page.page_number());
        page = &bufPool[frameId];
        pageNo = allocated_page.page_number();
    }

    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId frameId;

        //Check if hash table has the particular page
        if (hashTable->lookup(file, PageNo, frameId)) {
            //If present, clear the frame and remove the hash table entry for the file
            bufDescTable[frameId].Clear();
            hashTable->remove(file, PageNo);
        }
        //Delete the page from file
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
