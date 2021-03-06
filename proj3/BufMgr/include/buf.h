///////////////////////////////////////////////////////////////////////////////
/////////////  The Header File for the Buffer Manager /////////////////////////
///////////////////////////////////////////////////////////////////////////////


#ifndef BUF_H
#define BUF_H

#include "db.h"
#include "page.h"
#include "new_error.h"
#include <queue>
#include <stack>

#define NUMBUF 20   
// Default number of frames, artifically small number for ease of debugging.

#define HTSIZE 7
// Hash Table size



/*******************ALL BELOW are purely local to buffer Manager********/

// You could add more enums for internal errors in the buffer manager.
enum bufErrCodes  {HASHMEMORY, HASHDUPLICATEINSERT, HASHREMOVEERROR, HASHNOTFOUND, QMEMORYERROR, QEMPTY, INTERNALERROR, 
			BUFFERFULL, BUFMGRMEMORYERROR, BUFFERPAGENOTFOUND, BUFFERPAGENOTPINNED, BUFFERPAGEPINNED};

class Replacer; // may not be necessary as described below in the constructor

struct FrameDesc {
    PageId    pageNo;
    unsigned int pin_cnt;  // The pin count for the page in this frame
    bool dirty;
};

class BucketPair {
	friend class BufMgr;

	PageId		pageNo;
	int			frameNo;
	BucketPair	*next;

	BucketPair() {
		pageNo = INVALID_PAGE;
		frameNo = -1;
		next = NULL;
	}

	BucketPair(PageId pageId, int frame) {
		pageNo = pageId;
		frameNo = frame;
		next = NULL;
	}
};

class BufMgr {

private:
//   unsigned int	numBuffers = NUMBUF;
   int numBuffers = NUMBUF;
   FrameDesc	*frmeTable;
   BucketPair	**hashTable;
   Replacer		*replacer;
   stack<PageId> hated;
   queue<PageId> loved;

   int hashPage(PageId pageId);
   int findFrame(PageId pageId);
   int getPageToReplace();
   void insertHTEntry(PageId pageId, int frameNo);
   Status removeHTEntry(PageId pageId);

public:
    Page* bufPool; // The actual buffer pool

    BufMgr (int numbuf, Replacer *replacer = 0);

    ~BufMgr();           // Flush all valid dirty pages to disk

    void printBufPool(Page*& Page); // delete; for debugging
    void printHashTable();

    Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage);
        // Check if this page is in buffer pool, otherwise
        // find a frame for this page, read in and pin it.
        // also write out the old page if it's dirty before reading
        // if emptyPage==TRUE, then actually no read is done to bring
        // the page

    Status unpinPage(PageId globalPageId_in_a_DB, int dirty, int hate);
        // hate should be TRUE if the page is hated and FALSE otherwise
        // if pincount>0, decrement it and if it becomes zero,
        // put it in a group of replacement candidates.
        // if pincount=0 before this call, return error.

    Status newPage(PageId& firstPageId, Page*& firstpage, int howmany=1);
        // call DB object to allocate a run of new pages and 
        // find a frame in the buffer pool for the first page
        // and pin it. If buffer is full, ask DB to deallocate 
        // all these pages and return error

    Status freePage(PageId globalPageId); 
        // User should call this method if it needs to delete a page
        // this routine will call DB to deallocate the page 

    Status flushPage(PageId pageid);
        // Used to flush a particular page of the buffer pool to disk
        // Should call the write_page method of the DB class

    Status flushAllPages();
	// Flush all pages of the buffer pool to disk, as per flushPage.

    /*** Methods for compatibility with project 1 ***/
    Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename);
	// Should be equivalent to the above pinPage()
	// Necessary for backward compatibility with project 1

    Status unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename);
	// Should be equivalent to the above unpinPage()
	// Necessary for backward compatibility with project 1
    
    unsigned int getNumBuffers() const { return numBuffers; }
	// Get number of buffers

    unsigned int getNumUnpinnedBuffers();
	// Get number of unpinned buffers
};

#endif
