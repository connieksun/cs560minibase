#include "heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
   
    // fill in the body

    fileName = NULL;
    //if name already denotes a file, open file; otherwise, a new empty file is created

    if(name != NULL) {
      fileName = new char[strlen(name) + 1];
      strcpy(fileName, name);
    }

    Page *page;
    Status status;

    //open DB and get file entry for given file
    status = MINIBASE_DB->get_file_entry(fileName, firstDirPageId);

    if (status != OK) {
      //create new page and add to file entry
      MINIBASE_BM->newPage(firstDirPageId, page);
      MINIBASE_DB->add_file_entry(fileName, firstDirPageId);


      HFPage* hfpage = (HFPage*) page;
      hfpage->init(firstDirPageId);
      hfpage->setPrevPage(INVALID_PAGE);
      hfpage->setNextPage(INVALID_PAGE);

    }

    file_deleted = 0;
    returnStatus = OK;
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // fill in the body 

   if (file_deleted != 1) {
     Status status = deleteFile();

     if (status != OK) {
       delete[] fileName;
       return;
     }
   }
   delete[] fileName;
   fileName = NULL;

}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
   // fill in the body

   int recs = 0;

   //start at first page and traverse
   PageId pageId = firstDirPageId;
   PageId nextPage = INVALID_PAGE;

   HFPage *page;

   while(pageId != INVALID_PAGE) {
     //FINISH
   }

   return OK;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    // fill in the body

    PageId pageId = firstDirPageId;
    PageId nextPage = INVALID_PAGE;
    PageId endPage = INVALID_PAGE;

    HFPage *page;
    HFPage *next;

    Status status;
    while(pageId != INVALID_PAGE) {
      // pin the page
      //MINIBASE_BM->pinPage(pageId, page);

      status = page->insertRecord(recPtr, recLen, outRid);

      nextPage = page->getNextPage();
      MINIBASE_BM->unpinPage(pageId);

      if (status == OK) break;
      else {
        endPage = pageId;
        pageId = nextPage;
        minibase_errors.clear_errors();
      }
    }

    return OK;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
  // fill in the body
  return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
  // fill in the body
  return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
  // fill in the body 
  return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
  // fill in the body 
  return NULL;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    // fill in the body
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    // fill in the body
    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid)
{
    // fill in the body
    return OK;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // fill in the body
    return OK;
}

// *******************************************
