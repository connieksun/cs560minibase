#include "heapfile.h"

#include <iostream>

/*
 * Authors: Connie Sun and Bailey Thompson
 * Course: CSC 560, Fall 2021
 */

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

      // unpin after being pinned from newPage()
      MINIBASE_BM->unpinPage(firstDirPageId);

    }

    file_deleted = 0;
    returnStatus = OK;
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
	if (fileName == NULL && file_deleted != 1) {
		deleteFile();
	}
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
   // fill in the body
	Status status;
   int recs = 0;

   //start at first page and traverse
   PageId dirPageId = firstDirPageId;
   DataPageInfo* dpInfoRecord;
   char* dpInfoRecPtr;
   int dpInfoRecLen;
   RID dataPageRid;
   RID nextRid;

   HFPage *dirPage;

   while(dirPageId != INVALID_PAGE) {
     MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
     //FINISH ... get # of recs from page, set next page, unpin
     status = dirPage->firstRecord(dataPageRid);
     while (status != DONE) {
    	 dirPage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
		 dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
		 recs += dpInfoRecord->recct;
		 status = dirPage->nextRecord(dataPageRid, nextRid);
		 dataPageRid = nextRid;
     }
     MINIBASE_BM->unpinPage(dirPageId);
     dirPageId = dirPage->getNextPage();
   }

   return recs;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
	if (recLen > 1000)
		return MINIBASE_FIRST_ERROR(HEAPFILE,NO_SPACE);
	Status status = OK;
    PageId dirPageId = firstDirPageId;
    PageId dataPageId = INVALID_PAGE;
    DataPageInfo* dpInfoRecord;
    HFPage* dirPage;
    HFPage* dataPage;
    RID dataPageRid;
    char* dpInfoRecPtr;
    int dpInfoRecLen;
	// pin the first directory page; store in dirPage
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
	// get the record ID of the first DataPageInfo record
	status = dirPage->firstRecord(dataPageRid);
	// first, search to find the directory and data page to insert
	while (status != DONE) {
		dirPage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
		dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
		dataPageId = dpInfoRecord->pageId; // get ID of the data page, stored in DataPageInfo record
		status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
		if (dataPage->available_space() >= recLen)
		    break; // page with available space was found
		status = MINIBASE_BM->unpinPage(dataPageId); // unpin current data page
		// get the record ID of the next data page info record in directory page
		RID nextRid;
		status = dirPage->nextRecord(dataPageRid, nextRid);
		if (status == DONE) { // no more data page info records
			// need to make new data page and add to current directory page
			// will need to make new directory page if cur dir page is full
			status = MINIBASE_BM->unpinPage(dirPageId);
			dirPageId = dirPage->getNextPage(); // next directory page
			if (dirPageId == INVALID_PAGE) { // if no next, use old directory page
				dirPageId = dirPage->page_no();
				dataPageId = INVALID_PAGE; // no existing data page
				MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
				break;
			};
			status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage); // pin new page
			status = dirPage->firstRecord(nextRid);
		}
		dataPageRid = nextRid;
	}
    if (status == DONE || dataPageId == INVALID_PAGE) { // no data page with enough space
    	// create new data page
    	if (dataPageId != INVALID_PAGE)
    	    MINIBASE_BM->unpinPage(dataPageId);
    	dpInfoRecord = new DataPageInfo();
    	status = newDataPage(dpInfoRecord);
    	dataPageId = dpInfoRecord->pageId;
    	MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    	dataPage->insertRecord(recPtr, recLen, outRid);
    	dpInfoRecord->recct = 1;
    	// add data page record to matching directory page
    	// check if current page has space
    	if ((int)dirPage->available_space() < (int)sizeof(DataPageInfo)) { // make another dir page
    		MINIBASE_BM->unpinPage(dirPageId); // unpin old directory page;
    		PageId newDirPageId;
    		HFPage* newDirPage;
    		// get new directory page from BM
    		status = MINIBASE_BM->newPage(newDirPageId, (Page *&) newDirPage);
    		newDirPage->init(newDirPageId);
    		dirPage->setNextPage(newDirPageId);
    		newDirPage->setPrevPage(dirPageId);
    		MINIBASE_BM->unpinPage(newDirPageId); // unpin
    		dirPage = newDirPage; // set new to be cur
    		dirPageId = newDirPageId;
    		MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    	}
    	RID dpInfoRid;
    	dirPage->insertRecord((char*) dpInfoRecord, sizeof(DataPageInfo), dpInfoRid);
    } else { // existing data page with enough space; can just insert record
    	status = dataPage->insertRecord(recPtr, recLen, outRid);
    	dirPage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
		dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
		dpInfoRecord->recct = dpInfoRecord->recct + 1;
    }
    if (dirPageId != INVALID_PAGE)
    	MINIBASE_BM->unpinPage(dirPageId, true);
    if (dataPageId != INVALID_PAGE)
    	MINIBASE_BM->unpinPage(dataPageId, true);
    return status;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
	PageId dirPageId, dataPageId;
	HFPage *dirPage, *dataPage;
	RID dataPageRid;
	Status status;
	if (findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, dataPageRid) == OK) {
		//found record
		status = dataPage->deleteRecord(rid);
		MINIBASE_BM->unpinPage(dirPageId, true);
		MINIBASE_BM->unpinPage(dataPageId, true);
		return status;
	} else return DONE;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
	PageId dirPageId, dataPageId;
	HFPage *dirPage, *dataPage;
	RID dataPageRid;
	char *oldRecPtr;
	int oldRecLen;

	if (findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, dataPageRid) == OK) {
		//found record
		dataPage->returnRecord(rid, oldRecPtr, oldRecLen);
		MINIBASE_BM->unpinPage(dirPageId, true);
		MINIBASE_BM->unpinPage(dataPageId, true);
	} else return DONE;

	if (oldRecLen != recLen) {
		return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
	}
	memcpy(oldRecPtr, recPtr, recLen);
	return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{

	PageId dirPageId, dataPageId;
	HFPage *dirPage, *dataPage;
	RID dataPageRid;

	if (findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, dataPageRid) == OK) {
		//found record
		dataPage->getRecord(rid, recPtr, recLen);
		MINIBASE_BM->unpinPage(dirPageId);
		MINIBASE_BM->unpinPage(dataPageId);
		return OK;
	} else return DONE;

}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
  return new Scan(this, status);
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
	PageId dirPageId = firstDirPageId;
	PageId dataPageId;
	HFPage *dirPage;
	RID dataPageRid, nextRid;
	int dpInfoRecLen;
	DataPageInfo* dpInfoRecord;
	char *dpInfoRecPtr;
	Status status;
	if (file_deleted == 1) return DONE;
	while (dirPageId != INVALID_PAGE) {
		status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
		status = dirPage->firstRecord(dataPageRid);
		while (status != DONE) {
			dirPage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
			dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
			dataPageId = dpInfoRecord->pageId; // get ID of the data page, stored in DataPageInfo record
			status = dirPage->nextRecord(dataPageRid, nextRid);
			// free data page
			MINIBASE_BM->freePage(dataPageId);
			dataPageRid = nextRid;
		}
		status = MINIBASE_BM->unpinPage(dirPageId);
		MINIBASE_BM->freePage(dirPageId);
		dirPageId = dirPage->getNextPage();
	}
	status = MINIBASE_DB->delete_file_entry(fileName);
	file_deleted = 1;
    return status;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
	Status status;
	Page *newDataPage;
	PageId pageId;
	// get new page from BM
	status = MINIBASE_BM->newPage(pageId, newDataPage);
	if(status != OK) return status;

	HFPage *hfpage = (HFPage*) newDataPage;
	hfpage->init(pageId);

	dpinfop->availspace = hfpage->available_space();
	dpinfop->recct = 0;
	dpinfop->pageId = pageId;
	MINIBASE_BM->unpinPage(pageId);
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
                    const RID &rpDataPageRid)
{
	Status status = OK;
	rpDirPageId = firstDirPageId;
	rpDataPageId = INVALID_PAGE;
	DataPageInfo* dpInfoRecord;
	RID dataPageRid, nextRid;
	char *dpInfoRecPtr, *recPtr;
	int dpInfoRecLen, recLen;

	while (rpDirPageId != INVALID_PAGE){
		status = MINIBASE_BM->pinPage(rpDirPageId, (Page *&) rpdirpage);
		status = rpdirpage->firstRecord(dataPageRid);
		while (status != DONE) {
			rpdirpage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
			dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
			rpDataPageId = dpInfoRecord->pageId; // get ID of the data page, stored in DataPageInfo record
			status = MINIBASE_BM->pinPage(rpDataPageId, (Page *&) rpdatapage);
			status = rpdatapage->returnRecord(rid, recPtr, recLen);
			if (status == OK) {
				return OK;
			}
			status = MINIBASE_BM->unpinPage(rpDataPageId);
			status = rpdirpage->nextRecord(dataPageRid, nextRid);
			dataPageRid = nextRid;
		}
		status = MINIBASE_BM->unpinPage(rpDirPageId);
		rpDirPageId = rpdirpage->getNextPage();
	}
	rpdirpage = NULL;
	rpdatapage = NULL;
    return DONE; // not found
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // this should only need to be used in insertRecord() but insertRecord()
	// takes care of calling MINIBASE_BM newPage() and adding a DataPageInfo
	// record when needed, so this method is unused
    return OK;
}

// *******************************************
