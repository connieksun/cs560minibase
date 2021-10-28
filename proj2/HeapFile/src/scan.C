/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

/*
 * Authors: Connie Sun and Bailey Thompson
 * Course: CSC 560, Fall 2021
 */

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{
  status = init(hf);
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
	reset();
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
	if (nxtUserStatus == 0) { // no next record to get
		reset();
		return DONE;
	}
	rid = userRid; // userRid always holds the next non-returned Rid
	dataPage->getRecord(userRid, recPtr, recLen);
	mvNext(userRid); // move userRid to the next record
	return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
	_hf = hf;
	dirPageId = INVALID_PAGE; // init page IDs
	dataPageId = INVALID_PAGE;
	nxtUserStatus = 1; // assume next record exists
  return firstDataPage(); // pin the first data page
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
	Status status = OK;
	if (dirPageId != INVALID_PAGE) {
		status = MINIBASE_BM->unpinPage(dirPageId);
		dirPageId = INVALID_PAGE;
	}
	if (dataPageId != INVALID_PAGE) {
		status = MINIBASE_BM->unpinPage(dataPageId);
		dataPageId = INVALID_PAGE;
	}
	dataPage = NULL;
	dirPage = NULL;
	nxtUserStatus = 1;
  return status;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
	char* dpInfoRecPtr;
	int dpInfoRecLen;
	DataPageInfo* dpInfoRecord;
	Status status = OK;
	// initialize cur dirPageID using header ID from hf
	dirPageId = _hf->firstDirPageId;
	// pin the first directory page; store in dirPage
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
	// get the record ID of the first DataPageInfo record
	status = dirPage->firstRecord(dataPageRid);
	if (status == DONE) {
		nxtUserStatus = 0; // empty header page, no records
		return DONE;
	}
	// get the ID of the data page, stored in the DataPageInfo record
	dirPage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
	dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
	dataPageId = dpInfoRecord->pageId; // get ID of the data page, stored in DataPageInfo record
	// pin the first data page; store in dataPage pointer
	status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
	status = dataPage->firstRecord(userRid);
	if (status == DONE)
		nxtUserStatus = 0; // empty first data page, no records
  return status;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
	char* dpInfoRecPtr;
	int dpInfoRecLen;
	DataPageInfo* dpInfoRecord;
	// TODO: add checks and statuses
	Status status = OK;
	// get the record ID of the next data page info record in directory page
	RID nextRid;
	status = dirPage->nextRecord(dataPageRid, nextRid);
	if (status == DONE) { // no more data page info records, go to next directory page
		status = nextDirPage();
		if (status == DONE) { // no more directory pages
			return DONE;
		} // else, read the first record of new directory page
		status = dirPage->firstRecord(nextRid);
		if (status == DONE) return DONE; // no records on next dir page
	}
	status = MINIBASE_BM->unpinPage(dataPageId); // unpin current data page
	dataPageRid = nextRid;
	dirPage->returnRecord(dataPageRid, dpInfoRecPtr, dpInfoRecLen);
	dpInfoRecord = (DataPageInfo*) dpInfoRecPtr;
	dataPageId = dpInfoRecord->pageId;
	status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
  return status;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
	Status status = OK;
	status = MINIBASE_BM->unpinPage(dirPageId); // unpin current directory page
	dirPageId = dirPage->getNextPage();
	if (dirPageId == INVALID_PAGE) { // no more directory pages
		return DONE;
	}
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage); // pin new page
  return status;
}

// Move to the next record in a sequential scan.
// Also returns the RID of the (new) current record.
Status Scan::mvNext(RID& rid) {
	Status status;
	RID nextRid;
	status = dataPage->nextRecord(userRid, nextRid); // get next record on data page
	if (status == DONE) { // if data page has no more records, go to next data page
		status = nextDataPage();
		if (status == DONE) { // if no more data pages, no more records to read
			nxtUserStatus = 0;// next record does not exist
			return DONE;
		}
		status = dataPage->firstRecord(nextRid); // get first record of new data page
	}
	rid = nextRid;
	userRid = rid; // don't need this line I think
	return status;
}
