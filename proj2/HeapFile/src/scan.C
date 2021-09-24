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
  // TODO: checks and statuses
	if (nxtUserStatus == 0) { // no next record
		return DONE;
	}
	Status status;
	rid = userRid;
	status = dataPage->getRecord(userRid, recPtr, recLen);
	status = mvNext(userRid);
	return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
  _hf = hf;
  return firstDataPage();
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
	// TODO: checks and statuses
	Status status = OK;
	status = MINIBASE_BM->unpinPage(dirPageId);
	status = MINIBASE_BM->unpinPage(dataPageId);
	dataPage = NULL;
	dirPage = NULL;
  return status;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
	// TODO: lots of checks for empty
	Status status = OK; // need to work on using this
	// initialize cur dirPageID using header ID from hf
	dirPageId = _hf->firstDirPageId;
	// pin the first directory page; store in dirPage pointer
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
	// get the record ID of the first DataPageInfo record
	status = dirPage->firstRecord(dataPageRid);
	// get the ID of the data page; stored in the DataPageInfo record
	dataPageId = dataPageRid.pageNo;
	// pin the first data page; store in dataPage pointer
	status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);

	status = dataPage->firstRecord(userRid);

  return status;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
	// TODO: add checks and statuses
	Status status = OK;
	status = MINIBASE_BM->unpinPage(dataPageId);
	// get the record ID of the next data page info record in directory page
	RID nextRid;
	status = dirPage->nextRecord(dataPageRid, nextRid);
	if (status == DONE) {
		// move to the next directory page
		status = nextDirPage();
		if (status == DONE) {
			// no more directory pages
			return DONE;
		} // else, read the first record of new directory page
		status = dirPage->firstRecord(nextRid);
	}
	dataPageRid = nextRid;
	dataPageId = dataPageRid.pageNo;
	status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
//	status = dataPage->firstRecord(userRid); // read the first record of new data page
  return status;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
	// TODO: add checks and statuses
	Status status = OK;
	status = MINIBASE_BM->unpinPage(dirPageId);
	dirPageId = dirPage->getNextPage();
	if (dirPageId == INVALID_PAGE) {
		// no more directory pages
		return DONE;
	}
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
  return status;
}

// Look ahead the next record
Status Scan::peekNext(RID& rid) {
	rid = userRid;
	return OK;
}

// Move to the next record in a sequential scan.
// Also returns the RID of the (new) current record.
Status Scan::mvNext(RID& rid) {
	Status status;
	RID nextRid;
	status = dataPage->nextRecord(userRid, nextRid);
	if (status == DONE) {
		status = nextDataPage();
		if (status == DONE) {
			nxtUserStatus = 0;
			return DONE;
		}
		status = dataPage->firstRecord(nextRid);
	}
	rid = nextRid;
	userRid = rid; // don't need this?
	return status;
}
