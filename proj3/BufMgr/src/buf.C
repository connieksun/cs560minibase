/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "buf.h"
#include <iostream>
using namespace std;

// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
	this->replacer = NULL;
	//cout << "start of constructor for " << numbuf << " buffers" << endl;
	numBuffers = numbuf;
	bufPool = new Page[numbuf];
	//cout << "bufPool " << bufPool[0] << endl;
	frmeTable = new FrameDesc[numbuf];
	hashTable = new BucketPair*[HTSIZE];
	for (int i = 0; i < numbuf; i++) {
		frmeTable[i].pageNo = INVALID_PAGE;
		frmeTable[i].pin_cnt = 0;
		frmeTable[i].dirty = false;
	}
	for (int i = 0; i < HTSIZE; i++)
		hashTable[i] = NULL;
	//cout << "end of constructor" << endl;
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
	//cout << "start of destructor" << endl;
	for (int frame = 0; frame < numBuffers; frame++) {
		if (frmeTable[frame].dirty) {
			flushPage(frmeTable[frame].pageNo);
		}
	}
	delete[] frmeTable;
	delete[] bufPool;
	BucketPair *tmp;
	for(int i = 0; i < HTSIZE; i++) {
		while (hashTable[i]) {
			tmp = hashTable[i];
			hashTable[i] = tmp->next;
			delete tmp;
		}
		delete hashTable[i];
	}
	delete[] hashTable;//delete the array of pointers
}

void BufMgr::printBufPool(Page*& page){
	//cout << "*******contents of bufPool*******" << endl;
	for (int i = 0; i < numBuffers; i++) {
		cout << "frame " << i << ": ";
		int pageId = frmeTable[i].pageNo;
		if (pageId == INVALID_PAGE) {
			cout << "INVALID_PAGE (frame table)" << endl;
		}
		page = &bufPool[i];
		MINIBASE_DB->read_page(pageId, page);
		cout << (char *)page << "\tpage id is " << pageId << "(frame table)" << endl;
	}
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
	//cout << "start of pinPage for pageId " << PageId_in_a_DB << endl;
	int frameNo = findFrame(PageId_in_a_DB);
	if (frameNo != -1) { // page is already pinned in buffer
		//cout << "already pinned in frame " << frameNo << endl;
		page = &bufPool[frameNo];
		frmeTable[frameNo].pin_cnt++;
		return OK;
	}
	// check for empty frame
	for (int frame = 0; frame < numBuffers; frame++) {
		if (frmeTable[frame].pageNo == INVALID_PAGE) {
			//cout << "pin page to empty frame" << endl;
			// found an empty frame; add page to frame table, buffer pool, and hash
			frmeTable[frame].pageNo = PageId_in_a_DB;
			frmeTable[frame].pin_cnt++;
			page = &bufPool[frame];
			if (! emptyPage) {
				//cout << "DB is reading the page" << endl;
				Status status = MINIBASE_DB->read_page(PageId_in_a_DB, page);
				if (status != OK)
					return MINIBASE_CHAIN_ERROR(BUFMGR, status);
				//cout << "DB read the page" << endl;
				//cout << "copied page to bufPool array" << endl;
			}
			insertHTEntry(PageId_in_a_DB, frame);
			//cout << "empty: pinned page " << PageId_in_a_DB << " in frame " << frame << endl;
			return OK;
		}
	}
	// not already pinned, no empty frames; replace an existing page
	// cout << "pin page by replacing" << endl;
	PageId pageToReplace = getPageToReplace();
	if (pageToReplace == -1)
		return DONE; // could not pin page; buffer is full?
	int frame = findFrame(pageToReplace);
	if (frmeTable[frame].dirty) flushPage(pageToReplace);
	frmeTable[frame].pageNo = PageId_in_a_DB;
	frmeTable[frame].pin_cnt = 0;
	frmeTable[frame].dirty = 0;
	page = &bufPool[frame];
	// if page is not empty, read in new page
	if (! emptyPage) {
		Status status = MINIBASE_DB->read_page(PageId_in_a_DB, page);
		if (status != OK)
			return MINIBASE_CHAIN_ERROR(BUFMGR, status);
	}
	insertHTEntry(PageId_in_a_DB, frame);
	removeHTEntry(pageToReplace);
	frmeTable[frame].pin_cnt++;
	//cout << "replace: pinned in frame " << frame << endl;
	return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
	//cout << "start of unpinPage for pageId " << page_num << endl;
	int frame = findFrame(page_num);
	//cout << "frame to unpin from " << frame << endl;
	//cout << "pin count of frame " << frmeTable[frame].pin_cnt << endl;
	if (frame == -1 || frmeTable[frame].pin_cnt == 0)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
	frmeTable[frame].pin_cnt--;
	frmeTable[frame].dirty = dirty;
	if (frmeTable[frame].pin_cnt == 0) {
		if (hate) {
			//cout << "add page " << page_num << " to hated" << endl;
			hated.push(page_num);
		}
		else {
			//cout << "add page " << page_num << " to loved" << endl;
			loved.push(page_num);
		}
	}
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
	//cout << "start of newPage for page " << firstPageId << endl;
	int full = 1;
	for (int frame = 0; frame < numBuffers; frame++) {
		if (frmeTable[frame].pin_cnt == 0) full = 0;
	}
	if (full) return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERFULL);
	Status status = MINIBASE_DB->allocate_page(firstPageId, howmany);
	if (status != OK) return MINIBASE_CHAIN_ERROR(BUFMGR, status);
	status = pinPage(firstPageId, firstpage, 0);
	if (status != OK)
		MINIBASE_DB->deallocate_page(firstPageId, howmany);
	return status;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
	//cout << "start of freePage for pageId " << globalPageId << endl;
	int frame = findFrame(globalPageId);
	if (frame != -1) { // exists somewhere already
		if (frmeTable[frame].pin_cnt != 0)
			return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);
		frmeTable[frame].pageNo = INVALID_PAGE;
		frmeTable[frame].pin_cnt = 0;
		frmeTable[frame].dirty = 0;
		removeHTEntry(globalPageId);
	}
	Status status = MINIBASE_DB->deallocate_page(globalPageId);
	return status;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
	//cout << "start of flushPage" << endl;
	// find the frame where the page lives
	int frame = findFrame(pageid);
	if (frame == -1) {
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
	}
	if (frmeTable[frame].dirty) {
		Page *page = &bufPool[frame];
		Status status = MINIBASE_DB->write_page(pageid, page);
		return status;
	}
	return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
	//cout << "start of flushAllPages" << endl;
  for (int frame = 0; frame < numBuffers; frame++) {
	  if (frmeTable[frame].dirty) {
		  flushPage(frmeTable[frame].pageNo);
	  }
  }
  return OK;
}

//****** begin own private methods ******
/*
 * hash function for a page; returns bucket
 */
int BufMgr::hashPage(PageId pageId) {
	return (3 * pageId + 5) % HTSIZE;
}

void BufMgr::printHashTable(){
	//cout << "*******contents of hashTable*******" << endl;
	for (int b = 0; b < HTSIZE; b++) {
		cout << "bucket " << b << ": ";
		BucketPair *cur = hashTable[b];
		while (cur != NULL) {
			cout << cur->pageNo << "," << cur->frameNo;
			cout << "(ff:" << findFrame(cur->pageNo) << ") ";
			cur = cur->next;
		}
		cout << endl;
	}
}
/*
 * use the hash function to find the frame that a page belongs
 */
int BufMgr::findFrame(PageId pageId) {
	//cout << "start of findFrame for pageId " << pageId << endl;
	int bucket = hashPage(pageId);
	//cout << "check bucket " << bucket << endl;
	BucketPair *cur = hashTable[bucket];
	while (cur != NULL) {
		//cout << "iterating through linked list" << endl;
		if (cur->pageNo == pageId) {
			// cout << "in findFrame, page " << pageId << " lives in frame " << cur->frameNo << endl;
			return cur->frameNo;
		}
		cur = cur->next;
	}
	return -1;
}
/*
 * use the love/hate replacement policy to get page to replace
 */
int BufMgr::getPageToReplace() {
	//cout << "start of getPageToReplace" << endl;
	if (!hated.empty()) {
		PageId toReplace = hated.top();
		if (loved.front() == toReplace) { // loved and hated
			hated.pop();
			toReplace = hated.top();
		}
		hated.pop();
		// cout << "page to be written over is " << toReplace << endl;
		return toReplace;
	}
	if (!loved.empty()) {
		PageId toReplace = loved.front();
		loved.pop();
		// cout << "page to be written over is " << toReplace << endl;
		return toReplace;
	}
	// cout << "page to be written over is -1" << endl;
	return -1;
}
/*
 * insert a new HT pair into the linked list of the bucket
 */
void BufMgr::insertHTEntry(PageId pageId, int frameNo) {
	//cout << "start of insertHTEntry for pageId " << pageId << endl;
	int bucket = hashPage(pageId);
	//cout << "insert to bucket " << bucket << endl;
	BucketPair *cur = hashTable[bucket];
	if (cur == NULL) {
		hashTable[bucket] = new BucketPair(pageId, frameNo);
		//cout << "inserted as head" << endl;
		return;
	} while(cur->next != NULL) cur = cur->next;
	BucketPair *newEntry = new BucketPair(pageId, frameNo);
	cur->next = newEntry;
}
/*
 * traverse linked list of bucket and remove entry
 */
Status BufMgr::removeHTEntry(PageId pageId) {
	//cout << "start of removeHTEntry" << endl;
	int bucket = hashPage(pageId);
	//cout << "delete from bucket " << bucket << endl;
	BucketPair *cur = hashTable[bucket]; // head of list
	if (cur->pageNo == pageId) { // first item, move head up one
		//cout << "delete head" << endl;
		hashTable[bucket] = cur->next;
		delete(cur);
		return OK;
	}
	// otherwise, iterate until the next entry is the one to delete
	//cout << "delete from middle" << endl;
	while (cur->next != NULL && cur->next->pageNo != pageId)
		cur = cur->next;
	if (cur->next == NULL)
		return MINIBASE_FIRST_ERROR(BUFMGR, HASHNOTFOUND);
	if (cur->next->pageNo == pageId) {
		BucketPair *temp = cur->next;
		cur->next = cur->next->next;
		delete(temp);
	}
	return OK;
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  //put your code here
	//cout << "start of pinPage with filename" << endl;
  return pinPage(PageId_in_a_DB, page, emptyPage);
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  //put your code here
	//cout << "start of unpinPage with filename" << endl;
  return unpinPage(globalPageId_in_a_DB, dirty, filename);
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
	//cout << "start of getNumUnpinnedBuffers" << endl;
  int count = 0;
  for (int frame = 0; frame < numBuffers; frame++){
	  if (frmeTable[frame].pin_cnt == 0)
		  count++;
  }
  return count;
}
