#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"

/*
 * Authors: Connie Sun and Bailey Thompson
 * Course: CSC 560, Fall 2021
 */

// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
  nextPage = INVALID_PAGE;
  prevPage = INVALID_PAGE;
  curPage = pageNo;
  slotCnt = 0;
  freeSpace = MAX_SPACE - DPFIXED + sizeof(slot_t);
  usedPtr = MAX_SPACE - DPFIXED ; // index + 1

  slot[0].length = EMPTY_SLOT;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    // fill in the body
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    // fill in the body
	prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    // fill in the body
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
	nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    if (recLen > freeSpace) return DONE; // space check for record

    int freeSlot = slotCnt; // default is new slot
    // see if there's a hole in the slot directory
    for (int i = 0; i < slotCnt; i++) {
    	if (slot[i].length == EMPTY_SLOT) {
    		freeSlot = i;
    		break;
    	}
    }
    if (freeSlot == slotCnt) { // no hole, space check again
    	if ((int)(recLen + sizeof(slot_t)) > (int)freeSpace) return DONE;
    	else { // add a slot
    		slotCnt++;
    		freeSpace -= sizeof(slot_t);
    	}
    }
    freeSpace -= recLen;
    usedPtr -= recLen;
    // update slot info with record location
    slot[freeSlot].length = recLen;
    slot[freeSlot].offset = usedPtr;
    // update record info with page/slot location
    rid.pageNo = curPage;
    rid.slotNo = freeSlot;
    // copy into data
    memcpy(data + usedPtr, recPtr, recLen);
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
	int slotNo = rid.slotNo;
	// check that the record actually exists in the page
	if (rid.pageNo != curPage || slotNo >= slotCnt ||
			slotNo < 0 || slot[slotNo].length == EMPTY_SLOT) return FAIL;
	int recStart = slot[slotNo].offset; // start of record to delete
	int delSpace = slot[slotNo].length; // amount of space deleted
	if (slot[slotNo].length == EMPTY_SLOT) return DONE; // what to return if not in here?
	slot[slotNo].length = EMPTY_SLOT;
	if (usedPtr != recStart) { // there are records to compact up in data
		int lenToMove = recStart - usedPtr;	// length of records to move
		// move prior records up by the amount of deleted space
		memmove(data + usedPtr + delSpace, data + usedPtr, lenToMove);
		for (int i = 0; i < slotCnt; i++) {
			if (slot[i].length != EMPTY_SLOT && slot[i].offset < recStart) {
				slot[i].offset += delSpace;
			}
		}
	}
	usedPtr += delSpace;
	freeSpace += delSpace;
	// compacting end of slot array
	while (slotCnt > 0 && slot[slotCnt-1].length == EMPTY_SLOT) {
		freeSpace += sizeof(slot_t);
		slotCnt--;
	}
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    for (int i = 0; i < slotCnt; i ++) {
    	if (slot[i].length != EMPTY_SLOT) {
    		firstRid.pageNo = curPage;
    		firstRid.slotNo = i;
    		return OK;
    	}
    }
    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
	int curSlotNo = curRid.slotNo;
	if (curRid.pageNo != curPage || curSlotNo >= slotCnt ||
				curSlotNo < 0 || slot[curSlotNo].length == EMPTY_SLOT) return FAIL;
    for (int i = curSlotNo + 1; i < slotCnt; i++) {
    	if (slot[i].length != EMPTY_SLOT){
    		nextRid.pageNo = curPage;
    		nextRid.slotNo = i;
    		return OK;
    	}
    }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
	int slotNo = rid.slotNo;
	if (rid.pageNo != curPage || slotNo >= slotCnt ||
					slotNo < 0 || slot[slotNo].length == EMPTY_SLOT) return FAIL;
    recLen = slot[slotNo].length;
    int recStart = slot[slotNo].offset;
    memcpy(recPtr, data + recStart, recLen);
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
	int slotNo = rid.slotNo;
	if (rid.pageNo != curPage || slotNo >= slotCnt ||
					slotNo < 0 || slot[slotNo].length == EMPTY_SLOT) return FAIL;
    recLen = slot[slotNo].length;
    int recStart = slot[slotNo].offset;
    recPtr = &data[recStart];
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    return freeSpace - sizeof(slot_t);
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    for (int i = 0; i < slotCnt; i++) {
    	if (slot[i].length != EMPTY_SLOT) return 0;
    }
    return 1;
}


