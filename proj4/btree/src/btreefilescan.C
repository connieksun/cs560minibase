/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::~BTreeFileScan()
{
  // put your code here
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
  RID nextRid;
  PageId nextPageId;
  if (curLeafPage == NULL || done) {
    just_returned.slotNo = INVALID_SLOT;
    just_returned.pageNo = INVALID_PAGE;
    return DONE;
  }
  RID cursorRid = curRid; // tracks the next thing for scan to return
  // now we are going to iterate curRid to next record
  Status status = curLeafPage->get_next(curRid, keyptr, nextRid);
  while (status == NOMORERECS) {
    // traverse to next non-empty leaf page
    MINIBASE_BM->unpinPage(curLeafPage->page_no(), 0, 0);
    nextPageId = curLeafPage->getNextPage();
    // case 1 to end scan: no pages left, done
    if (nextPageId == INVALID_PAGE) {
      curLeafPage = NULL;
      done = true;
      break;
    }
    MINIBASE_BM->pinPage(nextPageId, (Page *&) curLeafPage, 0);
    status = curLeafPage->get_first(curRid, keyptr, nextRid);
  }
  // case 2 to end scan: reached end of scan bounds, done
  if (hi_key != NULL && curLeafPage != NULL) {
    if (keyCompare(keyptr, hi_key, key_type) > 0) {
      MINIBASE_BM->unpinPage(curLeafPage->page_no(), 0, 0);
      done = true;
    }
  }
  rid = cursorRid; // next thing that has not been returned
  RID just_returned;
  just_returned.slotNo = curLeafPage->curIterSlot;
  just_returned.pageNo = curLeafPage->page_no();
  just_deleted = false; 
  curRid = nextRid; // ready to return for next time
  return OK;
}

Status BTreeFileScan::delete_current()
{
  // just_returned field tracks the RID returned
  // for the most recent call to get_next()
  if (just_deleted || curLeafPage == NULL || just_returned.slotNo == INVALID_SLOT)
    return MINIBASE_FIRST_ERROR(BTREE, SORTED_PAGE_DELETE_CURRENT_FAILED);
  RID ridToDelete = just_returned;
  BTLeafPage *pageToDeleteFrom;
  MINIBASE_BM->pinPage(ridToDelete.pageNo, (Page *&) pageToDeleteFrom, 0);
  pageToDeleteFrom->deleteRecord(ridToDelete);
  // now the slot directory has changed for this page
  pageToDeleteFrom->curIterSlot--;
  MINIBASE_BM->unpinPage(ridToDelete.pageNo, 0, 0);
  just_deleted = true;
  return OK;
}


int BTreeFileScan::keysize() 
{
  return btree->keysize();
}
