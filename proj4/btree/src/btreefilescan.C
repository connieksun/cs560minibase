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
  if (curLeafPage == NULL) return DONE;
  RID cursorRid = curRid; 
  Status status = curLeafPage->get_next(curRid, keyptr, nextRid);
  while (status == NOMORERECS) {
    // traverse to next non-empty leaf page
    MINIBASE_BM->unpinPage(curLeafPage->page_no(), 0, 0);
    nextPageId = curLeafPage->getNextPage();
    if (nextPageId == INVALID_PAGE) {
      curLeafPage = NULL;
      return DONE;
    }
    MINIBASE_BM->pinPage(nextPageId, (Page *&) curLeafPage, 0);
    status = curLeafPage->get_first(curRid, keyptr, nextRid);
  }
  if (hi_key != NULL) { // check end of range
    if (keyCompare(keyptr, hi_key, key_type) > 0) {
      return DONE;
    }
  }
  rid = cursorRid;
  curRid = nextRid;
  return OK;
}

Status BTreeFileScan::delete_current()
{
  // put your code here
  return OK;
}


int BTreeFileScan::keysize() 
{
  return btree->keysize();
}
