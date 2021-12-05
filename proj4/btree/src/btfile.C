/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  "OK" // _OK
  "Can't find header page" // CANT_FIND_HEADER
  "Can't pin header page" // CANT_PIN_HEADER,
  "Can't allocate header page" // CANT_ALLOC_HEADER
  "Can't add new file entry" // CANT_ADD_FILE_ENTRY
  "Can't unpin header page" // CANT_UNPIN_HEADER
  "Can't pin page" // CANT_PIN_PAGE
  "Can't unpin page" // CANT_UNPIN_PAGE
  "Invalid scan" // INVALID_SCAN
  "SortedPage failed to delete current" // SORTED_PAGE_DELETE_CURRENT_FAILED
  "Can't delete file entry" // CANT_DELETE_FILE_ENTRY
  "Can't free page" // CANT_FREE_PAGE,
  "Can't delete subtree" // CANT_DELETE_SUBTREE,
  "Key too long" // KEY_TOO_LONG
  "Insert failed" // INSERT_FAILED
  "Could not create root" // COULD_NOT_CREATE_ROOT
  "Delete data entry failed" // DELETE_DATAENTRY_FAILED
  "Data entry not found" // DATA_ENTRY_NOT_FOUND
  "Can't get page no" // CANT_GET_PAGE_NO
  "Can't allocate new page" // CANT_ALLOCATE_NEW_PAGE
  "Can't split leaf page" // CANT_SPLIT_LEAF_PAGE
  "Can't split index page" // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
  returnStatus = MINIBASE_DB->get_file_entry(filename, headerPageId);
  if (returnStatus != OK) {
    returnStatus = MINIBASE_FIRST_ERROR(BTREE, DATA_ENTRY_NOT_FOUND);
    return;
  }
  returnStatus = MINIBASE_BM->pinPage(headerPageId, (Page*&) headerPage, 0);
  if (returnStatus != OK) {
    returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_HEADER);
    return;
  }
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
  returnStatus = MINIBASE_DB->get_file_entry(filename, headerPageId);
  if (returnStatus != OK) { // create file
    returnStatus = MINIBASE_BM->newPage(headerPageId, (Page *&) headerPage);
    if (returnStatus != OK) {
      returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_ALLOC_HEADER);
      return;
    }
    returnStatus = MINIBASE_DB->add_file_entry(filename, headerPageId);
    if (returnStatus != OK) {
      returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_ADD_FILE_ENTRY);
      return;
    }
    // new btree has a header page w/ info and an empty root page (btleaf_page)
    ((HFPage *)headerPage)->init(headerPageId);
    BTLeafPage *rootPage;
    PageId rootPageId;
    MINIBASE_BM->newPage(rootPageId, (Page *&)rootPage);
    rootPage->init(rootPageId);
    headerPage->rootPageId = rootPageId;
    headerPage->key_type = keytype;
    headerPage->key_size = keysize;
    MINIBASE_BM->unpinPage(rootPageId, 1, 0);
  } else {
    returnStatus = MINIBASE_BM->pinPage(headerPageId, (Page *&) headerPage, 0);
    if (returnStatus != OK) {
      returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_HEADER);
      return;
    }
  }
}

BTreeFile::~BTreeFile ()
{
  // put your code here
}

Status BTreeFile::destroyFile ()
{
  // put your code here
  return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
  Status status;
  KeyDataEntry* rootEntryPtr;
  int size;
  status = recursiveInsert(key, rid, headerPage->rootPageId, &rootEntryPtr, &size);
  return status;
  // put your code here
  // if (page->available_space() ...
  //  BTLeafPage *page;
  // // outRid will be the RID of the inserted data entry in the B+-tree leaf node.
  // // NOT to be confused with the rid passed in, which is a pointer to a tuple
  // // in a heap file.
  // RID outRid;
  // MINIBASE_BM->pinPage(pid, (Page *&)page);
  // page->insert(key, rid, outRid);
  // MINIBASE_BM->unpinPage(pid, DIRTY);
}

Status BTreeFile::recursiveInsert(const void *key, const RID rid, PageId curPageId, 
            KeyDataEntry **parent, int *size) {
  Status status;
  AttrType key_type = headerPage->key_type;
  RID newRid = rid;
  SortedPage *curPage;
  MINIBASE_BM->pinPage(curPageId, (Page *&) curPage, 0);
  short curPageType = curPage->get_type();
  if (curPageType == INDEX) {
    BTIndexPage *curIndexPage = (BTIndexPage*) curPage;
    KeyDataEntry* newRecord = new KeyDataEntry();
    int newSize;
    PageId curIndexPageId;
    curIndexPage->get_page_no(key, key_type, curIndexPageId);
    recursiveInsert(key, rid, curIndexPageId, &newRecord, &newSize);
    if (newRecord != NULL) {
      if (newSize <= curIndexPage->available_space())
        curIndexPage->insertKey((void*) (&newRecord->key), key_type, newRecord->data.pageNo, newRid);
      else {
        // page full, split index page to the right
        BTIndexPage *rightIndexPage;
        PageId rightIndexPageId;
        MINIBASE_BM->newPage(rightIndexPageId, (Page *&) rightIndexPage);
        rightIndexPage->init(rightIndexPageId);
        // TODO: split and shift records
        splitIndexPage(curIndexPage, rightIndexPage);
      }
    } else {
      *parent = NULL;
    }
  } else if (curPageType == LEAF) {
    BTLeafPage *curLeafPage = (BTLeafPage*) curPage;
    RID recordRID;
    if (int(sizeof(KeyDataEntry)) <= curLeafPage->available_space()) {
      curLeafPage->insertRec(key, key_type, rid, recordRID);
      *parent = NULL;
    } else {
      // page full, split leaf page to the right
      BTLeafPage *rightLeafPage;
      PageId rightLeafPageId;
      MINIBASE_BM->newPage(rightLeafPageId, (Page *&) rightLeafPage);
      rightLeafPage->init(rightLeafPageId);
      // TODO: split and shift records
      splitLeafPage(curLeafPage, rightLeafPage);
      // and update the linked list
      PageId oldNext = curLeafPage->getNextPage();
      curLeafPage->setNextPage(rightLeafPageId);
      rightLeafPage->setPrevPage(curLeafPage->page_no());
      rightLeafPage->setNextPage(oldNext);
      // make new entry for parent
    }
  }
  // remember to unpin pages when done
  status = OK;
  return status;
}

Status BTreeFile::splitIndexPage(BTIndexPage *left, BTIndexPage *right){
  // TODO
  return OK;
}

Status BTreeFile::splitLeafPage(BTLeafPage *left, BTLeafPage *right){
  // TODO
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  BTreeFileScan *scan = new BTreeFileScan();
  scan->lo_key = lo_key;
  scan->hi_key = hi_key;
  scan->btree = this;
  scan->key_type = headerPage->key_type;
  Status status = setUpScan(scan);
  if (status != OK)
    return NULL;
  return scan;
}

Status BTreeFile::setUpScan(BTreeFileScan *scan){
  // TODO: intiialize the scan by setting the scan pointer to the first rec
  // that matches lo_key
  // if lo_key is NULL, go all the way left. else search tree
  Status status;
  PageId curPageId = headerPage->rootPageId;
  SortedPage *curPage;
  status = MINIBASE_BM->pinPage(curPageId, (Page *&) curPage, 0);
  if (status != OK)
    return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
  RID nextRid;
  Keytype *nextKey;
  while(curPage->get_type() == INDEX) {
    BTIndexPage *curIndexPage = (BTIndexPage *) curPage;
    // in the middle of the tree, need to get to an index
    if (scan->lo_key == NULL) // go all the way to the left
      curIndexPage->get_first(nextRid, (void *) &nextKey, curPageId);
    else // get inner page w/ key >= lo_key
      curIndexPage->get_page_no((void *) &nextKey, headerPage->key_type, curPageId);
    status = MINIBASE_BM->pinPage(curPageId, (Page *&) curPage, 0);
  }
  // now we should be at the starting leaf page
  RID curRid;
  BTLeafPage *curLeafPage = (BTLeafPage *) curPage;
  status = curLeafPage->get_first(nextRid, &nextKey, curRid);
  if (scan->lo_key != NULL) {
    while (keyCompare(&nextKey, scan->lo_key, headerPage->key_type) < 0) {
      status = curLeafPage->get_next(nextRid, &nextKey, curRid);
      if (status == NOMORERECS)
        break;
    }
  }
  scan->curLeafPage = curLeafPage;
  scan->curRid = curRid;
  return OK;
}

int BTreeFile::keysize(){
  return headerPage->key_size;
}
