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

#include <iostream>
using namespace std;

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
  //cout << "******start of constructor for existing BTreeFile: " << filename << endl;
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
  //cout << "*****start of constructor for new BTreeFile: " << filename << endl;
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
    returnStatus = MINIBASE_BM->newPage(rootPageId, (Page *&)rootPage);
    rootPage->init(rootPageId);
    headerPage->rootPageId = rootPageId;
    headerPage->key_type = keytype;
    headerPage->key_size = keysize;
    returnStatus = MINIBASE_BM->unpinPage(rootPageId, 1, 0);
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
  //cout << "*****start of destructor for BTreeFile" << endl;
  // put your code here
}

Status BTreeFile::destroyFile ()
{
  //cout << "*****start of destroyFile() for BTreeFile" << endl;
  // put your code here
  return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
  cout << "*****start of insert() for BTreeFile" << endl;
  int printKey = *(int *) key;
  cout << "\t inserting key " << printKey << endl;
  Status status;
  status = recursiveInsert(headerPage->rootPageId, key, rid, NULL);
  return status;
}

Status BTreeFile::recursiveInsert(PageId curPageId, const void *key, const RID rid,
                  KeyDataEntry *newChildEntry) {
  //cout << "*****start of recursiveInsert() for BTreeFile" << endl;
  Status status;
  AttrType key_type = headerPage->key_type;
  SortedPage *curPage;
  RID holderRid;
  PageId holderPageId;
  status = MINIBASE_BM->pinPage(curPageId, (Page *&) curPage, 0);
  short curPageType = curPage->get_type();
  if (curPageType == INDEX) {
    cout << "splitting index page" << endl;
    BTIndexPage *curIndexPage = (BTIndexPage*) curPage;
    PageId childPageId;
    // get the right child page using search implementation from book
    curIndexPage->get_page_no(key, key_type, childPageId);
    recursiveInsert(childPageId, key, rid, newChildEntry);
    if (newChildEntry == NULL) return OK;
    // if we have space, we add the child and don't need to keep splitting up
    else if (curIndexPage->available_space() >= get_key_data_length(key, key_type, INDEX)) {
      curIndexPage->insertKey((void *) &newChildEntry->key, key_type, newChildEntry->data.pageNo, holderRid);
      newChildEntry = NULL;
      return OK;
    } // if we don't have space, we propagate the split upwards
    else {
      PageId rightIndexPageId;
      BTIndexPage *rightIndexPage;
      MINIBASE_BM->newPage(rightIndexPageId, (Page *&) rightIndexPage);
      rightIndexPage->init(rightIndexPageId);
      // what are we adding to this page? (newChildEntry key and pageNo)
      splitIndexPage(curIndexPage, rightIndexPage, &newChildEntry->key, newChildEntry->data.pageNo);
      KeyDataEntry *newEntry;  // entry that will be added to parent page
      Datatype newEntryPage; // hold pageId of new page that was made
      newEntryPage.pageNo = rightIndexPageId;
      Keytype rightPageFirstKey;
      // need to get the first key of the right page
      // get_first(RID& rid, void *key, PageId & pageNo)
      rightIndexPage->get_first(holderRid, (void*) &rightPageFirstKey, holderPageId);
      int entryLen;
      make_entry(newEntry, key_type, (void *) &rightPageFirstKey, INDEX, newEntryPage, &entryLen);
      newChildEntry = newEntry;
      if (curPageId == headerPage->rootPageId) {
        BTIndexPage *newRoot;
        PageId newRootId;
        MINIBASE_BM->newPage(newRootId, (Page *&) newRoot);
        newRoot->init(newRootId);
        newRoot->setLeftLink(headerPage->rootPageId);
        headerPage->rootPageId = newRootId;
        newRoot->insertKey((void *) &newChildEntry->key, key_type, newChildEntry->data.pageNo, holderRid);
      }
    }
  } else if (curPageType == LEAF) {
    BTLeafPage *curLeafPage = (BTLeafPage*) curPage;
    int entryLen;
    KeyDataEntry *leafEntry;
    Datatype newEntryRid;
    newEntryRid.rid = rid;
    make_entry(leafEntry, key_type, key,
                 LEAF, newEntryRid, &entryLen);
    if (curLeafPage->available_space() >= entryLen) {
      curLeafPage->insertRec(key, key_type, rid, holderRid);
      newChildEntry = NULL;
      return OK;
    } else {
      cout << "splitting leaf page" << endl;
      // page full, split leaf page to the right
      BTLeafPage *rightLeafPage;
      PageId rightLeafPageId;
      MINIBASE_BM->newPage(rightLeafPageId, (Page *&) rightLeafPage);
      rightLeafPage->init(rightLeafPageId);
      // TODO: split and shift records
      // what are we adding to this page? (key and rid)
      splitLeafPage(curLeafPage, rightLeafPage, (Keytype *) key, rid);
      // make new child entry for parent
      KeyDataEntry *newEntry;  // entry that will be added to parent page
      Datatype newEntryPage; // hold pageId of new page that was made
      newEntryPage.pageNo = rightLeafPageId;
      Keytype *rightPageFirstKey;
      // need to get the first key of the right page
      // get_first(RID& rid, void *key, PageId & pageNo)
      RID holderDataRid;
      rightLeafPage->get_first(holderRid, (void*) rightPageFirstKey, holderDataRid);
      int entryLen;
      make_entry(newEntry, key_type, rightPageFirstKey, INDEX, newEntryPage, &entryLen);
      newChildEntry = newEntry;
      return OK;
    }
  }
  return OK;
}

/**
 * Index page split AND insert
 * 1. move upper half of existing entries on left page to the (empty) right page
 *    - both pages should have same number of entries (or off by 1 if odd)
 * 2. insert the new entry onto the correct page (right if key >= first key on right)
 */
Status BTreeFile::splitIndexPage(BTIndexPage *left, BTIndexPage *right, Keytype *key, PageId pageId){
  //cout << "*****start of splitIndexPage() for BTreeFile" << endl;
  int totalRecs = left->numberOfRecords();
  int half = totalRecs / 2; // index of slot dir to start moving
  RID ridHolder;
  Keytype keyHolder;
  PageId pageHolder, leftLink;
  Status status = left->get_first(ridHolder, (void *) &keyHolder, pageHolder);
  for (int i = 0; i < half; i++) {
    status = left->get_next(ridHolder, (void *) &keyHolder, pageHolder);
    if (i == half - 2)
      leftLink = pageHolder;
  } // now we are halfway through the page
  while (status != OK) {
    status = left->deleteKey((void *) &keyHolder, headerPage->key_type, ridHolder);
    status = right->insertKey((void *) &keyHolder, headerPage->key_type, pageHolder, ridHolder);
    status = left->get_next(ridHolder, (void *) &keyHolder, pageHolder);
  }
  // need to update the left link of the right page; point to the rightmost page of the left
  right->setLeftLink(leftLink);
  // now we actually insert the rec we want to insert
  // first find if it should go on the left or right page
  status = right->get_first(ridHolder, (void *) &keyHolder, pageHolder);
  if (keyCompare((void *) &key, (void *) &keyHolder, headerPage->key_type) >= 0) {
    right->insertKey((void *) &key, headerPage->key_type, pageId, ridHolder);
  } else {
    left->insertKey((void *) &key, headerPage->key_type, pageId, ridHolder);
  }
  
  return OK;
}

/**
 * Leaf page split and insert
 * 1. move later half of entries to new right leaf
 * 2. insert the new data record onto the correct page
 * 3. update the linked list
 */
Status BTreeFile::splitLeafPage(BTLeafPage *left, BTLeafPage *right, Keytype *key, RID rid)
{
  //cout << "*****start of splitLeafPage() for BTreeFile" << endl;
  int totalRecs = left->numberOfRecords();
  int half = totalRecs / 2; // index of slot dir to start moving
  RID ridHolder;
  Keytype keyHolder;
  RID dataRidHolder;
  Status status = left->get_first(ridHolder, (void *) &keyHolder, dataRidHolder);
  for (int i = 0; i < half; i++) {
    status = left->get_next(ridHolder, (void *) &keyHolder, dataRidHolder);
  } // now we are halfway through the page
  while (status != OK) {
    status = left->deleteRecord(ridHolder);
    status = right->insertRec((void *) &keyHolder, headerPage->key_type, dataRidHolder, ridHolder);
    status = left->get_next(ridHolder, (void *) &keyHolder, dataRidHolder);
  }
  // now we actually insert the rec we want to insert
  // first find if it should go on the left or right page
  status = right->get_first(ridHolder, (void *) &keyHolder, dataRidHolder);
  if (keyCompare((void *) &key, (void *) &keyHolder, headerPage->key_type) >= 0) {
    right->insertRec((void *) &key, headerPage->key_type, rid, ridHolder);
  } else {
    left->insertRec((void *) &key, headerPage->key_type, rid, ridHolder);
  }
  // update linked list
  PageId oldNext = left->getNextPage();
  left->setNextPage(right->page_no());
  right->setPrevPage(left->page_no());
  right->setNextPage(oldNext);
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  //cout << "*****start of Delete() for BTreeFile" << endl;
  // can just open up a scan and keep deleting
  RID ridHolder;
  void *keyHolder;
  IndexFileScan *scan = new_scan(key, key);
  Status status = scan->get_next(ridHolder, keyHolder);
  if (status == DONE) return DONE;
  // else, record with matching key exists, so we delete
  while (status == OK) {
    status = scan->get_next(ridHolder, keyHolder);
    status = scan->delete_current();
  }
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  //cout << "*****start of new_scan() for BTreeFile" << endl;
  BTreeFileScan *scan = new BTreeFileScan();
  scan->lo_key = lo_key;
  scan->hi_key = hi_key;
  scan->btree = this;
  scan->key_type = headerPage->key_type;
  scan->just_deleted = false;
  scan->done = false;
  Status status = setUpScan(scan);
  if (status != OK)
    return NULL;
  return scan;
}

Status BTreeFile::setUpScan(BTreeFileScan *scan){
  //cout << "*****start of setUpScan() for BTreeFile" << endl;
  // TODO: intiialize the scan by setting the scan pointer to the first rec
  // that matches lo_key
  // if lo_key is NULL, go all the way left. else search tree
  Status status;
  PageId curPageId = headerPage->rootPageId;
  SortedPage *curPage;
  status = MINIBASE_BM->pinPage(curPageId, (Page *&) curPage, 0);
  if (status != OK)
    return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
  RID curRid;
  Keytype *curKey;
  while(curPage->get_type() == INDEX) {
    BTIndexPage *curIndexPage = (BTIndexPage *) curPage;
    status = MINIBASE_BM->unpinPage(curPageId, 0, 0);
    // in the middle of the tree, need to get to an index
    if (scan->lo_key == NULL) // go all the way to the left
      curIndexPage->get_first(curRid, (void *) &curKey, curPageId);
    else // get inner page w/ key >= lo_key
      curIndexPage->get_page_no((void *) &curKey, headerPage->key_type, curPageId);
    status = MINIBASE_BM->pinPage(curPageId, (Page *&) curPage, 0);
  }
  // now we should be at the starting leaf page
  RID curDataRid;
  BTLeafPage *curLeafPage = (BTLeafPage *) curPage;
  status = curLeafPage->get_first(curRid, &curKey, curDataRid);
  while (status == NOMORERECS) {
    // case where we might have a bunch of empty leaf pages after deletions
    MINIBASE_BM->unpinPage(curPageId, 0 , 0);
    curPageId = curLeafPage->getNextPage();
    if (curPageId == INVALID_PAGE) {
      scan->curLeafPage = NULL;
      scan->done = true; // empty scan
      return OK;
    }
    MINIBASE_BM->pinPage(curPageId, (Page *&) curLeafPage, 0);
    status = curLeafPage->get_first(curRid, &curKey, curDataRid);
  }
  if (scan->lo_key != NULL) {
    while (keyCompare(&curKey, scan->lo_key, headerPage->key_type) < 0) {
      status = curLeafPage->get_next(curRid, &curKey, curDataRid);
      if (status == NOMORERECS)
        break;
    }
  }
  scan->curLeafPage = curLeafPage;
  scan->curRid = curRid;
  return OK;
}

int BTreeFile::keysize(){
  //cout << "*****start of keysize() for BTreeFile" << endl;
  return headerPage->key_size;
}
