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
  returnStatus = MINIBASE_BM->pinPage(headerPageId, Page(*&) headerPage);
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
    ((HFPage *)headerPage)->init(headerPageId);
    SortedPage *rootPage;
    MINIBASE_BM->newPage(headerPage->root, rootPage);
    rootPage->init(headerPage->root);
    rootPage->setType(LEAF);
    headerPage->key_type = keytype;
    headerPage->key_size = keysize;
  } else {
    returnStatus = MINIBASE_BM->pinPage(headerPageId, Page(*&) headerPage);
    if (returnStatus != OK) {
      returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_HEADER);
      return;
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
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
  return NULL;
}

int keysize(){
  // put your code here
  return 0;
}
