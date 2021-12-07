/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  "OK" // OK
  "Record insertion failed" // INSERT_FAILED
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
  //cout << "*****start of insertKey() for BTIndexPage" << endl;
  KeyDataEntry entry;
  Datatype data;
  data.pageNo = pageNo;
  int entry_len;
  make_entry(&entry, key_type, key, INDEX, data, &entry_len);
  Status status = SortedPage::insertRecord(key_type, (char*)&entry, entry_len, rid);
  if (status != OK) {
    return MINIBASE_FIRST_ERROR(BTINDEXPAGE, INSERT_REC_FAILED);
  }
  return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
  //cout << "*****start of deleteKey() for BTIndexPage" << endl;
  // given the key, need to find the RID on the page
  // then need to delete the record
  void *curKey;
  PageId curPageId;
  Status status = get_first(curRid, curKey, curPageId);
  if (status == NOMORERECS) return DONE;
  while (keyCompare(curKey, key, key_type) != 0) {
    status = get_next(curRid, curKey, curPageId);
    // if we don't find the key on the page, return DONE
    if (status == NOMORERECS) return DONE;
  }
  // we should be at the right record here
  status = SortedPage::deleteRecord(curRid);
  return status;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
  //cout << "*****start of get_page_no() for BTIndexPage" << endl;
  // search routine from book
  void *curKey; // tracks key in the page
  PageId curPageId;
  PageId prevPageId;
  RID curRid;
  Status status = get_first(curRid, curKey, curPageId);
  if (status == NOMORERECS) return DONE;
  int comp = keyCompare(curKey, key, key_type);
  if (comp > 0) {
    pageNo = getLeftLink();
    return OK;
  }
  // pageId of rec w/ key is for page with recs whose keys are >= key
  while (comp <= 0) {
    prevPageId = curPageId;
    status = get_next(curRid, curKey, curPageId);
    if (status == NOMORERECS) {
      pageNo = prevPageId; // rightmost link
      return OK;
    }
    comp = keyCompare(curKey, key, key_type);
  }
  // we should be one ahead here, so return previous
  pageNo = prevPageId;
  return status;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
  //cout << "*****start of get_first() for BTIndexPage" << endl;
  Status status = HFPage::firstRecord(rid);
  if (status == DONE)
    return NOMORERECS;
  // else, unpack the record into key and data pair
  curIterRid = rid;
  char* recPtr; 
  int recLen;
  HFPage::returnRecord(rid, recPtr, recLen);
  get_key_data(key, (Datatype *) &pageNo, (KeyDataEntry *) recPtr, recLen, INDEX);
  return status;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
  //cout << "*****start of get_next() for BTIndexPage" << endl;
  Status status = HFPage::nextRecord(curIterRid, rid);
  if (status == DONE)
    return NOMORERECS;
  // else, unpack the record into key and data pair
  curIterRid = rid;
  char* recPtr; 
  int recLen;
  HFPage::returnRecord(rid, recPtr, recLen);
  get_key_data(key, (Datatype *) &pageNo, (KeyDataEntry *) recPtr, recLen, INDEX);
  return OK;
}
