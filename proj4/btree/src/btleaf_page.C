/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btleaf_page.h"

const char* BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
  //cout << "*****start of insertRec() for BTLeafPage" << endl;
  KeyDataEntry entry;
  Datatype data;
  data.rid = dataRid;
  int entry_len;
  make_entry(&entry, key_type, key, LEAF, data, &entry_len);
  Status status = SortedPage::insertRecord(key_type, (char*)&entry, entry_len, rid);
  if (status != OK)
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, INSERT_REC_FAILED);
  return status;
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
  //cout << "*****start of get_data_rid() for BTLeafPage" << endl;
  int low = 0;
  int high = slotCnt - 1;
  int mid, comp;
  KeyDataEntry* entry;
  while (low <= high) {
    mid = (high - low) / 2;
    entry = (KeyDataEntry *) data + slot[mid].offset;
    comp = keyCompare(key, (void *) entry, key_type);
    if (comp < 0)
      low = mid + 1;
    else if (comp > 0)
      high = mid - 1;
    else {
      void *keyHolder;
      get_key_data(keyHolder, (Datatype *) &dataRid, entry, slot[mid].length, LEAF);
      return OK;
    }
  }
  return RECNOTFOUND;
}


/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 
  // infinite loop
  cout << "*****start of get_first() for BTLeafPage" << endl;
  Status status = HFPage::firstRecord(rid);
  if (status == DONE)
    return NOMORERECS;
  // else, unpack the record into key and data pair
  curIterSlot = 0;
  int recLen;
  recLen = slot[curIterSlot].length;
  KeyDataEntry *entryPointer = (KeyDataEntry *) data + slot[curIterSlot].offset;
  get_key_data(key, (Datatype *) &dataRid, entryPointer, recLen, LEAF);
  return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
  cout << "*****start of get_next() for BTLeafPage" << endl;
  // we are now directly using the slot array instead of HFPage
  // because SortedPage messes up the RIDs of the page entries
  curIterSlot += 1;
  if (curIterSlot >= slotCnt)
    return NOMORERECS;
  rid.pageNo = curPage;
  rid.slotNo = curIterSlot;
  // else, unpack the record into key and data pair
  int recLen;
  recLen = slot[curIterSlot].length;
  KeyDataEntry *entryPointer = (KeyDataEntry *) data + slot[curIterSlot].offset;
  get_key_data(key, (Datatype *) &dataRid, entryPointer , recLen, LEAF);
  return OK;
}
