/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  "OK",
  "Insert Record Failed (SortedPage::insertRecord)",
  "Delete Record Failed (SortedPage::deleteRecord)",
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid)
{
  cout << "*****start of insertRecord() for SortedPage" << endl;
  Status status = HFPage::insertRecord(recPtr, recLen, rid);
  if (status != OK) return status;
  int curSlot = rid.slotNo;
  int leftSlot;
  // swap left to maintain sorted order, worst-case O(n)
  while (curSlot > 0) {
    leftSlot = curSlot - 1;
    char *curKey = data + slot[curSlot].offset;
		char *leftKey = data + slot[leftSlot].offset;
    if (keyCompare((void*)curKey, (void*)leftKey, key_type) < 0) {
      slot_t tmp_slot;
      tmp_slot  = slot[curSlot];
      slot[curSlot]  = slot[leftSlot];
      slot[leftSlot] = tmp_slot;
      rid.slotNo = leftSlot;
    }
    else break;
    curSlot = rid.slotNo;
  }
  return OK;
}

/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
  cout << "*****start of deleteRecord() for SortedPage" << endl;
  Status status = HFPage::deleteRecord(rid);
  if (status != OK)
    return MINIBASE_FIRST_ERROR(SORTEDPAGE, DELETE_REC_FAILED);
  // remove a hole if we delete a record to make things easier later
  HFPage::removeSlotHoles();
  return status;
}

int SortedPage::numberOfRecords()
{
  return slotCnt;
}
