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
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
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
  Status status = HFPage::insertRecord(recPtr, recLen, rid);
  if (status != OK) return status;
  int curSlot, leftmost, rightmost;
  // swap left or right to maintain sorted order
  while (true) {
    curSlot = rid.slotNo;
    leftmost = curSlot - 1;
    rightmost = curSlot + 1;
    while (leftmost >= 0 && slot[leftmost].length == EMPTY_SLOT)
      leftmost --; // skip empty slots
    while (rightmost < slotCnt && slot[rightmost].length == EMPTY_SLOT)
      rightmost ++; // skip empty slots
    char *curKey = data + slot[curSlot].offset;
    char *rightKey = data + slot[rightmost].offset;
		char *leftKey = data + slot[leftmost].offset;
    if (leftmost >= 0 && keyCompare((void*)curKey, (void*)leftKey, key_type) < 0) {
      slot_t tmp_slot;
      tmp_slot  = slot[curSlot];
      slot[curSlot]  = slot[leftmost];
      slot[leftmost] = tmp_slot;
      rid.slotNo = leftmost;
    }
    else if (rightmost < slotCnt && keyCompare((void*)curKey, (void*)rightKey, key_type) > 0) {
      slot_t tmp_slot;
      tmp_slot  = slot[curSlot];
      slot[curSlot]  = slot[rightmost];
      slot[rightmost] = tmp_slot;
      rid.slotNo = rightmost;
    }
    else break;
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
  return HFPage::deleteRecord(rid);
}

int SortedPage::numberOfRecords()
{
  int count = 0;
  for (int i = 0; i < slotCnt; i++) {
    if (slot[i].length != EMPTY_SLOT)
      count ++;
  }
  return count;
}
