/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
  //cout << "*****start of keyCompare() for key" << endl;
  Keytype* keytype1 = (Keytype*)key1;
  Keytype* keytype2 = (Keytype*)key2;
  if (t == attrInteger)
    return keytype1->intkey - keytype2->intkey;
  else if (t == attrString)
    return strcmp(keytype1->charkey, keytype2->charkey);
  else {
    // TODO: error, assume keys are either string or int types in this project
    return 0;
  }
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
  //cout << "*****start of make_entry() for key" << endl;
  target = new KeyDataEntry();
  // copy the key based on int or char
  int key_len = get_key_length(key, key_type);
  if (key_type == attrInteger)
    memcpy(&target->key.intkey, key, key_len);
  else if (key_type == attrString)
    memcpy(&target->key.charkey, key, key_len);
  // copy the data based on index or leaf
  int data_len;
  if (ndtype == INDEX) {
    data_len = sizeof(PageId);
    memcpy(&target->data.pageNo, &data.pageNo, data_len);
  } else if (ndtype == LEAF) {
    data_len = sizeof(RID);
    memcpy(&target->data.rid, &data.rid, data_len);
  }
  *pentry_len = key_len + data_len;
  return;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
  //cout << "*****start of get_key_data() for key" << endl;
  int data_len;
  if (ndtype == INDEX) // index page, so record is of form (key, pageNo)
    data_len = sizeof(PageId);
  else if (ndtype == LEAF) // leaf page, so record is of form (key, RID)
    data_len = sizeof(RID);
  int key_len = entry_len - data_len;
  if (key_len == sizeof(int))
    memcpy(targetkey, &psource->key.intkey, key_len);
  else {// char key
    key_len = get_key_length(&psource->key.charkey, attrString);
    memcpy(targetkey, &psource->key.charkey, key_len);
  }
  if (ndtype == INDEX)
    memcpy(&targetdata->pageNo, &psource->data.pageNo, data_len);
  else
    memcpy(&targetdata->rid, &psource->data.rid, data_len);
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
  //cout << "*****start of get_key_length() for key" << endl;
  if (key_type == attrInteger)
    return sizeof(int);
  else if (key_type == attrString)
    return strlen((char*)key) + 1 ;
  else {
    // TODO: error, assume keys are either string or int types in this project
    return 0;
  }
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
  //cout << "*****start of get_key_data_length() for key" << endl;
 int key_length = get_key_length(key, key_type);
 if (ndtype == INDEX) // index page, so record is of form (key, pageNo)
   return key_length + sizeof(PageId);
 else if (ndtype == LEAF) // leaf page, so record is of form (key, RID)
   return key_length + sizeof(RID);
 else // error, must be either index or leaf
   return 0;
}
