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
    KeyDataEntry tmpEntry;
    int entry_len;
    Datatype dtype;
    dtype.rid = dataRid;
    nodetype ndtype = LEAF;
    make_entry(&tmpEntry,key_type,key,ndtype,dtype,
               &entry_len);
char* rec = new char[entry_len];
get_key_data(rec,(Datatype *)&rec[entry_len - sizeof(RID)],&tmpEntry,entry_len,LEAF);
    printf("insert rec int key %d, datarid pageno %d, datarid slotno %d\n",((Keytype*)key)->intkey,dataRid.pageNo,dataRid.slotNo);
    insertRecord (key_type, rec, entry_len,rid);
delete[] rec;
    return OK;
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
  // put your code here
   Status status = OK;
    
    // if this page is deleted
    if(this->empty()==true){
        return OK;
    }
    
    // read the key inside leaf record one by one
    RID currRID, prevRID;
    status = this->firstRecord(currRID);               // read the first index RID
    do{
        KeyDataEntry recPtr;
        int recLen;
        
        status = this->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record
        
        // read the key from entry
        Keytype *targetkey;  // the key in the tree
        Datatype targetdata;
        get_key_data(&targetkey, &targetdata, &recPtr, recLen, INDEX); // must be index
        
        // compare the key
        int compareResult = keyCompare(key,targetkey,key_type);
//        if(compareResult==0){  // the key already there
//            return DONE;
//        }
        if(compareResult<0){  // the insert key is smaller
            
            // this record point to the page is the next level search page
           dataRid = targetdata.rid;
            return OK;
            
        }
        // else the insert key is larger, move on to next key record
        
        prevRID = currRID;
        
    }
    while(this->nextRecord(prevRID,currRID)==OK);
    
    return status;
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
Status status;
    RID currRID;
    status = this->firstRecord(currRID);   
if(status != OK){
	printf("no record on first record");
	return DONE;
}
KeyDataEntry recPtr;
        int recLen;
        
        status = this->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record
Datatype tmpID;
get_key_data(key, &tmpID,&recPtr, recLen, LEAF);
dataRid = tmpID.rid;
rid = currRID;
  return status;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
Status status;
KeyDataEntry recPtr;
        int recLen;
        status = this->getRecord(rid,(char*)&recPtr,recLen);
if(status != OK){
	printf("no record on this record\n");
	return DONE;
}
RID nextRid;
	status = this->nextRecord(rid, nextRid);
if(status != OK){
	printf("no record on next record\n");
	return DONE;
}
        status = this->getRecord(nextRid,(char*)&recPtr,recLen);
rid = nextRid;
Datatype tmpID;
get_key_data(key, &tmpID,&recPtr, recLen, INDEX);
dataRid = tmpID.rid;
  return OK;
}
