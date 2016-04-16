/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation
 */

#include "btindex_page.h"
#include "buf.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
    //Possbile error messages,
    //OK,
    //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
    // put your code here
    KeyDataEntry tmpEntry;
    int entry_len;
    Datatype dtype;
    dtype.pageNo = pageNo;
    nodetype ndtype = INDEX;
    make_entry(&tmpEntry,key_type,key,ndtype,dtype,
               &entry_len);
    
    insertRecord(key_type, (char *)&tmpEntry, entry_len,rid);
    return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
    // put your code here
    Status status;
    RID currRID, prevRID;
    status = this->firstRecord(currRID);               // read the first index RID
    do{
        KeyDataEntry recPtr;
        int recLen;
        
        status = this->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record
        
        // read the key from entry
        void *targetkey;  // the key in the tree
        Datatype *targetdata;
        get_key_data(targetkey, targetdata, &recPtr, recLen, INDEX); // must be index
        
        
        // compare the key
        int compareResult = keyCompare(key,targetkey,key_type);
        if(compareResult==0){  // the key already there, so delete it
            deleteRecord (currRID);
            return OK;
        }
        else if(compareResult<0){  // the insert key is smaller
            // don't want to delete it
            return DONE;
        }
        // else the insert key is larger, move on to next key record
        
        prevRID = currRID;
        
    }
    while(this->nextRecord(prevRID,currRID)==OK);
    
    return status;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
    Status status = OK;

    
    // if this page is deleted
    if(this->empty()==true){
        return OK;
    }
    
    // read the key inside index record one by one
    RID currRID, prevRID;
    status = this->firstRecord(currRID);               // read the first index RID
    do{
        KeyDataEntry recPtr;
        int recLen;
        
        status = this->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record
        
        // read the key from entry
        KeyDataEntry targetkey;  // the key in the tree
        Datatype targetdata;
        get_key_data(&targetkey, &targetdata, &recPtr, recLen, INDEX); // must be index
        
        // compare the key
        int compareResult = keyCompare(key,&targetkey.key,key_type);
//        if(compareResult==0){  // the key already there
//            return DONE;
//        }
            printf("compare result %d\n",compareResult);
        if(compareResult<0){  // the insert key is smaller
            printf("page no found\n");
            // this record point to the page is the next level search page
            pageNo = targetdata.pageNo;
            return OK;
            
        }
        // else the insert key is larger, move on to next key record
        
        prevRID = currRID;
        
    }
    while(this->nextRecord(prevRID,currRID)==OK);
    
    return status;
}


Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
    // put your code here
Status status;
    RID currRID;
    status = this->firstRecord(currRID);   
if(status != OK){
	printf("no record on first record\n");
	return DONE;
}
KeyDataEntry recPtr;
        int recLen;
        
        status = this->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record
Datatype tmpID;
get_key_data(key, &tmpID,&recPtr, recLen, INDEX);
pageNo = tmpID.pageNo;
rid = currRID;
  return status;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
    // put your code here
Status status;
KeyDataEntry recPtr;
        int recLen;
        status = this->getRecord(rid,(char*)&recPtr,recLen);
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
pageNo = tmpID.pageNo;
    return OK;
}
