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
  return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
  // put your code here
  return OK;
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
        char *recPtr;
        int recLen;
        
        status = this->getRecord(currRID,recPtr,recLen);   // read the first index record
        
        // read the key from entry
        void *targetkey;  // the key in the tree
        Datatype *targetdata;
        get_key_data(targetkey, targetdata, (KeyDataEntry *)recPtr, recLen, INDEX); // must be index
        
        
        // if the target key is deleted
        // ...
        
        // compare the key
        int compareResult = keyCompare(key,targetkey,key_type);
        if(compareResult==0){  // the key already there
            return DONE;
        }
        else if(compareResult<0){  // the insert key is smaller
            
            // this record point to the page is the next level search page
            pageNo = targetdata->pageNo;
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
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
  // put your code here
  return OK;
}
