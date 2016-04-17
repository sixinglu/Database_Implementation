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
    //printf("int key %d, pageno %d\n",key,pageNo);
char* rec = new char[entry_len];
get_key_data(rec,(Datatype *)&rec[entry_len - sizeof(pageNo)],&tmpEntry,entry_len,INDEX);
    printf("insert key length %d int key %d, key pageno %d\n",entry_len,((Keytype*)key)->intkey,rec[entry_len - sizeof(pageNo)]);
    insertRecord(key_type, rec, entry_len,rid);
delete[] rec;
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
    PageId prev_pointTo = INVALID_PAGE;
    status = this->firstRecord(currRID);               // read the first index RID
    do{
        KeyDataEntry recPtr;
	char* tmpPtr =(char*)&recPtr;
        int recLen;
        Keytype* cur_key = (Keytype*)&recPtr;
        status = this->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record

        pageNo = *(PageId*)&tmpPtr[recLen-sizeof(PageId)];

        
        // compare the key
        int compareResult = keyCompare(key,cur_key,key_type);
//        if(compareResult==0){  // the key already there
//            return DONE;
//        }

        if(compareResult<0){  // the insert key is smaller
            
            // this record point to the page is the next level search page
            if( prev_pointTo!=INVALID_PAGE ){
                pageNo = prev_pointTo;
            }
            else{  // key smaller than all index keys
                PageId prevlink = this->getLeftLink();
                if(prevlink != INVALID_PAGE){
                    pageNo = prevlink;
                }
                else{   // prevLink == NULL, create a new page
                    PageId NewPid;
                    status = MINIBASE_DB->allocate_page(NewPid);
                    BTIndexPage *NewIndexPage;
                    status = MINIBASE_BM->newPage(NewPid, (Page* &)NewIndexPage);
                    NewIndexPage->init(NewPid);
                    this->setLeftLink(NewPid);
                    
                    pageNo = NewPid;
                }
            }
            
            return OK;
            
        }
        // else the insert key is larger, move on to next key record
        prev_pointTo = pageNo;
        prevRID = currRID;
        
    }
    while(this->nextRecord(prevRID,currRID)==OK);
    
    pageNo = prev_pointTo;  // key larger than all index keys, choose the last one
    
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
