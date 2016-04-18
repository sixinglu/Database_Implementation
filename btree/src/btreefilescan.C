/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::~BTreeFileScan()
{
    // not 100% sure
//    delete this;
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{

printf("in get_next\n");
  
    if(currRID==rightmostRID){
	return DONE;
    }

    Status status;
    
    SortedPage *current;
    status = MINIBASE_BM->pinPage(currentPage, (Page* &)current, 1);
    
    // get next RID
    RID nextRID;
    PageId nextPageId = INVALID_PAGE;
    SortedPage *next;  
    if(current->nextRecord(currRID,nextRID)==OK){
       rid = nextRID;
       nextPageId = currentPage;
       next = current;
    }
    else{ 
       bool ifEmpty = false; 
       while(ifEmpty!=true){
            nextPageId = current->getNextPage();
	
       	    if(nextPageId == INVALID_PAGE){  // reach the end
		return DONE;
	    }
	    status = MINIBASE_BM->pinPage(nextPageId, (Page* &)next, 1);
	    ifEmpty = next->empty();
	    status = MINIBASE_BM->unpinPage(nextPageId, 0, 0);
	}
    	
	status = MINIBASE_BM->pinPage(nextPageId, (Page* &)next, 1);
        next->firstRecord(rid);
    } 

    // get the key
    KeyDataEntry recPtr;
    int recLen;
    status = next->getRecord(rid,(char*)&recPtr,recLen);  // return value rid
    char* tmpPtr =(char*)&recPtr;
    Keytype* cur_key = (Keytype*)&recPtr;
    memcpy(keyptr,cur_key,keysize());   //return value keyptr
    
    // unpin
    status = MINIBASE_BM->unpinPage(currentPage, 0, 1);
    if(currentPage!=nextPageId){
       status = MINIBASE_BM->unpinPage(nextPageId, 0, 1);
    }
    
    // update
    currentPage = nextPageId;
    currRID = rid;

printf("nextRIDslot: %d\n", rid.slotNo);
    
    return OK;
}

Status BTreeFileScan::delete_current()
{
    // do I need to delete the key as well?
    
    Status status;
    
    SortedPage *current;
    status = MINIBASE_BM->pinPage(currentPage, (Page* &)current, 1);
    
    current->deleteRecord(currRID);
    status = MINIBASE_BM->unpinPage(currentPage, 1, 0);
    
    return status;
}


int BTreeFileScan::keysize() 
{
    return Keysize;
}
