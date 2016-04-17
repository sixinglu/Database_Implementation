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
    delete this;
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
    
    Status status;
    
  // put your code here
    SortedPage *current;
    status = MINIBASE_BM->pinPage(currentPage, (Page* &)current, 1);
    
    PageId nextPageId = current->getNextPage();
    SortedPage *next;
    status = MINIBASE_BM->pinPage(nextPageId, (Page* &)next, 1);
    
    // read according record
    char *recPtr;
    int recLen;
    status = next->getRecord(rid,recPtr,recLen);  // return value rid
    
    // get the key
    Datatype *targetdata;
    get_key_data(keyptr, targetdata, (KeyDataEntry *)recPtr, recLen, INDEX); //return value keyptr
    
    // unpin
    status = MINIBASE_BM->unpinPage(currentPage, 0, 1);
    status = MINIBASE_BM->unpinPage(nextPageId, 0, 1);
    
    // update
    currentPage = nextPageId;
    currRID = rid;
    
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
