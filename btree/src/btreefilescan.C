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


Status BTreeFileScan::get_next(RID & rid_input, void* keyptr)
{
RID rid_tmp;
	Status status;
	//printf("in get_next\n");
	if(usedUp == true){
		//all traversed
		return DONE;
	}
	if(getFirst==true){
		rid_tmp = leftmostRID;
		// first element
		KeyDataEntry recPtr;
		int recLen;
		SortedPage* current;
		status = MINIBASE_BM->pinPage(leftmostPage, (Page* &)current, 1);
		status = current->getRecord(rid_tmp,(char*)&recPtr,recLen);  // return value rid
		Keytype* cur_key = (Keytype*)&recPtr;
		memcpy(keyptr,cur_key,keysize());   //return value keyptr
		status = MINIBASE_BM->unpinPage(currentPage, 0, 1);
		getFirst = false;

		//in case of exact match, only one element in scan
		if(currRID == rightmostRID) usedUp = true;
		currRID = rid_tmp;
		currentPage = rid_tmp.pageNo;
		char* tmpPtr =(char*)&recPtr;
rid_input = *(RID*)&tmpPtr[recLen-sizeof(RID)];
		return OK;
	}

	if(currRID==rightmostRID && usedUp == false){
		//last element
		KeyDataEntry recPtr;
		int recLen;
		SortedPage* current;
		status = MINIBASE_BM->pinPage(rightmostPage, (Page* &)current, 1);
		status = current->getRecord(currRID,(char*)&recPtr,recLen);  // return value rid
		Keytype* cur_key = (Keytype*)&recPtr;
		memcpy(keyptr,cur_key,keysize());   //return value keyptr
		status = MINIBASE_BM->unpinPage(currentPage, 0, 1);
		char* tmpPtr =(char*)&recPtr;
rid_input = *(RID*)&tmpPtr[recLen-sizeof(RID)];
		usedUp = true;
		return DONE;
	}


	SortedPage *current;
	status = MINIBASE_BM->pinPage(currentPage, (Page* &)current, 1);

	// get next RID
	RID nextRID;
	PageId nextPageId = INVALID_PAGE;
	SortedPage *next;  
	if(current->nextRecord(currRID,nextRID)==OK){
		rid_tmp = nextRID;
		nextPageId = currentPage;
		next = current;
	}
	else{ 
		bool ifEmpty = true; 
		while(ifEmpty==true){
			nextPageId = current->getNextPage();

			if(nextPageId == INVALID_PAGE){  // reach the end
				return DONE;
			}
			status = MINIBASE_BM->pinPage(nextPageId, (Page* &)next, 1);
			next->firstRecord(rid_tmp);
			ifEmpty = next->empty();
			status = MINIBASE_BM->unpinPage(nextPageId, 0, 1);
		}

		status = MINIBASE_BM->pinPage(nextPageId, (Page* &)next, 1);
		next->firstRecord(rid_tmp);
	} 

	// get the key
	KeyDataEntry recPtr;
	int recLen;
	status = next->getRecord(rid_tmp,(char*)&recPtr,recLen);  // return value rid
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
	currRID = rid_tmp;

	//printf("nextRIDslot: %d\n", rid.slotNo);
memcpy(&rid_input,&tmpPtr[recLen-sizeof(RID)],sizeof(RID));
	return OK;
}

Status BTreeFileScan::delete_current()
{
	// do I need to delete the key as well?

	Status status;

	SortedPage *current;
	status = MINIBASE_BM->pinPage(currentPage, (Page* &)current, 1);

	current->deleteRecord(currRID);
	if(currRID == rightmostRID)
		usedUp = true;
	status = MINIBASE_BM->unpinPage(currentPage, 1, 0);

	return status;
}


int BTreeFileScan::keysize() 
{
	return Keysize;
}
