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

	Status status;
	//printf("in get_next\n");
	if(usedUp == true){
		//all traversed
		return DONE;
	}

Keytype* first_cur_key;
	if(getFirst==true){

		rid = leftmostRID;

		// first element
		KeyDataEntry recPtr;
		int recLen;
		SortedPage* current;
		status = MINIBASE_BM->pinPage(leftmostPage, (Page* &)current, 1);
		status = current->getRecord(rid,(char*)&recPtr,recLen);  // return value rid
		first_cur_key = (Keytype*)&recPtr;
//		memcpy(keyptr,first_cur_key,keysize());   //return value keyptr
		status = MINIBASE_BM->unpinPage(currentPage, 0, 1);
		//getFirst = false;
delFirst = true;
		//in case of exact match, only one element in scan
		if(currRID == rightmostRID) usedUp = true;
		currRID = rid;
		currentPage = rid.pageNo;
		//return OK;
		
	}

	if(currRID==rightmostRID && usedUp == false){
		//last element
		KeyDataEntry recPtr;
		int recLen;
		SortedPage* current;
		status = MINIBASE_BM->pinPage(rightmostPage, (Page* &)current, 1);
		status = current->getRecord(rid,(char*)&recPtr,recLen);  // return value rid
		Keytype* cur_key = (Keytype*)&recPtr;
		memcpy(keyptr,cur_key,keysize());   //return value keyptr
		status = MINIBASE_BM->unpinPage(currentPage, 0, 1);
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
		rid = nextRID;
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
			ifEmpty = next->empty();
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
	prevPage = currentPage;
	prevRID = currRID;

if(getFirst == true){

	temp_leftmostPage = nextPageId;
	temp_leftmostRID = rid;

	rid = leftmostRID;  // re-flush rid
	memcpy(keyptr,first_cur_key,keysize());   //re-flush value keyptr	
	getFirst = false;
}
else{
	currentPage = nextPageId;
	currRID = rid;
}

	printf("nextRIDslot: %d\n", rid.slotNo);

	return OK;
}

Status BTreeFileScan::delete_current()
{

if(currentPage==INVALID_PAGE){
return OK;  // we cannot return DONE, end should be controled by get_next_record
}

	Status status;

PageId temp_pageId = currentPage;
RID temp_RID = currRID;

	
printf("prevRID slot is: %d, prevPage is: %d\n",prevRID.slotNo,  prevPage);


if(delFirst==true){ // reset the current

leftmostPage = temp_leftmostPage;
leftmostRID = temp_leftmostRID;

printf("leftmostPage: %d, leftmostRID_slot: %d\n",leftmostPage, leftmostRID.slotNo);

RID uselessrid;
char* temp = new char[this->keysize()];
this->get_next(uselessrid, temp);
delFirst = false;
getFirst= true;

}
else{
	currRID = prevRID; // go back
	currentPage = prevPage;
}

// deletion
	SortedPage *deltarget;
	status = MINIBASE_BM->pinPage(temp_pageId, (Page* &)deltarget, 1);

if(deltarget->empty()==true){
	return OK;  // we cannot return DONE, end should be controled by get_next_record
}

	deltarget->deleteRecord(temp_RID);

	status = MINIBASE_BM->unpinPage(currentPage, 1, 0);		


	return status;
}


int BTreeFileScan::keysize() 
{
	return Keysize;
}
