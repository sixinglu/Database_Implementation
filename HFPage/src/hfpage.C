/******************************************************
 * Author:  Yang Liu, Sixing Lu
 * Date: 2016-02-20
 ******************************************************/

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
	// fill in the body
	this->nextPage = INVALID_PAGE; 
	this->prevPage = INVALID_PAGE;
	this->slotCnt = 0;
	this->curPage = pageNo;
	this->usedPtr = MAX_SPACE - DPFIXED - 1;
	this->freeSpace = MAX_SPACE - DPFIXED;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
	int i;

	cout << "dumpPage, this: " << this << endl;
	cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
	cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
		<< ", slotCnt=" << slotCnt << endl;

	for (i=0; i < slotCnt; i++) {
		cout << "slot["<< i <<"].offset=" << slot[i].offset
			<< ", slot["<< i << "].length=" << slot[i].length << endl;
	}
}

// **********************************************************
PageId HFPage::getPrevPage()
{
	// fill in the body

	return this->prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{

	// fill in the body
	this->prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
	// fill in the body
	return this->nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
	// fill in the body
	this->nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
	// fill in the body
//printf("insert recLen %d, space %d\n",recLen,this->freeSpace);

	//enough space?
	if((this->freeSpace - (int)sizeof(slot_t)) < recLen) return DONE;
	//enough space!
	this->freeSpace -= recLen;
	int targetSlot = -1;
	for(int i = 0; i < this->slotCnt; i++)
		if(this->slot[i].length == EMPTY_SLOT)
		{targetSlot = i;
			break;
		}
	if (targetSlot == -1)
	{targetSlot = this->slotCnt;
		this->slotCnt++;
		this->freeSpace -= sizeof(slot_t);
	}
	this->slot[targetSlot].length = recLen;
	this->slot[targetSlot].offset = this->usedPtr - recLen + 1;
	this->usedPtr -= recLen;
	memcpy(&this->data[this->slot[targetSlot].offset],recPtr,recLen);

	rid.pageNo = this->curPage;
	rid.slotNo = targetSlot;
	//char* tmp = &this->data[this->slot[targetSlot].offset];

	return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
	// fill in the body
	int targetSlot = rid.slotNo;
	//non-existent rid?
	if(this->slot[targetSlot].length == EMPTY_SLOT || targetSlot < 0 || targetSlot >= slotCnt) return FAIL;
	//valid rid
	int holeSize = this->slot[targetSlot].length;
	int holeOffset = this->slot[targetSlot].offset;
	this->slot[targetSlot].length = EMPTY_SLOT;
	//dumpPage();
	//printf("target offset %d target slot %d, used %d\n move length %d\n",holeOffset,targetSlot,this->usedPtr+1,holeOffset - this->usedPtr + 1);
	//compacting data hole
	memmove(&this->data[this->usedPtr +holeSize + 1],&this->data[this->usedPtr+1],holeOffset - this->usedPtr - 1);
	this->usedPtr += holeSize;
	//compacting slot hole

	if (targetSlot == this->slotCnt - 1)
	{slot_t* tail = &this->slot[targetSlot];
		while(tail->length == EMPTY_SLOT && this->slotCnt >= 0){
			this->slotCnt--;
			tail--;
			this->freeSpace += sizeof(slot_t);
		}

	}
	for(int j = 0 ; j < this->slotCnt; j++)
	{if (this->slot[j].length != EMPTY_SLOT && this->slot[j].offset < holeOffset)
		this->slot[j].offset += holeSize;
	}
	this->freeSpace += holeSize;

	return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
	// fill in the body
	for(int j = 0 ; j < this->slotCnt; j++)
	{if (this->slot[j].length != EMPTY_SLOT)
		{firstRid.pageNo = this->curPage;
			firstRid.slotNo = j;
			return OK;
		}
	}
	return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
	// fill in the body
	int curSlot = curRid.slotNo;
	if (curSlot < 0 || curSlot >= slotCnt) return FAIL;

	for(int j = curSlot + 1; j < this->slotCnt; j++)
	{if (this->slot[j].length != EMPTY_SLOT)
		{nextRid.pageNo = this->curPage;
			nextRid.slotNo = j;
//printf("next sucker pno %d, sno %d, upbd %d\n",this->curPage,j,this->slotCnt);
			return OK;
		}
	}
	//dumpPage();
	return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
	// fill in the body
	int targetSlot = rid.slotNo;
	recLen = this->slot[targetSlot].length;
	memcpy(recPtr,&this->data[this->slot[targetSlot].offset],this->slot[targetSlot].length);
/*printf("get pno %d, sno %d, data offset %d\n",rid.pageNo,rid.slotNo,this->slot[targetSlot].offset);
for(int m = 0; m < recLen; m++)
printf("%d ",recPtr[m]);
printf("\n");
*/
	return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
	// fill in the body

	int targetSlot = rid.slotNo;
//printf("return pno %d, sno %d, data offset %d\n",rid.pageNo,rid.slotNo,this->slot[targetSlot].offset);
	recLen = this->slot[targetSlot].length;
	recPtr = &this->data[this->slot[targetSlot].offset];
//for(int m = 0; m < recLen; m++)
//printf("%d ",recPtr[m]);
//printf("\n");
	return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
	// fill in the body
	return this->freeSpace - sizeof(slot_t);
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
	for(int j = 0 ; j < this->slotCnt; j++)
		if (this->slot[j].length != EMPTY_SLOT)
			return 0;
	return 1;


}



