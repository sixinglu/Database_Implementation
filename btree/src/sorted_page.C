/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
	//OK,
	//Insert Record Failed (SortedPage::insertRecord),
	//Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */
void SortedPage::slotPrint(){
for(int i = 0; i < this->slotCnt; i++){
	if(this->slot[i].length != EMPTY_SLOT){
	printf("slot %d has offset %d, has int key %d, char key %c\n",i,\
		slot[i].offset,((Keytype*)(&data[slot[i].offset]))->intkey,((Keytype*)(&data[slot[i].offset]))->charkey
		);
		}
	}
}
Status SortedPage::insertRecord (AttrType key_type,
		char * recPtr,
		int recLen,
		RID& rid)
{
	// put your code here
	RID tmpRid;
	Status status;
	//standard insert
	status = HFPage::insertRecord(recPtr,recLen,tmpRid);
	if(status != OK){
		return MINIBASE_FIRST_ERROR( SORTEDPAGE, INSERT_REC_FAILED );
	}
	int slotTail = tmpRid.slotNo;
	//after standard insert, only the newly inserted record violates the 
	//ordering. And the newly inserted record has the largest rid.
	//So, first find the slotNo to place current record by finding slotPivot
	int slotPivot = slotTail;
	int prevSlot = 0;
	for(int i = 0; i < this->slotCnt; i++){
	//if every record > new record, slotPivot = 0
	//if every record < new record, slotPivot = tmprid.slotNo
		if(this->slot[i].length != EMPTY_SLOT){
		char* recordKey;
		recordKey = &this->data[this->slot[i].offset];
			if(keyCompare(recordKey,recPtr,key_type)){
			slotPivot = prevSlot;
			}
		prevSlot = i;
		}
	}
	//every slot after pivot points to a larger key than each other
	//yet pivot holds the minimum key, so they have to shift by one
	for(int i = slotPivot + 1; i < this->slotCnt; i++){
		if(this->slot[i].length != EMPTY_SLOT){
		//switch
		int tmpOffset = this->slot[i].offset;
		this->slot[i].offset = this->slot[slotTail].offset;
		this->slot[slotTail].offset = tmpOffset;
		}
	}
	return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
	// put your code here
	HFPage::deleteRecord(rid);
	return OK;
}

int SortedPage::numberOfRecords()
{
	// put your code here
	int i,num_records;
	for(i = 0; i < this->slotCnt; i++){
		if(this->slot[i].length != EMPTY_SLOT)
			num_records++;
	}
	return num_records;
}
