/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
	// Possible error messages
	"OK", // 0
	"CANT_FIND_HEADER", // 1
	"CANT_PIN_HEADER",  // 2
	"CANT_ALLOC_HEADER", // 3
	"CANT_ADD_FILE_ENTRY", // 4
	"CANT_UNPIN_HEADER", // 5
	"CANT_PIN_PAGE", // 6
	"CANT_UNPIN_PAGE", // 7
	"INVALID_SCAN", // 8
	"SORTED_PAGE_DELETE_CURRENT_FAILED", // 9
	"CANT_DELETE_FILE_ENTRY", // 10
	"CANT_FREE_PAGE", // 11
	"CANT_DELETE_SUBTREE", // 12
	"KEY_TOO_LONG", // 13
	"INSERT_FAILED", // 14
	"COULD_NOT_CREATE_ROOT",  // 15
	"DELETE_DATAENTRY_FAILED", // 16
	"DATA_ENTRY_NOT_FOUND", // 17
	"CANT_GET_PAGE_NO", // 18
	"CANT_ALLOCATE_NEW_PAGE", // 19
	"CANT_SPLIT_LEAF_PAGE", // 20
	"CANT_SPLIT_INDEX_PAGE", // 21
	"CANT_FIND_FILE"  // 22
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
	// if the file not exist
PageId headPageId;
	if(MINIBASE_DB->get_file_entry(filename,headPageId)!=OK){
		cout<<BtreeErrorMsgs[22]<<endl;
		returnStatus =  DONE;
	}
	else{  // if the file does exist
MINIBASE_BM->pinPage( headPageId, (Page* &)headerpage, 1 );
printf("headerpage's root: %d\n",headerpage.rootPageID);
		returnStatus =  OK;
	}

}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
		const AttrType keytype,
		const int keysize)
{
	// if the file exist
PageId headPageId;
	if(MINIBASE_DB->get_file_entry(filename,headPageId)==OK){
MINIBASE_BM->pinPage( headPageId, (Page* &)headerpage, 1 );
		returnStatus = OK;
	}
	else{  // create a new file

		// allocate space for header page, add file entry
		PageId headPageId;
		returnStatus = MINIBASE_BM->newPage(headPageId, (Page* &)headerpage);
		returnStatus = MINIBASE_DB->add_file_entry(filename,headPageId);	

		// allocate space for root page
		SortedPage* newrootPage;  // = new HFPage();
		returnStatus = MINIBASE_BM->newPage(headerpage.rootPageID, (Page* &)newrootPage);
		newrootPage->init(headerpage.rootPageID);
		newrootPage->set_type(LEAF);

		// set the root page parameters
		headerpage.keytype = keytype;
		headerpage.keysize = keysize;
		headerpage.PageType = LEAF;   // set root as LEAF, for search algorithm
		strcpy(headerpage.filename,filename);

		returnStatus = MINIBASE_BM->unpinPage(headerpage.rootPageID, 1, 1);
	}
}

BTreeFile::~BTreeFile ()
{
	MINIBASE_BM->flushAllPages();
}

Status BTreeFile::destroyFile ()
{
	Status status;

	// traverse all key and record and delete all of them
	destroyFile_Helper(headerpage.rootPageID);
	status = MINIBASE_DB->delete_file_entry(headerpage.filename);
	// delete headerpage.filename;

	return OK;
}

Status BTreeFile::destroyFile_Helper(PageId currPageId)
{
	Status status = OK;

	if(currPageId==INVALID_PAGE){
		return OK;
	}

	SortedPage* currIndex;
	status = MINIBASE_BM->pinPage( currPageId, (Page* &)currIndex, 1 );

	// delete what's inside it
	if(currIndex->empty()!=true){

		// traverse records in this page
		RID currRID, prevRID;
		prevRID.pageNo = INVALID_PAGE;
		status = currIndex->firstRecord(currRID);               // read the first index RID
		do{
			// delete record
			if(prevRID.pageNo != INVALID_PAGE){
				status = currIndex->deleteRecord(prevRID);
			}

			char *recPtr;
			int recLen;

			status = currIndex->getRecord(currRID,recPtr,recLen);   // read the first index record
			nodetype ndtype = (nodetype)currIndex->get_type();

			// read the key from entry
			void *targetkey;  // the key in the tree
			Datatype *targetdata;

			if(ndtype==INDEX){  // index node
				get_key_data(targetkey, targetdata, (KeyDataEntry *)recPtr, recLen, INDEX);
				destroyFile_Helper(targetdata->pageNo); // recursively call delete_helper
			}
			else{ // leaf node
				get_key_data(targetkey, targetdata, (KeyDataEntry *)recPtr, recLen, LEAF);
			}

			prevRID = currRID;

		}
		while(currIndex->nextRecord(prevRID,currRID)==OK);
		// delete the last one
		if(prevRID.pageNo != INVALID_PAGE){
			status = currIndex->deleteRecord(prevRID);
		}
	}

	// unpin and deallocate the current page
	status = MINIBASE_BM->unpinPage(currPageId, 1, 1);
	status = MINIBASE_DB->deallocate_page(currPageId);

	return status;
}

Status BTreeFile::insert(const void *key, const RID rid)
{

	Status status;
	static int insert_count = 0;
	insert_count++;

	// if it is very beginning, the root is empty
	SortedPage* rootPage;
	status = MINIBASE_BM->pinPage( headerpage.rootPageID, (Page* &)rootPage, 1 );
	if(rootPage->empty()==true){
		RID returnRid;
		status = ((BTLeafPage*)rootPage)->insertRec(key, headerpage.keytype, rid, returnRid);
		if(status != OK){
			printf("insertRec zero failed\n");
			return DONE;
		}
		status = MINIBASE_BM->unpinPage(headerpage.rootPageID, 1, 1);
		return status;
	}
	status = MINIBASE_BM->unpinPage(headerpage.rootPageID, 1, 1);

	// search the insert location
//	PageId result= INVALID_PAGE;
//	printf("headerpage.rootPageID %d\n",headerpage.rootPageID);
//	status = Search_record(result, headerpage.rootPageID, *(Keytype*)key); // from root always

	// call helper function
	Datatype insertdata;
	insertdata.rid = rid;
    PageId leftchild = INVALID_PAGE;
    Keytype upkey;

    Search_Insert_Helper(headerpage.rootPageID, *(Keytype*)key, insertdata, leftchild, upkey);
printf("calling helper %d\n",insert_count);
	return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid)
{
	Status status = OK;
	PageId indexPage=INVALID_PAGE;
	PageId leafPage=INVALID_PAGE;


	Search_index(indexPage, leafPage, headerpage.rootPageID, *(Keytype*)key );  // find the delete targets

	if(leafPage==INVALID_PAGE){
		cout<<"cannot find delete key!"<<endl;
	        return DONE;
	}

	if( ADVANCED_DELTE==0 ){  // if it is the root, just simply delete

		/**************** basic delete without redistribution or merging ******************/

		BTLeafPage* deleteLeaf;
		status = MINIBASE_BM->pinPage( leafPage, (Page* &)deleteLeaf, 1 );

		if(leafPage!=headerpage.rootPageID){

			// re-connect leaf link
			PageId prev =deleteLeaf->getPrevPage();
			PageId next =deleteLeaf->getNextPage();

			SortedPage *prevPage, *nextPage;
			if(prev!=INVALID_PAGE){
			     status = MINIBASE_BM->pinPage(prev, (Page* &)prevPage, 1);
			     prevPage->setNextPage(next);
	  		     status = MINIBASE_BM->unpinPage(prev, 1, 1); // dirty
			}

			if(next!=INVALID_PAGE){
			     status = MINIBASE_BM->pinPage(next, (Page* &)nextPage, 1);
		   	     nextPage->setPrevPage(prev);
		 	     status = MINIBASE_BM->unpinPage(next, 1, 1); // dirty
			}
		}

		// delete leaf after re-link
		status = deleteLeaf->deleteRecord(rid);
	        if(status!=OK){
		   return DONE;
		}

		// delete the index if the pointed leaf is empty
		if(leafPage!=headerpage.rootPageID && deleteLeaf->empty()==true){
			BTIndexPage* deleteIndex;
			status = MINIBASE_BM->pinPage( indexPage, (Page* &)deleteIndex, 1 );
			RID curRidIndex;
printf("delete index page: %d\n",indexPage);
			status = deleteIndex->deleteKey(key, headerpage.keytype, curRidIndex);
			status = MINIBASE_BM->unpinPage(indexPage, 1, 1);  //  dirty
			
		}

		status = MINIBASE_BM->unpinPage(leafPage, 1, 1);

	}
	/**************** advanced delete with redistribution or merging ******************/
	else{
		Delete_helper(leafPage, *(Keytype*)key, rid);
	}

	return OK;
}

// for advanced delete only
// recursively call for merge
Status BTreeFile::Delete_helper(PageId currentDel, Keytype &key, const RID rid)
{
	Status status = OK;

	SortedPage* deleteTarget;
	status = MINIBASE_BM->pinPage( currentDel, (Page* &)deleteTarget, 1 );
	nodetype ndtype = (nodetype)deleteTarget->get_type();

	int recSize =1;
	KeyDataEntry recodOne;
	int entryLen;
	Datatype datatype;

	if(ndtype==INDEX){
		datatype.pageNo = currentDel;  // only estimate the size
		make_entry(&recodOne,headerpage.keytype,(void*)&key,ndtype, datatype, &entryLen); //only use to get the lenth
	}
	else{
		datatype.rid = rid;
		make_entry(&recodOne,headerpage.keytype,(void*)&key,ndtype, datatype, &entryLen); //only use to get the lenth
	}

	/****** check if we can directly delete, namely has at least d+1 entries before delete ******/
	int numRec = deleteTarget->numberOfRecords();
	int d = MINIBASE_PAGESIZE/entryLen;

	if(numRec>=d+1){  // if have enough records, do basic delete

		deleteTarget->deleteRecord(rid);
		return OK;
	}

	/***************** find sibling *************/
	PageId LeftSiblingPage = INVALID_PAGE;
	PageId RightSiblingPage = INVALID_PAGE;
	RID* ParentRID = NULL;
	RID* rightParent = NULL;
	BTIndexPage* parentNode =NULL;
	find_sibling(deleteTarget, key, LeftSiblingPage, RightSiblingPage, ParentRID, rightParent, parentNode);


	/***************** check which sibling can borrow, redistribute, return *************/
	// prority is rightsib, get the first record from right, does not need traverse all rightsib
	// do not need to change parent index

	if(RightSiblingPage!=INVALID_PAGE){
		SortedPage* rightsib;
		status = MINIBASE_BM->pinPage(RightSiblingPage, (Page* &)rightsib, 1);
		if(rightsib->numberOfRecords()>=d+1){
			deleteTarget->deleteRecord(rid); // delete before re-distribute
			status = rightDistibute(deleteTarget, rightsib, parentNode);
			status = MINIBASE_BM->unpinPage(currentDel, 1, 1);
			return status;
		}
		status = MINIBASE_BM->unpinPage(currentDel, 1, 1);
	}

	if(LeftSiblingPage!=INVALID_PAGE ){
		SortedPage* leftsib;
		status = MINIBASE_BM->pinPage(LeftSiblingPage, (Page* &)leftsib, 1);
		if(leftsib->numberOfRecords()>=d+1){
			deleteTarget->deleteRecord(rid);  // delete before re-distribute
			status = leftDistribute(deleteTarget, leftsib, parentNode);
			status = MINIBASE_BM->unpinPage(currentDel, 1, 1);
			return status;
		}
		status = MINIBASE_BM->unpinPage(currentDel, 1, 1);
	}


	/*****************  need merge, decide which sibling to merge *************/
	// left first, no specific reason
	if(LeftSiblingPage!=INVALID_PAGE){

		SortedPage* leftsib;
		status = MINIBASE_BM->pinPage(LeftSiblingPage, (Page* &)leftsib, 1);

		if(leftsib->numberOfRecords()<d+1){

			leftMerge(deleteTarget, leftsib, parentNode, ParentRID);
			status = MINIBASE_BM->unpinPage(LeftSiblingPage, 1, 1);
			status = MINIBASE_BM->unpinPage(currentDel, 1, 1);
			return status;
		}
		status = MINIBASE_BM->unpinPage(LeftSiblingPage, 1, 1);
	}

	if(RightSiblingPage!=INVALID_PAGE){

		SortedPage* rightsib;
		status = MINIBASE_BM->pinPage(RightSiblingPage, (Page* &)rightsib, 1);

		if(rightsib->numberOfRecords()<d+1){

			rightMerge(deleteTarget, rightsib, parentNode, ParentRID, rightParent);
			status = MINIBASE_BM->unpinPage(RightSiblingPage, 1, 1);
			status = MINIBASE_BM->unpinPage(currentDel, 1, 1);
			return status;
		}
		status = MINIBASE_BM->unpinPage(RightSiblingPage, 1, 1);
	}

	// can neither re-distribute nor merge, extreme case, basic delete
	deleteTarget->deleteRecord(rid);
	status = MINIBASE_BM->unpinPage(currentDel, 1, 1);  // unpin before the recursion

	return status;
}

// find the left and right sibling
bool BTreeFile::find_sibling(SortedPage* currIndex, Keytype &key, PageId &LeftSiblingPage, PageId &RightSiblingPage, RID* parentRID, RID*rightParent, BTIndexPage* parentNode)
{
	Status status = OK;

	PageId parent = INVALID_PAGE;
	Search_parent(currIndex->page_no(), headerpage.rootPageID, key, parent);

	if(parent == INVALID_PAGE){ // root does not has parent	
		LeftSiblingPage =currIndex->getNextPage();
		RightSiblingPage =currIndex->getPrevPage();
		return true;
	}

	status = MINIBASE_BM->pinPage( parent, (Page* &)parentNode, 1 );  // parent must be INDEX

	// initilize
	LeftSiblingPage = INVALID_PAGE;
	RightSiblingPage = INVALID_PAGE;
	//leftParent = NULL;
	//rightParent = NULL;

	// traverse records in this page
	RID currRID, prevRID;
	status = parentNode->firstRecord(currRID);               // read the first index RID
	printf("first record page %d, slot %d\n",currRID.pageNo,currRID.slotNo);
	if(status != OK){
		cout<<"failed first record"<<endl;
	}
	bool findflag =false;

	do{
		KeyDataEntry recPtr;
		int recLen;

		status = parentNode->getRecord(currRID,(char *)&recPtr,recLen);

		// read the key from entry
		Keytype targetkey;  // the key in the tree
		Datatype targetdata;
		get_key_data(&targetkey, &targetdata, (KeyDataEntry *)&recPtr, recLen, INDEX);

		if(findflag==false){

			if(targetdata.pageNo == currIndex->page_no()){  // hit the record point to current delete page
				*parentRID = currRID;
				findflag = true;
			}
			else{
				// update left PageId, move on
				LeftSiblingPage = targetdata.pageNo;
				//*leftParent = currRID;
			}
		}
		else{
			RightSiblingPage = targetdata.pageNo;
			*rightParent = currRID;
			return true;
		}

		prevRID = currRID;

	}while(parentNode->nextRecord(prevRID,currRID)==OK);

	return findflag;

}

// re-distribute from leftsib
// get the last record in leftsib, insert into current
Status BTreeFile::leftDistribute(SortedPage* current, SortedPage* left, BTIndexPage* parent)
{
	Status status = OK;

	// traverse to find the larest record
	RID currRID, prevRID;
	status = left->firstRecord(currRID);               // read the first index RID
	do{
		prevRID = currRID;
	}
	while(left->nextRecord(prevRID,currRID)==OK);

	char *recPtr;
	int recLen;
	status = left->getRecord(prevRID,recPtr,recLen);  // prevRID is the last record
	RID tempRid;
	status = current->insertRecord(headerpage.keytype, recPtr, recLen, tempRid);
	status = left->deleteRecord(tempRid);

	return status;
}

// re-distribute from rightsib
// get the first record in rightsib, insert into current
Status BTreeFile::rightDistibute(SortedPage* current, SortedPage* right, BTIndexPage* parent)
{
	Status status = OK;

	// traverse to find the larest record
	RID currRID;
	status = right->firstRecord(currRID);               // read the first index RID

	char *recPtr;
	int recLen;
	status = right->getRecord(currRID,recPtr,recLen);
	RID tempRid;
	status = current->insertRecord(headerpage.keytype, recPtr, recLen, tempRid);
	status = right->deleteRecord(tempRid);

	return status;
}

// merge with the leftsib
// get all records from current to left
Status BTreeFile::leftMerge(SortedPage* current, SortedPage* left, BTIndexPage* parent, RID* parentRID)
{
	Status status = OK;
	nodetype ndtype = (nodetype)current->get_type();

	// get all records in current
	RID currRID, prevRID;
	prevRID.pageNo = INVALID_PAGE;
	status = current->firstRecord(currRID);               // read the first index RID
	do{
		// delete record
		if(prevRID.pageNo != INVALID_PAGE){
			status = current->deleteRecord(prevRID);
		}

		char *recPtr;
		int recLen;
		status = current->getRecord(currRID,recPtr,recLen);

		// insert into left
		RID tempRid;
		left->insertRecord(headerpage.keytype, recPtr, recLen, tempRid);

		prevRID = currRID;
	}
	while(current->nextRecord(prevRID,currRID)==OK);
	// delete the last record
	if(prevRID.pageNo != INVALID_PAGE){
		status = current->deleteRecord(prevRID);
	}

	// if it is leaf, re-link and delete the page as well
	if(ndtype==LEAF){
		// re-connect leaf link
		PageId prev =current->getPrevPage();
		PageId next =current->getNextPage();

		SortedPage *prevPage, *nextPage;
		status = MINIBASE_BM->pinPage(prev, (Page* &)prevPage, 1);
		status = MINIBASE_BM->pinPage(next, (Page* &)nextPage, 1);
		prevPage->setNextPage(next);
		nextPage->setPrevPage(prev);

		status = MINIBASE_BM->unpinPage(prev, 1, 1); // dirty
		status = MINIBASE_BM->unpinPage(next, 1, 1); // dirty
	}
	status = MINIBASE_DB->deallocate_page(current->page_no());   // deallocate current page

	// if the parent is the root, do basic delete
	if(parent->page_no() == headerpage.rootPageID){
		parent->deleteRecord(*parentRID);
	}
	else{ // recursively call delete_helper the parent index to delete current
		// read the key from parent node
		KeyDataEntry ParrecPtr;
		int ParrecLen;
		status = parent->getRecord(*parentRID,(char*)&ParrecPtr,ParrecLen);
		Keytype parekey;
		Datatype paredata;
		get_key_data(&parekey, &paredata, &ParrecPtr, ParrecLen, INDEX);  // only used to get key
		Delete_helper(parent->page_no(), parekey, *parentRID);
	}

	return status;
}

// merge with the rightsib
// get all records from current to right
Status BTreeFile::rightMerge(SortedPage* current, SortedPage* right, BTIndexPage* parent, RID* parentRID, RID* rightParent)
{
	// get all records in rightsib  // this way do not need to update key in parent
	Status status = OK;
	nodetype ndtype = (nodetype)right->get_type();

	// get all records in current
	RID currRID, prevRID;
	prevRID.pageNo = INVALID_PAGE;
	status = right->firstRecord(currRID);               // read the first index RID
	do{
		// delete records in rightsib
		if(prevRID.pageNo != INVALID_PAGE){
			status = right->deleteRecord(prevRID);
		}

		KeyDataEntry recPtr;
		int recLen;
		status = right->getRecord(currRID,(char*)&recPtr,recLen);

		// insert into current
		RID tempRid;
		current->insertRecord(headerpage.keytype, (char*)&recPtr, recLen, tempRid);

		prevRID = currRID;
	}
	while(right->nextRecord(prevRID,currRID)==OK);
	// delete the last record
	if(prevRID.pageNo != INVALID_PAGE){
		status = right->deleteRecord(prevRID);
	}

	// if it is leaf, re-link and delete the page as well
	// if it is leaf, re-link and delete the page as well
	if(ndtype==LEAF){
		// re-connect leaf link
		PageId prev =right->getPrevPage();
		PageId next =right->getNextPage();

		SortedPage *prevPage, *nextPage;
		status = MINIBASE_BM->pinPage(prev, (Page* &)prevPage, 1);
		status = MINIBASE_BM->pinPage(next, (Page* &)nextPage, 1);
		prevPage->setNextPage(next);
		nextPage->setPrevPage(prev);

		status = MINIBASE_BM->unpinPage(prev, 1, 1); // dirty
		status = MINIBASE_BM->unpinPage(next, 1, 1); // dirty
	}
	status = MINIBASE_DB->deallocate_page(right->page_no());  // deallocate right page


	// if the parent is the root, do basic delete
	if(parent->page_no() == headerpage.rootPageID){
		parent->deleteRecord(*rightParent);
	}

	else{ // recursively call delete_helper the parent index to delete current
		// read the key from parent node
		char *ParrecPtr;
		int ParrecLen;
		status = parent->getRecord(*rightParent,ParrecPtr,ParrecLen);
		Keytype rigkey;
		Datatype rigdata;
		get_key_data(&rigkey, &rigdata, (KeyDataEntry *)ParrecPtr, ParrecLen, INDEX);  // only used to get key
		Delete_helper(parent->page_no(), rigkey, *rightParent);
	}

	return status;
}


IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
    
    Status status = OK;
    
    
    BTreeFileScan *scanner = new BTreeFileScan();
    
    scanner->Keysize = headerpage.keysize;
    
    // go most left LEAF
    PageId currPageId = headerpage.rootPageID;
    
    while(1){
        
        SortedPage* currIndex;
        status = MINIBASE_BM->pinPage( currPageId, (Page* &)currIndex, 1 );
        nodetype ndtype = (nodetype)currIndex->get_type();
        
        if(ndtype!=LEAF){  // found the left most
            if(currIndex->getPrevPage()!=INVALID_PAGE){ // go down with prevLink
                status = MINIBASE_BM->unpinPage(currPageId, 0, 1);
                currPageId = currIndex->getPrevPage();
                continue;
            }
            else{

		RID rid;
                currIndex->firstRecord(rid);  // read the first index record
   
 		KeyDataEntry recPtr;
		char* tmpPtr =(char*)&recPtr;
       		int recLen;
		PageId pageNo;
        	Keytype* cur_key = (Keytype*)&recPtr;
        	status = currIndex->getRecord(rid,(char*)&recPtr,recLen);   // read the next index record

        	pageNo = *(PageId*)&tmpPtr[recLen-sizeof(PageId)];
                
                currPageId = pageNo;
                status = MINIBASE_BM->unpinPage(currPageId, 0, 1);
            }
        }
        else{  // find the LEAF
            status = MINIBASE_BM->unpinPage(currPageId, 0, 1);
            break;
        }
    }
    
    if(lo_key==NULL){
        scanner->leftmostPage = currPageId;
        scanner->currentPage = currPageId;
        
        SortedPage* currIndex;
        status = MINIBASE_BM->pinPage( currPageId, (Page* &)currIndex, 1 );
        // read first record
      	 KeyDataEntry recPtr;
        int recLen;
        RID rid;
        currIndex->firstRecord(rid);
        status = currIndex->getRecord(rid,(char*)&recPtr,recLen);   // read the first index record
        status = MINIBASE_BM->unpinPage(currPageId, 0, 1);
        
        scanner->leftmostRID = rid;
        scanner->currRID = rid;
    }

    // search the lo_key and high key
    PageId traverse = currPageId;
    PageId lastPage = INVALID_PAGE;
    RID lastRID;
    while(traverse!=INVALID_PAGE){
        SortedPage* traversePage;
        status = MINIBASE_BM->pinPage( traverse, (Page* &)traversePage, 1 );
        
        // read the key inside index record one by one
        RID currRID, prevRID;
        status = traversePage->firstRecord(currRID);               // read the first index RID
        do{

            //status = traversePage->getRecord(currRID,(char*)&recPtr,recLen);   // read the first index record
            
		KeyDataEntry recPtr;
		char* tmpPtr =(char*)&recPtr;
       		int recLen;
		PageId pageNo;
        	Keytype* cur_key = (Keytype*)&recPtr;
        	status = traversePage->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record


            // compare the key
            int compareResult_l = keyCompare((void*)lo_key,(void*)cur_key,headerpage.keytype);
            if(compareResult_l==0){  // the low key found
                scanner->leftmostPage = traverse;
                scanner->leftmostRID = currRID;
                scanner->currentPage = traverse;
                scanner->currRID = currRID;
            }
            int compareResult_r = keyCompare((void*)lo_key,(void*)cur_key,headerpage.keytype);
            if(compareResult_r==0) {  // the high key found
                scanner->rightmostPage = traverse;
                scanner->rightmostRID = currRID;
                status = MINIBASE_BM->unpinPage(traverse, 0, 1);
                return scanner;
            }
            
            lastRID = currRID;  // record prevous RID for remember last page's last RID
            prevRID = currRID;
            
        }
        while(traversePage->nextRecord(prevRID,currRID)==OK);
        
        status = MINIBASE_BM->unpinPage(traverse, 0, 1);
        lastPage = traverse;  // record prevous PageId for remember last page
        
        traverse = traversePage->getNextPage();
    }
    
    
    // does not find
    if(lo_key!=NULL && traverse!=INVALID_PAGE){
        cout<<"cannot find the left key!"<<endl;
        return NULL;
    }
    
    if(hi_key!=NULL && traverse!=INVALID_PAGE){
        cout<<"cannot find the right key!"<<endl;
        return NULL;
    }
    
    if(hi_key==NULL){ // find the last record in last page
        scanner->rightmostPage = lastPage;
        scanner->rightmostRID = lastRID;
    }
    
    return scanner;
}

int BTreeFile::keysize()
{
	return headerpage.keysize;
}

// recursively find the exact key Page and record page
// called in delete
Status BTreeFile::Search_index(PageId& indexPage, PageId& leafPage, PageId currPage, Keytype key )
{
	Status status = OK;
	if(currPage==INVALID_PAGE){
		return DONE;
	}
	if(indexPage!=INVALID_PAGE){  // find them already, quick return
		return OK;
	}

	// pin the page out
	SortedPage* currIndex;
	status = MINIBASE_BM->pinPage( currPage, (Page* &)currIndex, 1 );

	PageId recordpageId =INVALID_PAGE;
	PageId child =INVALID_PAGE;
	if (get_matchedkey_page(currIndex, key, recordpageId, child)==true) {  
		
printf("recordpageId: %d, child: %d\n",recordpageId, child);


		if(recordpageId!=INVALID_PAGE){  // found the index with matched key
			indexPage = currPage; // find index
			leafPage = recordpageId;  // find leaf
			status = MINIBASE_BM->unpinPage(currPage, 0, 1);
			return OK;
		}
		else{
			Search_index(indexPage, leafPage, child, key);  // keep searching
		}
	}
	else{
		status = MINIBASE_BM->unpinPage(currPage, 0, 1);
		return DONE;
	}

	return OK;
}

// find the PageId with a specific key in a Page
// return the recordPageNo, similar to BTIndexPage::get_page_no, but not create new page for prevLink
// used for delete
bool BTreeFile::get_matchedkey_page(SortedPage* currIndex, Keytype key, PageId & recordpageId, PageId& child)
{
	Status status = OK;

	// if this page is deleted
	if(currIndex->empty()==true){
		return OK;
	}
	if(recordpageId!=INVALID_PAGE){
		return OK;
	}


	// read the key inside index record one by one
	PageId prev_pointTo = INVALID_PAGE;
	RID currRID, prevRID;
	status = currIndex->firstRecord(currRID);               // read the first index RID
	do{
//printf("currRID.slotNo: %d\n", currRID.slotNo);
		KeyDataEntry recPtr;
		char* tmpPtr =(char*)&recPtr;
        	int recLen;
        	Keytype* cur_key = (Keytype*)&recPtr;
        	status = currIndex->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record
		nodetype ndtype = (nodetype)currIndex->get_type(); 
//cout<<"node type-"<<ndtype<<endl;
		if(ndtype==LEAF){  // it only possible when root is the LEAF, namely btree only has one level
printf("reach the leaf!\n");
		    recordpageId = headerpage.rootPageID;
		    return true;
		}

        	PageId pageNo = *(PageId*)&tmpPtr[recLen-sizeof(PageId)];  //  must be index


		// compare the key
		int compareResult = keyCompare((void*)&key,(void*)&cur_key,headerpage.keytype);
printf("delkey: %d, cur_key: %d\n", (int)key.intkey, (int)cur_key->intkey);
		if(compareResult==0){  // the key found
			recordpageId = pageNo;
			return true;
		}
		else if(compareResult<0){
//printf("reach here 2 !\n");
			// this record point to the page is the next level search page
            		if( prev_pointTo!=INVALID_PAGE ){
                		child = prev_pointTo;
				return true;
           		 }
			else{   // samller than all record
				PageId prevlink = ((BTIndexPage*)currIndex)->getLeftLink();
                		if(prevlink != INVALID_PAGE){
                   			 child = prevlink;
               			 }
				else{
					cout<<"in delete: cannot find the key!"<<endl;
					return false;
				}
			}
		}
		
		// else the insert key is larger, move on to next key record
		prev_pointTo = pageNo;
		prevRID = currRID;

	}
	while(currIndex->nextRecord(prevRID,currRID)==OK);
	child = prev_pointTo;  // key larger than all index keys, choose the last one

	return true;
}

// recursive search, return key location, index is used for first split
// called in insert()
// return LEAF
// Status BTreeFile::Search_index(pair<PageId&, PageId&> result, PageId parentPage, PageId currPage, const void *key, bool &reduflag)
Status BTreeFile::Search_record( PageId& result, PageId currPage, Keytype &key )
{
	Status status = OK;

	// base case, reach deeper than the leaf, cannot find
	if(currPage==INVALID_PAGE){
		return DONE;
	}
	//    if(result!=INVALID_PAGE){  // found the location already
	//        return OK;
	//    }

	// pin the page out
	SortedPage* currIndex;  // last leaf do not need to go deeper, so here only index page
	status = MINIBASE_BM->pinPage( currPage, (Page* &)currIndex, 0 );  // should emptyPage == 1?
	nodetype ndtype = (nodetype)currIndex->get_type();  // be careful to match

	if(ndtype==INDEX){  // search this page, find the next child to search
		PageId child = INVALID_PAGE;
		status = ((BTIndexPage*)currIndex)->get_page_no((void*)&key,headerpage.keytype,child);
		status = MINIBASE_BM->unpinPage(currPage, 0, 1);
		printf("index child %d\n",child);


		if(child!=INVALID_PAGE || status == OK){ // find the child
			Search_record(result, child, key );
		}
		else{   // all keys in this page smaller than the target key, call search again
			cout<<BtreeErrorMsgs[17]<<endl;
			return DONE;
		}
	}
	else if(ndtype==LEAF){
		result = currPage;
		status = MINIBASE_BM->unpinPage(currPage, 0, 1);
	}

	return status;
}

// recursively search the parent of the split target, child could be index/leaf
// called in Insert_helper()
Status BTreeFile::Search_parent(const PageId targetchild, PageId currPage, Keytype &childkey, PageId& parent)
{
	Status status = OK;

	// base case, reach deeper than the leaf, cannot find
	if(currPage==INVALID_PAGE){
		return DONE;
	}
	//    if(parent!=INVALID_PAGE){  // found the parent
	//        return OK;
	//   }


	// pin the page out
	SortedPage* currIndex;  // last leaf do not need to go deeper, so here only index page
	status = MINIBASE_BM->pinPage( currPage, (Page* &)currIndex, 1 );  // should emptyPage == 1?
	nodetype ndtype = (nodetype)currIndex->get_type();  // be careful to match

	if(ndtype==INDEX){  // search this page, find the next child to search
		PageId child = INVALID_PAGE;
		RID pRID;
		status = ((BTIndexPage*)currIndex)->get_page_no((void*)&childkey, headerpage.keytype, child);
		status = MINIBASE_BM->unpinPage(currPage, 0, 1);

		printf("parent childkey %d child %d\n",childkey,child);

		if(child!=INVALID_PAGE || status == OK){ // find the child
			if(child == targetchild){
				parent = currPage;
				return OK;
			}
			else{
				Search_parent(targetchild, child, childkey, parent);
			}
		}
		else{   // all keys in this page smaller than the target key, call search again
			cout<<BtreeErrorMsgs[17]<<endl;
			return DONE;
		}
	}
	else if(ndtype==LEAF){  // parent can not be leaf
		status = MINIBASE_BM->unpinPage(currPage, 0, 1);
		return DONE;
	}


	return OK;
}


// may recursively insert, split, search_parent
//Status BTreeFile::Insert_helper(PageId insertLoc, Keytype &key, Datatype datatype, nodetype createdtype)
//{
//	Status status = OK;
//
//	// base cases
//	if(insertLoc==INVALID_PAGE){
//		return DONE;
//	}
//
//	// make the entry intended to insert
//	KeyDataEntry insertOne;
//	int entryLen;
//	make_entry(&insertOne,headerpage.keytype,(void*)&key,createdtype, datatype, &entryLen); //only use to get the lenth
//
//	// see if it need split
//	SortedPage* targetPage;
//	status = MINIBASE_BM->pinPage(insertLoc, (Page *&)targetPage, 1);
//	status = MINIBASE_BM->unpinPage(insertLoc, 0, 1);  // because I may pin it again in split function
//
//	if(targetPage->available_space()<entryLen){  // no enough space, need split
//		//printf("entrylen %d available_space()%d \n",entryLen,targetPage->available_space()); 
//		// split into two page, <upkey, leftchild> will be copy or push up
//		PageId leftchild;
//		Keytype upkey;
//        targetPage->slotPrint();
//		Split(insertLoc, leftchild, upkey);
//		printf("end split\n");
//
//		// after split, decide where the new entry should go, new key = upkey
//		// if less than left min, go left child and update parentkey
//		// if less than right min, go left child
//		// if larger than right min, go right
//		SortedPage* leftPage;
//		status = MINIBASE_BM->pinPage(leftchild, (Page *&)leftPage, 1);
//		
//		RID leftMin;
//		leftPage->firstRecord(leftMin);
//		KeyDataEntry recPtr;
//		int recLen;
//		status = leftPage->getRecord(leftMin,(char*)&recPtr,recLen);   // read the next index record
//		nodetype ndtype = (nodetype)leftPage->get_type();
//		// read the key from entry
//		KeyDataEntry leftkey;
//		Datatype targetdata;
//		get_key_data(&leftkey, &targetdata, &recPtr, recLen, ndtype); 
//		//left min key read
//
//		//KeyDataEntry recPtr;
//		//int recLen;
//		SortedPage* rightPage;
//		RID rightMin;
//		//Datatype targetdata;
//		status = MINIBASE_BM->pinPage(insertLoc, (Page *&)rightPage, 1);
//		//nodetype ndtype = (nodetype)rightPage->get_type();
//		rightPage->firstRecord(rightMin);
//		status = rightPage->getRecord(rightMin,(char*)&recPtr,recLen);
//		KeyDataEntry rightkey;
//		get_key_data(&rightkey, &targetdata, &recPtr, recLen, ndtype); 
//
//		//right min key read
//		int compareResLeft = 0;
//		int compareResRight = 0;
//		compareResLeft = keyCompare((void *)&key, ( void *)&leftkey.key, headerpage.keytype);
//		compareResRight = keyCompare((void *)&key, ( void *)&rightkey.key, headerpage.keytype);
//        
//        
//        
//		// search parent
//		PageId parent = INVALID_PAGE;
//		status = Search_parent(insertLoc, headerpage.rootPageID, key, parent); // search start from root
//		RID rid;
//        if(compareResLeft<0){ // inserting key is smallest in the left (new) child page
//            upkey = leftkey.key;
//        }
// 		if( compareResRight < 0){
//			//second case, go left child
//			if(ndtype == LEAF){
//				((BTLeafPage*)leftPage)->insertRec((void*)&key, headerpage.keytype, datatype.rid, rid);
//			}
//			else if (ndtype == INDEX){
//				((BTIndexPage*)leftPage)->insertKey((void*)&key, headerpage.keytype, datatype.pageNo, rid);
//			}
//		}
//		else if (compareResRight > 0){
//			//3rd case, go right child
//			if(ndtype == LEAF){
//				((BTLeafPage*)rightPage)->insertRec((void*)&key, headerpage.keytype, datatype.rid, rid);
//			}
//			else if (ndtype == INDEX){
//				((BTIndexPage*)rightPage)->insertKey((void*)&key, headerpage.keytype, datatype.pageNo, rid);
//			}
//		}
//        status = MINIBASE_BM->unpinPage(insertLoc, 1, 1); // dirty
//        status = MINIBASE_BM->unpinPage(leftchild, 1, 1); // dirty
//
//
//		if(parent == INVALID_PAGE){
//			PageId rootNewPid;
//			status = MINIBASE_DB->allocate_page(rootNewPid);
//			BTIndexPage *rootNewIndexPage;
//			status = MINIBASE_BM->newPage(rootNewPid, (Page* &)rootNewIndexPage);
//			rootNewIndexPage->init(rootNewPid);
//			PageId oldRootPid = headerpage.rootPageID;
//			headerpage.rootPageID = rootNewPid;
//			headerpage.PageType = INDEX;
//			//reading min key from old root
//            KeyDataEntry recPtr2;
//            int recLen2;
//            SortedPage* oldPage;
//            RID oldMin;
//            Datatype olddata;
//            status = MINIBASE_BM->pinPage(oldRootPid, (Page *&)oldPage, 1);
//            nodetype ndtype = (nodetype)oldPage->get_type();
//            oldPage->firstRecord(oldMin);
//            status = oldPage->getRecord(oldMin,(char*)&recPtr2,recLen2);
//            KeyDataEntry oldkey;
//            get_key_data(&oldkey, &olddata, &recPtr2, recLen2, ndtype); 
//			rootNewIndexPage->insertKey((void*)&oldkey, headerpage.keytype, oldRootPid, rid);
//			rootNewIndexPage->insertKey((void*)&upkey,headerpage.keytype, leftchild, rid);
//            status = MINIBASE_BM->unpinPage(oldRootPid, 0, 1);
//            status = MINIBASE_BM->unpinPage(rootNewPid, 1, 1);
//			return status;
//
//		}
//        
//        // if the right child is come from parent's prevLink. need insert the first key into parent and clear the prevLink
//        BTIndexPage* parentPage;
//        status = MINIBASE_BM->pinPage(parent,(Page *&)parentPage, 1);
//        if(parentPage->getLeftLink()==insertLoc){
//            parentPage->setLeftLink(INVALID_PAGE);
//            status = MINIBASE_BM->unpinPage(parent, 1, 1);  // clear prevLink
//            status = Insert_helper(parent, rightkey.key, rightkey.data,INDEX);
//        }
//        else{
//            status = MINIBASE_BM->unpinPage(parent, 0, 1);
//        }
//        
//		// call Insert_helper to insert the overflowed key upper, second split cannot be type leaf, so dataRid is useless
//		Datatype newdatatype;
//		newdatatype.pageNo = leftchild;
//		status = Insert_helper(parent, upkey, newdatatype,INDEX);
//
//
//	}
//	else{ // enough space, directly insert into leaf, base case as well
//
//		// insert the insertOne entry
//		if(createdtype==INDEX){
//			RID rid;
//			((BTIndexPage*)targetPage)->insertKey((void*)&key, headerpage.keytype, datatype.pageNo, rid);
//		}
//		else if(createdtype==LEAF){
//			RID rid;
//			((BTLeafPage*)targetPage)->insertRec((void*)&key, headerpage.keytype, datatype.rid, rid);
//		}
//	}
//
//	return OK;
//}

Status BTreeFile::Search_Insert_Helper(PageId currPage, Keytype insertkey, Datatype insertData, PageId &rightchild, Keytype &upkey)
{
    Status status = OK;
    //printf("helper called at pageno %d\n",currPage);
    // base case, reach deeper than the leaf, cannot find
    if(currPage==INVALID_PAGE){
        return OK;
    }

    // pin the page out
    SortedPage* currIndex;  // last leaf do not need to go deeper, so here only index page
    status = MINIBASE_BM->pinPage( currPage, (Page* &)currIndex, 0 );  // should emptyPage == 1?
    nodetype ndtype = (nodetype)currIndex->get_type();  // be careful to match
    
    // make the entry intended to insert
    KeyDataEntry insertOne;
    int entryLen;
    make_entry(&insertOne,headerpage.keytype,(void*)&insertkey,ndtype, insertData, &entryLen); //only use to get the lenth
    
    if(ndtype==INDEX){  // search this page, find the next child to search
        PageId child = INVALID_PAGE;
        status = ((BTIndexPage*)currIndex)->get_page_no((void*)&insertkey,headerpage.keytype,child);
        
        if(child!=INVALID_PAGE || status == OK){ // find the child
             Search_Insert_Helper(child, insertkey, insertData, rightchild, upkey);  // recursion
            
            if(rightchild!=INVALID_PAGE){  // has split
                if(currIndex->available_space()>entryLen){  // have enough space, directly insert
                    RID rid;
                    ((BTIndexPage*)currIndex)->insertKey((void*)&upkey, headerpage.keytype, rightchild, rid);
                    rightchild = INVALID_PAGE;  // clear
                }
                else{  
// split
PageId newRightChild;
Keytype newUpkey;
Datatype new_insertdata;
new_insertdata.pageNo = rightchild;
                    Split(currPage, upkey, new_insertdata, newRightChild, newUpkey);
rightchild = newRightChild;
upkey = newUpkey;
//Split(PageId splitTarget, Keytype &insertkey, Datatype insertData, PageId &rightchild, Keytype &upkey);
                    if(currPage == headerpage.rootPageID){
                        PageId rootNewPid;
                        status = MINIBASE_DB->allocate_page(rootNewPid);
                        BTIndexPage *rootNewIndexPage;
                        status = MINIBASE_BM->newPage(rootNewPid, (Page* &)rootNewIndexPage);
                        rootNewIndexPage->init(rootNewPid);
                        PageId oldRootPid = headerpage.rootPageID;
                        headerpage.rootPageID = rootNewPid;
                        headerpage.PageType = INDEX;
                        
                        
                        RID rid;
                        //rootNewIndexPage->insertKey((void*)&oldkey, headerpage.keytype, currPage, rid);
                        rootNewIndexPage->insertKey((void*)&newUpkey,headerpage.keytype, newRightChild, rid);
rootNewIndexPage->setLeftLink(oldRootPid);
                        status = MINIBASE_BM->unpinPage(rootNewPid, 1, 1);
                        
                        rightchild = INVALID_PAGE;  // clear
                    }
                }
            }
            else{  // no split
                rightchild = INVALID_PAGE;  // clear, will return soon after unpin
            }
            
        }
        else{
            cout<<BtreeErrorMsgs[17]<<endl;
            return DONE;
        }
    }
    else if(ndtype==LEAF){   // base case
        if(currIndex->available_space()>entryLen){  // have enough space, directly insert
            // insert the insertOne entry
            RID rid;
            ((BTLeafPage*)currIndex)->insertRec((void*)&insertkey, headerpage.keytype, insertData.rid, rid);
        }
        else{  // split
            Split(currPage, insertkey, insertData, rightchild, upkey);
            //currIndex->slotPrint(INDEX);
            if(currPage == headerpage.rootPageID){   // because at first the root is leaf
                        PageId rootNewPid;
                        status = MINIBASE_DB->allocate_page(rootNewPid);
                        BTIndexPage *rootNewIndexPage;
                        status = MINIBASE_BM->newPage(rootNewPid, (Page* &)rootNewIndexPage);
                        rootNewIndexPage->init(rootNewPid);
                        PageId oldRootPid = headerpage.rootPageID;
                        headerpage.rootPageID = rootNewPid;
                        headerpage.PageType = INDEX;
                        
                        
                        RID rid;
                        //rootNewIndexPage->insertKey((void*)&oldkey, headerpage.keytype, currPage, rid);
                        rootNewIndexPage->insertKey((void*)&upkey,headerpage.keytype, rightchild, rid);
rootNewIndexPage->setLeftLink(oldRootPid);
                        status = MINIBASE_BM->unpinPage(rootNewPid, 1, 1);
                rootNewIndexPage->slotPrint(INDEX);
                rightchild = INVALID_PAGE;  // clear
            }
        }
    }
    
    status = MINIBASE_BM->unpinPage(currPage, 1, 1);
    return status;


}

// split the page into two page, copyup (leaf), pushup(index) key
// move former half to the new node, then we need not update parent's pointer
Status BTreeFile::Split(PageId splitTarget, Keytype &insertkey, Datatype insertData, PageId &rightchild, Keytype &upkey)
{
	Status status = OK;

	// split the page by half size (not half number of entry)
	//int halfsize = MINIBASE_PAGESIZE/2;

	// pin the split target page
	SortedPage *leftchild;
	status = MINIBASE_BM->pinPage(splitTarget, (Page *&)leftchild, 1);
	nodetype ndtype = (nodetype)leftchild->get_type();  // be careful to match

	// get half number of entries
	int halfnum = leftchild->numberOfRecords()/2;

	// create the leftchild page
	SortedPage *newpage;
	status = MINIBASE_BM->newPage(rightchild, (Page* &)newpage);
	newpage->init(rightchild);
	// connect them with each other (if it is leaf)
	if(ndtype==LEAF){
		((BTLeafPage*)newpage)->set_type(LEAF);

		PageId next =leftchild->getNextPage();
		newpage->setNextPage(next);

		SortedPage *nextPage;
		status = MINIBASE_BM->pinPage(next, (Page* &)nextPage, 1);
		nextPage->setPrevPage(rightchild);
		status = MINIBASE_BM->unpinPage(next, 0, 1);

		newpage->setPrevPage(splitTarget);
		leftchild->setPrevPage(rightchild);

	}
	else if(ndtype==INDEX){  //  do not need connection
		newpage->set_type(INDEX);
	}

    //leftchild->slotPrint(INDEX);
	// traverse the target page, move later half to the rightchild
	int recordCnt = 0;
	RID currRID, prevRID;
    	Status stopsign;
	Keytype targetkeyin;
	Datatype targetdatain;
	status = leftchild->firstRecord(currRID);               // read the first index RID
	do{
		KeyDataEntry recPtr;
		int recLen;

		status = leftchild->getRecord(currRID,(char*)&recPtr,recLen);   // get record
		recordCnt ++;
		
		// unpack the entry
		
		//get_key_data(&targetkeyin, &targetdatain, &recPtr,recLen, ndtype);	

		// if it is the former half
		if (recordCnt > halfnum){
//printf("recCnt: %d\n", recordCnt);
			// put this record into leftchild, cannot call sortpage insert directly, we need sorted insert

//printf("insert %d\n",prevRID.slotNo);
//printf("delete %d\n",prevRID.slotNo);

				// insert
				RID newRecord;
				/*if(ndtype==LEAF){
					status = ((BTLeafPage*)newpage)->insertRec(&targetkeyin, headerpage.keytype, targetdatain.rid, newRecord);
				}
				else if(ndtype==INDEX){
					status = ((BTIndexPage*)newpage)->insertKey(&targetkeyin, headerpage.keytype, targetdatain.pageNo, newRecord);
				}*/
status = newpage->insertRecord (headerpage.keytype,(char*)&recPtr,recLen, newRecord);

				// move on before delete
				prevRID = currRID;
				stopsign = leftchild->nextRecord(prevRID,currRID);

				// delete this record from rightchild directly, do not need sorts
				status = leftchild->deleteRecord(prevRID);
//printf("stopsign %d\n",stopsign);
				if(stopsign!=OK){
					break;
				}

		}
		else{
			prevRID = currRID;
			stopsign = leftchild->nextRecord(prevRID,currRID);
		}


	}while(1);

    /////// insert target record into the splited page  //////
    
    // read the first key from leftchild
    RID leftMin;
    leftchild->firstRecord(leftMin);
    KeyDataEntry recPtr;
    int recLen;
    status = leftchild->getRecord(leftMin,(char*)&recPtr,recLen);   // read the next index record
    //nodetype ndtype = (nodetype)newpage->get_type();
    KeyDataEntry leftkey;
    Datatype targetdata;
    get_key_data(&leftkey, &targetdata, &recPtr, recLen, ndtype);
    
    // read the first key from rightchild
    RID rightMin;
    newpage->firstRecord(rightMin);
    status = newpage->getRecord(rightMin,(char*)&recPtr,recLen);
    KeyDataEntry rightkey;
    get_key_data(&rightkey, &targetdata, &recPtr, recLen, ndtype);
    
    //right min key read
    int compareResLeft = 0;
    int compareResRight = 0;
    compareResLeft = keyCompare((void *)&insertkey, ( void *)&leftkey.key, headerpage.keytype);
    compareResRight = keyCompare((void *)&insertkey, ( void *)&rightkey.key, headerpage.keytype);
    
    // insert target record
    RID rid;
    if( compareResRight < 0){
        //less than right child, go left child
        if(ndtype == LEAF){
            ((BTLeafPage*)leftchild)->insertRec((void*)&insertkey, headerpage.keytype, insertData.rid, rid);
        }
        else if (ndtype == INDEX){
            ((BTIndexPage*)leftchild)->insertKey((void*)&insertkey, headerpage.keytype, insertData.pageNo, rid);
        }
        
    }
    else{  // compareResRight >= 0
        //larger than left child go right child
        if(ndtype == LEAF){
            ((BTLeafPage*)newpage)->insertRec((void*)&insertkey, headerpage.keytype, insertData.rid, rid);
        }
        else if (ndtype == INDEX){
            ((BTIndexPage*)newpage)->insertKey((void*)&insertkey, headerpage.keytype, insertData.pageNo, rid);
        }
    }
    
    // decide who is the upkey
    upkey = rightkey.key;
    leftchild->slotPrint(ndtype);
    newpage->slotPrint(ndtype);
	// upin the split page
	status = MINIBASE_BM->unpinPage(splitTarget, 1, 1); // dirty
	status = MINIBASE_BM->unpinPage(rightchild, 1, 1);  // dirty

	return OK;
}


void BTreeFile::treeDump(PageId currPage){
	Status status = OK;
static int treeLevel = 0;
treeLevel++;
printf("now at tree level %d\n",treeLevel);
	// base case, reach deeper than the leaf, cannot find
	if(currPage==INVALID_PAGE){
		return ;
	}
	//    if(result!=INVALID_PAGE){  // found the location already
	//        return OK;
	//    }

	// pin the page out
	SortedPage* currIndex;  // last leaf do not need to go deeper, so here only index page
	status = MINIBASE_BM->pinPage( currPage, (Page* &)currIndex, 0 );  // should emptyPage == 1?
	nodetype ndtype = (nodetype)currIndex->get_type();  // be careful to match

	if(ndtype==INDEX){  // search this page, find the next child to search
		currIndex->slotPrint(INDEX);
		RID currRID, prevRID;
		PageId prev_pointTo = INVALID_PAGE;
		status = currIndex->firstRecord(currRID);               // read the first index RID
if(currIndex->getPrevPage() != INVALID_PAGE){
treeDump(currIndex->getPrevPage());
}
		do{       
 KeyDataEntry recPtr;
	char* tmpPtr =(char*)&recPtr;
        int recLen;
	PageId pageNo;
        Keytype* cur_key = (Keytype*)&recPtr;
        status = currIndex->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record

        pageNo = *(PageId*)&tmpPtr[recLen-sizeof(PageId)];

			status = currIndex->getRecord(currRID,(char*)&recPtr,recLen);   // read the next index record

			treeDump(pageNo);
			// compare the key


			prevRID = currRID;

		}
		while((currIndex->nextRecord(prevRID,currRID))==OK);
	}
	else{
		currIndex->slotPrint(LEAF);
	}
treeLevel--;
	status = MINIBASE_BM->unpinPage( currPage, 0, 0 );
}

//Status BTreeFile::Delete_search_helper (PageId parentpage, PageId currPage, Keytype deletekey, Datatype deleteData, RID& oldchiIdentry)
//{
//    Status status;
//
//    // base case, reach deeper than the leaf, cannot find
//    if(currPage==INVALID_PAGE){
//        return OK;
//    }
//
//    // pin the page out
//    SortedPage* currIndex;  // last leaf do not need to go deeper, so here only index page
//    status = MINIBASE_BM->pinPage( currPage, (Page* &)currIndex, 0 );  // should emptyPage == 1?
//    nodetype ndtype = (nodetype)currIndex->get_type();  // be careful to match
//
//    if(ndtype==LEAF){
//        PageId child = INVALID_PAGE;
//        status = ((BTIndexPage*)currIndex)->get_page_no((void*)&deletekey,headerpage.keytype,child);
//        if(child!=INVALID_PAGE){
//            Delete_search_helper (currPage, child, deletekey, deleteData, oldchiIdentry);
//            if(oldchiIdentry.pageNo == INVALID_PAGE){
//                status = MINIBASE_BM->unpinPage(currPage, 0, 1);
//                return OK;
//            }
//        }
//
//    }
//    else{  // INDEX
//
//    }
//
//
//}



