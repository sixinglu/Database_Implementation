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
    if(MINIBASE_DB->get_file_entry(filename,headerpage.rootPageID)!=OK){
        cout<<BtreeErrorMsgs[22]<<endl;
        returnStatus =  DONE;
    }
    else{  // if the file does exist
        returnStatus =  OK;
    }

}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
    // if the file exist
    if(MINIBASE_DB->get_file_entry(filename,headerpage.rootPageID)==OK){
        returnStatus = OK;
    }
    else{  // create a new file
        // allocate space
        SortedPage* newrootPage;  // = new HFPage();
        returnStatus = MINIBASE_BM->newPage(headerpage.rootPageID, (Page* &)newrootPage);
        newrootPage->init(headerpage.rootPageID);
	newrootPage->set_type(LEAF);
        // add file entry
        returnStatus = MINIBASE_DB->add_file_entry(filename,headerpage.rootPageID);
        
        // set the root page parameters
        headerpage.keytype = keytype;
        headerpage.keysize = keysize;
        headerpage.PageType = LEAF;   // set root as LEAF, for search algorithm
headerpage.filename = new char[20];
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
   // printf("insert %d times\n",insert_count);
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
//printf("insertRec zero succeeded\n");
        return status;
    }
    status = MINIBASE_BM->unpinPage(headerpage.rootPageID, 1, 1);
    
    // search the insert location
    PageId result= INVALID_PAGE;
printf("headerpage.rootPageID %d\n",headerpage.rootPageID);
    status = Search_record(result, headerpage.rootPageID, key); // from root always

    // call helper function
    Datatype datatype;
    datatype.rid = rid;
printf("invalid page %d, result %d\n",INVALID_PAGE,result);
    Insert_helper(result, key, datatype, LEAF);
//printf("insert %d times\n",insert_count);    
    return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid)
{
    Status status = OK;
    PageId indexPage=INVALID_PAGE;
    PageId leafPage=INVALID_PAGE;
    
    Search_index(indexPage, leafPage, headerpage.rootPageID, key );  // find the delete target
    
    if(ADVANCED_DELTE==0 || indexPage==headerpage.rootPageID || leafPage==headerpage.rootPageID){  // if it is the root, just simply delete, not pull down
    
        /**************** basic delete without redistribution or merging ******************/

        BTLeafPage* deleteLeaf;
        status = MINIBASE_BM->pinPage( leafPage, (Page* &)deleteLeaf, 1 );
        
        // re-connect leaf link
        PageId prev =deleteLeaf->getPrevPage();
        PageId next =deleteLeaf->getNextPage();
        
        SortedPage *prevPage, *nextPage;
        status = MINIBASE_BM->pinPage(prev, (Page* &)prevPage, 1);
        status = MINIBASE_BM->pinPage(next, (Page* &)nextPage, 1);
        prevPage->setNextPage(next);
        nextPage->setPrevPage(prev);
        
        status = MINIBASE_BM->unpinPage(prev, 1, 1); // dirty
        status = MINIBASE_BM->unpinPage(next, 1, 1); // dirty
        
        // delete leaf after re-link
        deleteLeaf->deleteRecord(rid);
        
        // delete the index if the pointed leaf is empty
        if(deleteLeaf->empty()==true){
            BTIndexPage* deleteIndex;
            status = MINIBASE_BM->pinPage( indexPage, (Page* &)deleteIndex, 1 );
            RID curRidIndex;
            deleteIndex->deleteKey(key, headerpage.keytype, curRidIndex);
            status = MINIBASE_BM->unpinPage(indexPage, 1, 1);  //  dirty
        }
        
        status = MINIBASE_BM->unpinPage(leafPage, 1, 1);
        
    }
    /**************** advanced delete with redistribution or merging ******************/
    else{
        Delete_helper(leafPage, key, rid);
    }
    
    return OK;
}

// for advanced delete only
// recursively call for merge
 Status BTreeFile::Delete_helper(PageId currentDel, const void *key, const RID rid)
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
        make_entry(&recodOne,headerpage.keytype,key,ndtype, datatype, &entryLen); //only use to get the lenth
    }
    else{
        datatype.rid = rid;
        make_entry(&recodOne,headerpage.keytype,key,ndtype, datatype, &entryLen); //only use to get the lenth
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
bool BTreeFile::find_sibling(SortedPage* currIndex, const void *key, PageId &LeftSiblingPage, PageId &RightSiblingPage, RID* parentRID, RID*rightParent, BTIndexPage* parentNode)
{
    Status status = OK;
    
    PageId parent;
    Search_parent(currIndex->page_no(), headerpage.rootPageID, key, parent);
    
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
        char *ParrecPtr;
        int ParrecLen;
        status = parent->getRecord(*parentRID,ParrecPtr,ParrecLen);
        void *parekey;
        Datatype *paredata;
        get_key_data(parekey, paredata, (KeyDataEntry *)ParrecPtr, ParrecLen, INDEX);  // only used to get key
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
        
        char *recPtr;
        int recLen;
        status = right->getRecord(currRID,recPtr,recLen);
        
        // insert into current
        RID tempRid;
        current->insertRecord(headerpage.keytype, recPtr, recLen, tempRid);
        
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
        void *rigkey;
        Datatype *rigdata;
        get_key_data(rigkey, rigdata, (KeyDataEntry *)ParrecPtr, ParrecLen, INDEX);  // only used to get key
        Delete_helper(parent->page_no(), rigkey, *rightParent);
    }
    
    return status;
}


IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
  return NULL;
}

int BTreeFile::keysize()
{
    return headerpage.keysize;
}

// recursively find the exact key Page and record page
// called in delete
Status BTreeFile::Search_index(PageId& indexPage, PageId& leafPage, PageId currPage, const void *key )
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
   // nodetype ndtype = (nodetype)currIndex->get_type();  // be careful to match
    PageId recordpageId =INVALID_PAGE;
    PageId child =INVALID_PAGE;
    if (get_matchedkey_page(currIndex, key, recordpageId, child)==true) {  // found the key
        
        // find index
        indexPage = currPage;
        
        // find leaf
        leafPage = recordpageId;
        
        status = MINIBASE_BM->unpinPage(currPage, 0, 1);
        return OK;
    }
    else{
        status = MINIBASE_BM->unpinPage(currPage, 0, 1);
        Search_index(indexPage, leafPage, child, key );
    }
    
    
    
    return OK;
}

// find the PageId with a specific key in a Page
// return the recordPageNo
bool BTreeFile::get_matchedkey_page(SortedPage* currIndex, const void *key, PageId & recordpageId, PageId& child)
{
    Status status = OK;
    
    // if this page is deleted
    if(currIndex->empty()==true){
        return OK;
    }
    
    // read the key inside index record one by one
    RID currRID, prevRID;
    status = currIndex->firstRecord(currRID);               // read the first index RID
    do{
        char *recPtr;
        int recLen;
        
        status = currIndex->getRecord(currRID,recPtr,recLen);   // read the first index record
        
        // read the key from entry
        void *targetkey;  // the key in the tree
        Datatype *targetdata;
        get_key_data(targetkey, targetdata, (KeyDataEntry *)recPtr, recLen, INDEX); // must be index
        
        
        // compare the key
        int compareResult = keyCompare(key,targetkey,headerpage.keytype);
        if(compareResult==0){  // the key found
            recordpageId = targetdata->pageNo;
            return true;
        }
        else if(compareResult<0){
            child = targetdata->pageNo;
            return false;
        }
        // else the insert key is larger, move on to next key record
        
        prevRID = currRID;
        
    }
    while(currIndex->nextRecord(prevRID,currRID)==OK);
    
    
    return false;
}

// recursive search, return key location, index is used for first split
// called in insert()
// return LEAF
// Status BTreeFile::Search_index(pair<PageId&, PageId&> result, PageId parentPage, PageId currPage, const void *key, bool &reduflag)
Status BTreeFile::Search_record( PageId& result, PageId currPage, const void *key )
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
        status = ((BTIndexPage*)currIndex)->get_page_no(key,headerpage.keytype,child);
        status = MINIBASE_BM->unpinPage(currPage, 0, 1);
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
Status BTreeFile::Search_parent(const PageId targetchild, PageId currPage, const void *childkey, PageId& parent)
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
        status = ((BTIndexPage*)currIndex)->get_page_no(childkey, headerpage.keytype, child);
        status = MINIBASE_BM->unpinPage(currPage, 0, 1);
        
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
Status BTreeFile::Insert_helper(PageId insertLoc, const void *key, Datatype datatype, nodetype createdtype)
{
    Status status = OK;
    
    // base cases
    if(insertLoc==INVALID_PAGE){
        return DONE;
    }
    
    // make the entry intended to insert
    KeyDataEntry insertOne;
    int entryLen;
    make_entry(&insertOne,headerpage.keytype,key,createdtype, datatype, &entryLen); //only use to get the lenth
    
    // see if it need split
    SortedPage* targetPage;
    status = MINIBASE_BM->pinPage(insertLoc, (Page *&)targetPage, 1);
    status = MINIBASE_BM->unpinPage(insertLoc, 0, 1);  // because I may pin it again in split function
    
    if(targetPage->available_space()<entryLen){  // no enough space, need split
        
        // split into two page, <upkey, leftchild> will be copy or push up
        PageId leftchild;
        void *upkey;
        Split(insertLoc, leftchild, upkey);
        
        // check if it is the root
        if(insertLoc == headerpage.rootPageID){
            // create a new page, make rootPage point to it
            PageId newroot;
            BTIndexPage* rootpage;
            status = MINIBASE_BM->newPage(newroot, (Page* &)rootpage);
	rootpage->init(newroot);
            headerpage.rootPageID = newroot;
            headerpage.PageType = INDEX;    // once depth>1, the root is INDEX
            RID insertRid;
            rootpage->insertKey(upkey, headerpage.keytype, insertLoc, insertRid);
            status = MINIBASE_BM->unpinPage(newroot, 1, 1); // dirty
        }
        else{
            // search parent
            PageId parent;
            status = Search_parent(insertLoc, headerpage.rootPageID, key, parent); // search start from root
            
            // call Insert_helper to insert the overflowed key upper, second split cannot be type leaf, so dataRid is useless
            Datatype newdatatype;
            newdatatype.pageNo = leftchild;
            status = Insert_helper(parent, upkey, newdatatype,INDEX);
            
        }
        
        
    }
    else{ // enough space, directly insert into leaf, base case as well
        
        // insert the insertOne entry
        if(createdtype==INDEX){
            RID rid;
            ((BTIndexPage*)targetPage)->insertKey(key, headerpage.keytype, datatype.pageNo, rid);
        }
        else if(createdtype==LEAF){
            RID rid;
            ((BTLeafPage*)targetPage)->insertRec(key, headerpage.keytype, datatype.rid, rid);
        }
    }

    return OK;
}

// split the page into two page, copyup (leaf), pushup(index) key
// move former half to the new node, then we need not update parent's pointer
Status BTreeFile::Split(PageId splitTarget, PageId &leftchild, void *upkey)
{
    Status status = OK;
    
    // split the page by half size (not half number of entry)
    //int halfsize = MINIBASE_PAGESIZE/2;
    
    // pin the split target page
    SortedPage *rightchild;
    status = MINIBASE_BM->pinPage(splitTarget, (Page *&)rightchild, 1);
    nodetype ndtype = (nodetype)rightchild->get_type();  // be careful to match
    
    // get half number of entries
    int halfnum = rightchild->numberOfRecords()/2;
    
    // create the rightchild page
    SortedPage *newpage;
    status = MINIBASE_BM->newPage(leftchild, (Page* &)newpage);
    newpage->init(leftchild);
    // connect them with each other (if it is leaf)
    if(ndtype==LEAF){
        ((BTLeafPage*)newpage)->set_type(LEAF);
        
        PageId prev =rightchild->getPrevPage();
        newpage->setPrevPage(prev);
        
        SortedPage *prevPage;
        status = MINIBASE_BM->pinPage(prev, (Page* &)prevPage, 1);
        prevPage->setNextPage(leftchild);
        status = MINIBASE_BM->unpinPage(prev, 0, 1);
        
        newpage->setNextPage(splitTarget);
        rightchild->setPrevPage(leftchild);
        
    }
    else if(ndtype==INDEX){  //  do not need connection
        newpage->set_type(INDEX);
    }
    
    
    // traverse the target page, move latter half to rightchild
    int recordCnt = 0;
    RID currRID, prevRID;
    status = rightchild->firstRecord(currRID);               // read the first index RID
    do{
        KeyDataEntry recPtr;
        int recLen;
        
        status = rightchild->getRecord(currRID,(char*)&recPtr,recLen);   // get record
        recordCnt ++;
        
        // unpack the entry
        Keytype targetkey;
        Datatype targetdata;
        get_key_data(&targetkey, &targetdata, &recPtr,recLen, ndtype);
        
        // if it is the former half
        if (recordCnt < halfnum){
 
            // put this record into leftchild, cannot call sortpage insert directly, we need sorted insert
            RID newRecord;
            if(ndtype==LEAF){
                status = ((BTLeafPage*)newpage)->insertRec(&targetkey, headerpage.keytype, targetdata.rid, newRecord);
            }
            else if(ndtype==INDEX){
                status = ((BTIndexPage*)newpage)->insertKey(&targetkey, headerpage.keytype, targetdata.pageNo, newRecord);
            }
            
            // move on before delete
            prevRID = currRID;
            rightchild->nextRecord(prevRID,currRID);
            
            // delete this record from rightchild directly, do not need sort
            status = rightchild->deleteRecord(prevRID);
 
        }
        else{  // the first one in right child

            upkey = &targetkey;
            if(ndtype==INDEX){  // if index node, delete the key node for push up
                status = rightchild->nextRecord(prevRID,currRID);
            }
                // decide the up key
            break;
        }
        
    }while(1);
    
    // upin the split page
     status = MINIBASE_BM->unpinPage(splitTarget, 1, 1); // dirty
     status = MINIBASE_BM->unpinPage(leftchild, 1, 1);  // dirty
    
    return OK;
 }





