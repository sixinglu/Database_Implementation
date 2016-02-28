/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

// Question: the peekNext func is wired
// do we need to define mvNext func?
// how to unpin all pages in the constructor together?  traverse all pages again?


// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{
    status = init(hf);
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
    //MINIBASE_BM->unpinPage(dirPageId);

}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
    Status status;
    
    if(scanIsDone==1){
status = MINIBASE_BM->unpinPage(dirPageId);
        return DONE;

    }

	// directly scan through all data record pages
	HFPage *currDataPage;
	status = MINIBASE_BM->pinPage(dataPageId, (Page*&)currDataPage);
	rid = userRid;
	status = currDataPage->getRecord(rid, recPtr, recLen);

	// update next
	RID nextRecordid;
	status = currDataPage->nextRecord(userRid, nextRecordid);
	if(status==OK){
		userRid = nextRecordid;
		status = MINIBASE_BM->unpinPage(dataPageId);
	}
	else{   // reach the end of the record in this page
		HFPage *nextDataPage;
		PageId nextDataPageId = currDataPage->getNextPage();
		if (nextDataPageId==INVALID_PAGE){
//printf("rid %d");
//printf("curr next fail pno %d, sno %d\n",rid.pageNo,rid.slotNo);
			scanIsDone= 1;
			status = MINIBASE_BM->unpinPage(dataPageId);

			return OK;
		}
		status = MINIBASE_BM->pinPage(nextDataPageId, (Page*&)nextDataPage);
		status = nextDataPage->firstRecord(nextRecordid);
		//status = nextDataPage->getRecord(rid, recPtr, recLen);

		status = MINIBASE_BM->unpinPage(dataPageId);      // unpin curr page
		status = MINIBASE_BM->unpinPage(nextDataPageId);  // unpin next page

		// update class member
		dataPageId = nextDataPageId;
		dataPage = nextDataPage;
		userRid = nextRecordid;

	}


    
  return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
    Status status;
    
    _hf = hf;
    dirPageId = hf->firstDirPageId;
          printf("kabbbooooom %d\n",hf->firstDirPageId);
    dataPageId = INVALID_PAGE;
    dataPageRid.pageNo = INVALID_PAGE;
    dataPageRid.slotNo = INVALID_SLOT;
    
    scanIsDone = 0;
    nxtUserStatus = 0;
    scanIsDone = 0;
    
    status = firstDataPage();

    return status;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
    Status status;
    
    _hf = NULL;
    dirPageId = INVALID_PAGE;
    dataPageId = INVALID_PAGE;
    dataPageRid.pageNo = INVALID_PAGE;
    dataPageRid.slotNo = INVALID_SLOT;
    
    scanIsDone = 0;
    nxtUserStatus = 0;
    
    status = MINIBASE_BM->unpinPage(dataPageId);
    status = MINIBASE_BM->unpinPage(dirPageId);
    return status;
}

// *******************************************
// Copy data about first page in the file.
// no any outside interface, it is a pure inner func
Status Scan::firstDataPage()
{
    // NOTE: when call this func, dirPageId should always be hf->firstDirPageId. which means this func won't be called in the middle of scan.

    Status status;
    
    // read first dir page out
    HFPage *firstDirPage;
    status = MINIBASE_BM->pinPage(dirPageId, (Page*&)firstDirPage);  // dirPageId always be firstDirPageId
    dirPage = firstDirPage;
    
      printf("kabbbooooom %d\n",dirPageId);
    // read first dir record out
    RID firstDirRid;
    DataPageInfo dirinfo;
    int recDirLen;
    status = firstDirPage->firstRecord(firstDirRid);
    status = firstDirPage->getRecord(firstDirRid, (char*)&dirinfo, recDirLen);
    
    dataPageRid = firstDirRid;
    
    
    // read according record page out
    HFPage *recordDataPage;
    status = MINIBASE_BM->pinPage(dirinfo.pageId,(Page*&)recordDataPage);
    
    dataPageId = dirinfo.pageId;
    dataPage = recordDataPage;
    
    
    // read according record out
    RID firstDataRid;
    char *recPtr;
    int dataLen;
    status = recordDataPage->firstRecord(firstDataRid);
    status = recordDataPage->returnRecord(firstDataRid, recPtr, dataLen);  // I use returnRecord here because I don't want to allocate space for recPtr. still safe, because recPtr will not pass out
    
    userRid = firstDataRid;
    
    RID next;
    if(recordDataPage->nextRecord(firstDataRid, next)==OK){
        nxtUserStatus = 1;
    }
    else{
        nxtUserStatus = 0;
    }
  
    status = MINIBASE_BM->unpinPage(dirinfo.pageId);
    //status = MINIBASE_BM->unpinPage(dirPageId);

    return status;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
    
    return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
  // put your code here
  return OK;
}
