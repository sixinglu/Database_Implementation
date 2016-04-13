/******************************************************
 * Author: Sixing Lu, Yang Liu
 * Date: 2016-02-20
 ******************************************************/


#include "heapfile.h"
#include "buf.h"
#include <memory.h>

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
	"bad record id",
	"bad record pointer", 
	"end of file encountered",
	"invalid update operation",
	"no space on page for record", 
	"page is empty - no records",
	"last record on page",
	"invalid slot number",
	"file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
	// if the file already exists, do nothing, return
	if(MINIBASE_DB->get_file_entry(name,firstDirPageId)==OK){
		file_deleted = 0;    // set delete = false
		fileName = new char[strlen(name)+1];  // allocate spece for fileName
		strcpy(fileName,name);   // load file name
		returnStatus = OK;
	}
	else{

		file_deleted = 0;    // set delete = false
		fileName = new char[strlen(name)+1];  // allocate spece for fileName
		strcpy(fileName,name);   // load file name

		/******* allocate first directory page ***********/
		DataPageInfo firstdirinfo;
		RID firtrecRid;
		allocateDirSpace(&firstdirinfo,firstDirPageId,firtrecRid);  // only know firstDirPageId after allocate
		MINIBASE_DB->add_file_entry(fileName,firstDirPageId);	  // register an entry
		returnStatus = OK;
	}

}

// ******************
// Destructor
HeapFile::~HeapFile()
{
	if(file_deleted==0){  // still exist
		if(fileName==NULL){
			deleteFile();
		}
		else{
			MINIBASE_BM->flushAllPages();
		}
	}
	else{
		MINIBASE_BM->flushAllPages();
	}


}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
	int totalCnt=0;

	Status status;

	PageId iterate_dirPage = firstDirPageId;
	PageId next = firstDirPageId;
	HFPage* currdir;

	DataPageInfo dirinfo;  //read only
	int recLeninDir;

	///////////// traverse all dir pages link list  //////////////
	while(iterate_dirPage != INVALID_PAGE){   // when reach Pid=-1 means the end of the linklist

		status = MINIBASE_BM->pinPage( iterate_dirPage, (Page* &)currdir );   // read dir page from the disk
		if(status!=OK){
            MINIBASE_BM->unpinPage(iterate_dirPage);
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		// get information from dir page
		RID recordinPage;
		RID prevrecordinPage;
		status = currdir->firstRecord(recordinPage);
		if(status!=OK){
            MINIBASE_BM->unpinPage(iterate_dirPage);
			return MINIBASE_FIRST_ERROR(HEAPFILE,NO_RECORDS);
		}

		///////// traverse all dirinfos in one dir page  ///////////
		do{

			status = currdir->getRecord(recordinPage, (char*)&dirinfo, recLeninDir); // read recLeninDir out is useless here
			if(status!=OK){
                MINIBASE_BM->unpinPage(iterate_dirPage);
				return MINIBASE_FIRST_ERROR(HEAPFILE,NO_RECORDS);
			}

			totalCnt = totalCnt + dirinfo.recct;
			////////////////////////////////////////////////////////////////

			prevrecordinPage = recordinPage;     // shallow copy is enough for structure

		}while(currdir->nextRecord(prevrecordinPage,recordinPage)==OK);
		////////////////////////////////////////////////////////////////


		next = currdir->getNextPage();                       // must record before unpin
		status = MINIBASE_BM->unpinPage(iterate_dirPage);             // not dirty, unpin dir page
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		iterate_dirPage = next;

	}

	return totalCnt;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
	Status status;

	// search the directory, find a valid slot. if full, allocate new page
	PageId iterate_dirPage = firstDirPageId;
	PageId prev = firstDirPageId;
	PageId next = firstDirPageId;
	HFPage* currdir;
	DataPageInfo* dirinfo; // put it outside because if none of page available, this record the last DataPageInfo used to connect with new created Page

	int recLeninDir;
	bool successFlag = false;

	///////////// traverse all dir pages link list  //////////////
	while(iterate_dirPage != INVALID_PAGE){   // when reach Pid=-1 means the end of the linklist

		status = MINIBASE_BM->pinPage( iterate_dirPage, (Page* &)currdir );   // read dir page from the disk
		//cout<<"pin: "<<iterate_dirPage<<endl;

		if(status!=OK){
            MINIBASE_BM->unpinPage(iterate_dirPage);
            //cout<<"unpin: "<<iterate_dirPage<<endl;
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		// get information from dir page
		RID recordinPage;
		RID prevrecordinPage;
		status = currdir->firstRecord(recordinPage);
		if(status!=OK){
            MINIBASE_BM->unpinPage(iterate_dirPage);
            //cout<<"unpin: "<<iterate_dirPage<<endl;
			return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
		}

		///////// traverse all dirinfos in one dir page  ///////////
		do{

			status = currdir->returnRecord(recordinPage, (char*&)dirinfo, recLeninDir);  // read recLeninDir out is useless here
			//char* tmp = (char*)&dirinfo;
			//for(int m = 0; m < recLeninDir; m++)
			////printf("%d ",tmp[m]);
			////printf("\n");
			if(status!=OK){
                MINIBASE_BM->unpinPage(iterate_dirPage);
				return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
			}
			//printf("return good\n");
			// if the pointed page has space
			////printf("avail %d needed %d inserted %d\n",dirinfo->availspace,recLen,dirinfo->recct);
			if( dirinfo->availspace > recLen){  // avaiable space, read out the real record page
				HFPage* currpage;
				status = MINIBASE_BM->pinPage( dirinfo->pageId, (Page* &)currpage );  // the page contains real record
				//cout<<"pin: "<<dirinfo->pageId<<endl;

				if(status!=OK){
					MINIBASE_BM->unpinPage(dirinfo->pageId);
					//cout<<"unpin: "<<dirinfo->pageId<<endl;
                    MINIBASE_BM->unpinPage(iterate_dirPage);
                    //cout<<"unpin: "<<iterate_dirPage<<endl;
					return MINIBASE_FIRST_ERROR(HEAPFILE, status);
				}
				// //printf("pin good\n");
				//currpage->init(dirinfo.pageId);
				status = currpage->insertRecord(recPtr, recLen, outRid);   // insert the record
				//printf("insert pno %d sno %d to dir pno %d sno %d\n",outRid.pageNo,outRid.slotNo,recordinPage.pageNo,recordinPage.slotNo);
				if(status!=OK){
					//printf("insert bad\n");
                    MINIBASE_BM->unpinPage(dirinfo->pageId);
                    //cout<<"unpin: "<<dirinfo->pageId<<endl;
                    MINIBASE_BM->unpinPage(iterate_dirPage);
                    //cout<<"unpin: "<<iterate_dirPage<<endl;
					return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
				}
				//          //printf("insert good\n");
				dirinfo->availspace = currpage->available_space();    // reduce the space
				dirinfo->recct ++;   // increase record num

				status = MINIBASE_BM->unpinPage(dirinfo->pageId, true);              // dirty, unpin record page
				//cout<<"unpin: "<<dirinfo->pageId<<endl;

				if(status!=OK){
					return MINIBASE_FIRST_ERROR(HEAPFILE, status);
				}
				status = MINIBASE_BM->unpinPage(iterate_dirPage,true);                   // dirty, unpin dir page
				//cout<<"unpin: "<<iterate_dirPage<<endl;

				if(status!=OK){
					return MINIBASE_FIRST_ERROR(HEAPFILE, status);
				}

				successFlag = true;
				break;
			}

			prevrecordinPage = recordinPage;     // shallow copy is enough for structure
			//printf("not enough in cur dir slot pno %d sno %d\n",recordinPage.pageNo,recordinPage.slotNo);
		}while(currdir->nextRecord(prevrecordinPage,recordinPage)==OK);
		////////////////////////////////////////////////////////////////
		//printf("crrdir\n");
		if(successFlag==true){
			break;
		}
		else if(currdir->available_space()>int(sizeof(DataPageInfo))){   // but still have slot in this dir page, create a new page for record, and

			DataPageInfo newdpinfop;
			status = newDataPage(&newdpinfop);  // create a new data page
			HFPage* readPage;

			status = MINIBASE_BM->pinPage(newdpinfop.pageId, (Page *&)readPage);
			//cout<<"pin: "<<newdpinfop.pageId<<endl;

			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}

			// link this data page with prev record data page
			HFPage* prevpage;
			status = MINIBASE_BM->pinPage( dirinfo->pageId, (Page* &)prevpage );
			//cout<<"pin: "<<dirinfo->pageId<<endl;

			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}
			readPage->setPrevPage(dirinfo->pageId);
			prevpage->setNextPage(newdpinfop.pageId);
			status = MINIBASE_BM->unpinPage( dirinfo->pageId, true );
			//cout<<"unpin: "<<dirinfo->pageId<<endl;

			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}

			// insert the new record data
			status = readPage->insertRecord(recPtr,recLen,outRid);
			//printf("insert pno %d sno %d to dir pno %d sno %d\n",outRid.pageNo,outRid.slotNo,recordinPage.pageNo,recordinPage.slotNo);
			if(status!=OK){
                MINIBASE_BM->unpinPage(newdpinfop.pageId);
                //cout<<"unpin: "<<newdpinfop.pageId<<endl;
                MINIBASE_BM->unpinPage(iterate_dirPage);
                //cout<<"unpin: "<<iterate_dirPage<<endl;
				return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
			}
			newdpinfop.availspace = readPage->available_space();
			newdpinfop.recct = 1;
			status = MINIBASE_BM->unpinPage(newdpinfop.pageId,true);   // dirty
			//cout<<"unpin: "<<newdpinfop.pageId<<endl;

			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}

			// insert this new dir record into this dir page
			RID DirpageRid;
			status = currdir->insertRecord((char*)&newdpinfop, sizeof(DataPageInfo), DirpageRid);
			//printf("insert data'spno %d to dir pno %d sno %d\n",newdpinfop.pageId,DirpageRid.pageNo,DirpageRid.slotNo);
			if(status!=OK){
                MINIBASE_BM->unpinPage(iterate_dirPage);
                //cout<<"unpin: "<<iterate_dirPage<<endl;
				return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
			}
			//currdir->nextRecord(prevrecordinPage,recordinPage);
			//printf("dir expanded\n");
			status = MINIBASE_BM->unpinPage(iterate_dirPage,true);                     // not dirty, unpin dir page
			//cout<<"unpin: "<<iterate_dirPage<<endl;

			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}
			successFlag = true;
			break;

		}

		next = currdir->getNextPage();                       // must record before unpin
		prev = iterate_dirPage;
		status = MINIBASE_BM->unpinPage(iterate_dirPage);                     // not dirty, unpin dir page
		//cout<<"unpin: "<<iterate_dirPage<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		iterate_dirPage = next;

	}
	///////////////////////////////////////////////////////////


	// if all dir pages are full, allocate new space
	if(successFlag==false){

		// allocate new page from the db
		DataPageInfo NewDirinfo;
		RID allocDataPageRid;
		PageId NewDirPageId;
		status = allocateDirSpace(&NewDirinfo,NewDirPageId,allocDataPageRid);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
		}

		// insert record
		HFPage* recordPage;
		MINIBASE_BM->pinPage(allocDataPageRid.pageNo,(Page* &)recordPage);
		//cout<<"pin: "<<allocDataPageRid.pageNo<<endl;

		status = recordPage->insertRecord(recPtr, recLen, outRid);
		if(status!=OK){
            MINIBASE_BM->unpinPage(allocDataPageRid.pageNo);
            //cout<<"unpin: "<<allocDataPageRid.pageNo<<endl;
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}


		// connect prev record page with this new created record data page
		HFPage* prevpage;
		status = MINIBASE_BM->pinPage( dirinfo->pageId, (Page* &)prevpage );
		//cout<<"pin: "<<dirinfo->pageId<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		recordPage->setPrevPage(dirinfo->pageId);
		prevpage->setNextPage(allocDataPageRid.pageNo);

		status = MINIBASE_BM->unpinPage( dirinfo->pageId, true );
		//cout<<"unpin: "<<dirinfo->pageId<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		status = MINIBASE_BM->unpinPage(allocDataPageRid.pageNo, true);
		//cout<<"unpin: "<<allocDataPageRid.pageNo<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}


		// set currect dir page's first dirinfo availspace, recnt
		HFPage* currDirPage;
		RID NewDirRid;
		status = MINIBASE_BM->pinPage( NewDirPageId, (Page* &)currDirPage );
		//cout<<"pin: "<<NewDirPageId<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		status = currDirPage->firstRecord(NewDirRid);
		status = currDirPage->returnRecord(NewDirRid, (char*&)dirinfo, recLeninDir);   // read recLeninDir out is useless
		dirinfo->availspace = recordPage->available_space();
		dirinfo->recct ++;

		// connect prev dir page with this new created dir page
		HFPage* prevDirPage;
		status = MINIBASE_BM->pinPage(prev,(Page* &)prevDirPage);
		//cout<<"pin: "<<prev<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		prevDirPage->setNextPage(NewDirPageId);
		currDirPage->setPrevPage(prev);
		status = MINIBASE_BM->unpinPage(prev, true);
		//cout<<"unpin: "<<prev<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		status = MINIBASE_BM->unpinPage(NewDirPageId, true);
		//cout<<"unpin: "<<NewDirPageId<<endl;

		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

	}


	return OK;

} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
	Status status = OK;
	PageId rpDirPageId;
	HFPage *rpdirpage;
	PageId rpDataPageId;
	HFPage *rpdatapage;
	RID rpDataPageRid;    //  according dirinfo rid
	DataPageInfo dpinfop;
	int dirLen;
	//printf("heap delete pno %d sno %d\n",rid.pageNo,rid.slotNo);
	status = findDataPage(rid,rpDirPageId, rpdirpage,rpDataPageId, rpdatapage,rpDataPageRid);
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, status);
	}

	if(rpdatapage!=NULL){  // find the record
		status = MINIBASE_BM->pinPage(rpDirPageId, (Page*&)rpdirpage);     // pin here again
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		status = MINIBASE_BM->pinPage(rpDataPageId, (Page*&)rpdatapage);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		int beforeDelavSpace = rpdatapage->available_space();
		status = rpdatapage->deleteRecord(rid);    // delete record
		if(status!=OK){
            MINIBASE_BM->unpinPage(rpDirPageId);
            MINIBASE_BM->unpinPage(rpDataPageId);
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		int afterDelavSpace = rpdatapage->available_space();

		status = rpdirpage->returnRecord(rpDataPageRid, (char *&)dpinfop, dirLen);
		if(status!=OK){
            MINIBASE_BM->unpinPage(rpDirPageId);
            MINIBASE_BM->unpinPage(rpDataPageId);
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		dpinfop.availspace = dpinfop.availspace + (afterDelavSpace-beforeDelavSpace);
		dpinfop.recct--;


		status = MINIBASE_BM->unpinPage(rpDataPageId,true);   // unpin back here
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		status = MINIBASE_BM->unpinPage(rpDirPageId,true);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
		//        }


		return OK;
}
else{
	return DONE;
}

}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
	Status status = OK;
	PageId rpDirPageId;
	HFPage *rpdirpage;
	PageId rpDataPageId;
	HFPage *rpdatapage;
	RID rpDataPageRid;    //  according dirinfo rid

	status = findDataPage(rid,rpDirPageId, rpdirpage,rpDataPageId, rpdatapage,rpDataPageRid);
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, status);
	}

	if(rpdatapage!=NULL){  // find the record
	   // MINIBASE_BM->pinPage(rpDirPageId, (Page*&)rpdirpage);     // pin here again
		status = MINIBASE_BM->pinPage(rpDataPageId, (Page*&)rpdatapage);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		char *recordPtr;
		int oldLen;
		status = rpdatapage->returnRecord(rid, recordPtr, oldLen);
		if(status!=OK){
			MINIBASE_BM->unpinPage(rpDataPageId);
			return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
		}
		if(oldLen!=recLen){   // if the size of the record changes, cannot update
//            MINIBASE_BM->unpinPage(rpDirPageId);
            MINIBASE_BM->unpinPage(rpDataPageId);
			return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
		}
		memcpy(recordPtr,recPtr,recLen);  // update content
//
		status = MINIBASE_BM->unpinPage(rpDataPageId,true);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}


	}
	return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
	Status status = OK;
	PageId rpDirPageId;
	HFPage *rpdirpage;
	PageId rpDataPageId;
	HFPage *rpdatapage;
	RID rpDataPageRid;   // //  according dirinfo rid

	findDataPage(rid,rpDirPageId, rpdirpage,rpDataPageId, rpdatapage,rpDataPageRid);

	if(rpdatapage!=NULL){ // find the record

		status = MINIBASE_BM->pinPage(rpDirPageId, (Page*&)rpdirpage);     // pin here again
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		status = MINIBASE_BM->pinPage(rpDataPageId, (Page*&)rpdatapage);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}


		// recPtr = new char[recLen+1];  // outside already has array[]
		status = rpdatapage->getRecord(rid, recPtr, recLen);
		if(status!=OK){
            MINIBASE_BM->unpinPage(rpDirPageId);
            MINIBASE_BM->unpinPage(rpDataPageId);
			return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
		}

		status = MINIBASE_BM->unpinPage(rpDataPageId);   // unpin back here
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		status = MINIBASE_BM->unpinPage(rpDirPageId);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		return OK;
	}
	else{
		cout<<"cannot find the record!"<<endl;
	}


	return status;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
	Scan *scanner = new Scan(this,status);

	return scanner;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
	Status status;
	if(file_deleted==0){

		PageId iterate_dirPage = firstDirPageId;
		PageId next = firstDirPageId;
		HFPage* currdir;

		DataPageInfo* dirinfo;  //read only
		int recLeninDir;

		///////////// traverse all dir pages link list  //////////////
		while(iterate_dirPage != INVALID_PAGE){   // when reach Pid=-1 means the end of the linklist

			status = MINIBASE_BM->pinPage( iterate_dirPage, (Page* &)currdir );   // read dir page from the disk

			// debug
			//cout<<"pin_dirPage: "<<iterate_dirPage<<endl;

			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}
			//printf("file torn down loop \n");
			// get information from dir page
			RID recordinPage;
			RID prevrecordinPage;
			status = currdir->firstRecord(recordinPage);
			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
			}

			///////// traverse all dirinfos in one dir page  ///////////
			do{
				//printf("file torn down pno %d, sno %d\n",recordinPage.pageNo,recordinPage.slotNo);

				status = currdir->returnRecord(recordinPage, (char*&)dirinfo, recLeninDir); // read recLeninDir out is useless here
				if(status!=OK){
                    MINIBASE_BM->unpinPage(iterate_dirPage);
					return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
				}

				HFPage* currpage;
				status = MINIBASE_BM->pinPage(dirinfo->pageId, (Page* &)currpage );
				if(status!=OK){
					return MINIBASE_FIRST_ERROR(HEAPFILE, status);
				}

				RID DataPageRid;
				RID prevDataRid;
				status = currpage->firstRecord(DataPageRid);
				if(status!=OK){
                    MINIBASE_BM->unpinPage(iterate_dirPage);
                    MINIBASE_BM->unpinPage(dirinfo->pageId);
					return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
				}
				do{
					currpage->deleteRecord(DataPageRid);
					//printf("data pno %d sno %d\n",DataPageRid.pageNo,DataPageRid.slotNo);
					//printf("deleted pno %d, sno %d\n",DataPageRid.pageNo,DataPageRid.slotNo);
					prevDataRid = DataPageRid;

				}
				while(currpage->nextRecord(prevDataRid, DataPageRid) == OK);
				////////////////////////////////////////////////////////////////

				status = MINIBASE_BM->unpinPage(dirinfo->pageId,true);
				if(status!=OK){
					return MINIBASE_FIRST_ERROR(HEAPFILE, status);
				}

//				status = MINIBASE_BM->freePage(dirinfo->pageId);
//				if(status!=OK){
//					return MINIBASE_FIRST_ERROR(HEAPFILE, status);
//				}

				currdir->deleteRecord(recordinPage);


				prevrecordinPage = recordinPage;     // shallow copy is enough for structure

			}while(currdir->nextRecord(prevrecordinPage,recordinPage)==OK);
			////////////////////////////////////////////////////////////////

			next = currdir->getNextPage();                       // must record before unpin
			MINIBASE_BM->unpinPage(iterate_dirPage,true);

			// debug
			//cout<<"unpin_dirPage: "<<iterate_dirPage<<endl;

//			status = MINIBASE_BM->freePage(iterate_dirPage);       // free dir page
//			if(status!=OK){
//				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
//			}

			iterate_dirPage = next;
			//firstDirPageId = next;
		}
		//MINIBASE_BM->unpinPage(firstDirPageId,true);
		file_deleted =1;
		status = MINIBASE_DB->delete_file_entry(fileName);
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}
	}

	return OK;
}

/*Status HeapFile::deleteFile()
  {
  firstDirPageId = INVALID_PAGE;
  return OK;
  }*/
// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
	Status status;

	// allocate space for a new record page, and insert record
	HFPage* recordPage; // = new HFPage();
	PageId recordID;
	status = MINIBASE_BM->newPage(recordID, (Page* &)recordPage);  // allocate space for new record page
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
	}
	//    status = recordPage->insertRecord(recPtr,recLen,outRid);
	recordPage->init(recordID);
	// createa new record page info
	//DataPageInfo newdpinfop;
	dpinfop->availspace = recordPage->available_space();
	dpinfop->pageId = recordID;
	dpinfop->recct = 0;

	status = MINIBASE_BM->unpinPage(recordID, true);
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, status);
	}

	return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
		PageId &rpDirPageId, HFPage *&rpdirpage,
		PageId &rpDataPageId,HFPage *&rpdatapage,
		RID &rpDataPageRid)
{
	Status status;
	rpdirpage = NULL;
	rpdatapage = NULL;

	PageId iterate_dirPage = firstDirPageId;
	PageId next = firstDirPageId;
	HFPage* currdir;

	DataPageInfo dirinfo;  //read only
	int recLeninDir;

	///////////// traverse all dir pages link list  //////////////
	while(iterate_dirPage != INVALID_PAGE){   // when reach Pid=-1 means the end of the linklist

		status = MINIBASE_BM->pinPage( iterate_dirPage, (Page* &)currdir );   // read dir page from the disk
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		// get information from dir page
		RID recordinPage;
		RID prevrecordinPage;
		status = currdir->firstRecord(recordinPage);
		if(status!=OK){
            MINIBASE_BM->unpinPage(iterate_dirPage);
			return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
		}

		///////// traverse all dirinfos in one dir page  ///////////
		do{

			status = currdir->getRecord(recordinPage, (char*)&dirinfo, recLeninDir); // read recLeninDir out is useless here
			if(status!=OK){
                MINIBASE_BM->unpinPage(iterate_dirPage);
				return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
			}
			//printf("find data curr_dir pno %d sno %d containing %d, target pno %d sno%d \n",recordinPage.pageNo,recordinPage.slotNo,dirinfo.pageId,rid.pageNo,rid.slotNo);
			if(dirinfo.pageId != rid.pageNo){   // skip if pageNO not match

				prevrecordinPage = recordinPage;

				continue;
			}

			// read record out
			HFPage* currpage;
			status = MINIBASE_BM->pinPage(dirinfo.pageId, (Page* &)currpage );
			if(status!=OK){
                MINIBASE_BM->unpinPage(iterate_dirPage);
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}

			//////////// traverse all records in the record pages ////////////
			RID DataPageRid;
			RID prevDataRid;
			status = currpage->firstRecord(DataPageRid);
			if(status!=OK){
                MINIBASE_BM->unpinPage(iterate_dirPage);
                MINIBASE_BM->unpinPage(dirinfo.pageId);
				return MINIBASE_FIRST_ERROR(HEAPFILE, NO_RECORDS);
			}

			do{
				//printf("data pno %d sno %d\n",DataPageRid.pageNo,DataPageRid.slotNo);
				if(DataPageRid.slotNo==rid.slotNo){    // find the record
					rpDirPageId = iterate_dirPage;
					rpDataPageId = dirinfo.pageId;
					rpdirpage = currdir;
					rpdatapage = currpage;
					rpDataPageRid = recordinPage;                // according dirinfo RID
					status = MINIBASE_BM->unpinPage(dirinfo.pageId);   // do not uppin here, still need to use
					if(status!=OK){
						return MINIBASE_FIRST_ERROR(HEAPFILE, status);
					}

					status = MINIBASE_BM->unpinPage(iterate_dirPage);
					if(status!=OK){
						return MINIBASE_FIRST_ERROR(HEAPFILE, status);
					}

					return OK;
				}

				prevDataRid = DataPageRid;

			}while(currpage->nextRecord(prevDataRid, DataPageRid) == OK);
			////////////////////////////////////////////////////////////////
			status = MINIBASE_BM->unpinPage(dirinfo.pageId);
			if(status!=OK){
				return MINIBASE_FIRST_ERROR(HEAPFILE, status);
			}

			prevrecordinPage = recordinPage;     // shallow copy is enough for structure

		}while(currdir->nextRecord(prevrecordinPage,recordinPage)==OK);
		////////////////////////////////////////////////////////////////


		next = currdir->getNextPage();                       // must record before unpin
		status = MINIBASE_BM->unpinPage(iterate_dirPage);             // not dirty, unpin dir page
		if(status!=OK){
			return MINIBASE_FIRST_ERROR(HEAPFILE, status);
		}

		iterate_dirPage = next;

	}

	return DONE;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
		PageId &allocDirPageId,
		RID &allocDataPageRid)
{
	Status status;

	// allocate space for a new directory page
	HFPage* dirpage;  // = new HFPage();
	status = MINIBASE_BM->newPage(allocDirPageId, (Page* &)dirpage);  // allocate space for new dir page
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
	}

	dirpage->init(allocDirPageId);
	dirpage->setPrevPage(INVALID_PAGE);
	dirpage->setNextPage(INVALID_PAGE);


	// allocate space for a new record page
	HFPage* recordPage; // = new HFPage();
	status = MINIBASE_BM->newPage(allocDataPageRid.pageNo, (Page* &)recordPage);  // allocate space for new record page
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
	}
	recordPage->init(allocDataPageRid.pageNo);
	// give the pointed record page info
	dpinfop->pageId = allocDataPageRid.pageNo;
	dpinfop->availspace = MINIBASE_PAGESIZE;   // /sizeof(DataPageInfo);
	dpinfop->recct = 0;
	//printf("first record's pno %d avail space%d\n",dpinfop->pageId,dpinfop->availspace);
	// write the dpinfop to dir page
	RID DirpageRid;
	status = dirpage->insertRecord((char*)dpinfop, sizeof(DataPageInfo), DirpageRid);
	if(status!=OK){
        MINIBASE_BM->unpinPage(allocDataPageRid.pageNo);
        MINIBASE_BM->unpinPage(allocDirPageId);
		return MINIBASE_FIRST_ERROR(HEAPFILE, status);
	}

	status = MINIBASE_BM->unpinPage(allocDataPageRid.pageNo, true);
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, status);
	}

	status = MINIBASE_BM->unpinPage(allocDirPageId, true);
	if(status!=OK){
		return MINIBASE_FIRST_ERROR(HEAPFILE, status);
	}
	//printf("first dir pno at alloc %d\n",firstDirPageId);
	return OK;
}

// *******************************************
