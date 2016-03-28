/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "buf.h"


using namespace std;

// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",  
  "Inserting a duplicate entry in the hash table",  //1
  "Removing a non-existing entry from the hash table",  //2
  "Page not in hash table", //3
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};
void hashPrinting(unsigned index,vector< vector<pair<PageId,unsigned> > > directory,char*);
// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
    
    this->numBuffers = numbuf;
    this->bufPool = new Page[numBuffers];
   //vector<int> bufDescr2(4,100);
    for(int i =0; i<numbuf; i++){
        descriptors Pagedescr;
        Pagedescr.page_number = INVALID_PAGE;
        Pagedescr.dirtybit = false;
        Pagedescr.pin_count = 0;
	//cout<<bufDescr.size()<<endl;
	//bufDescr2.push_back(1);
        bufDescr.push_back(Pagedescr);
	MRUlist.push_back(numbuf - i - 1);
    }
    
    for(int j=0; j < HTSIZE; j++){
        vector<pair<PageId,unsigned> > temp(0);
        directory.push_back(temp);
    }
    
    
}

//*************************************************************
//** hash function to map the PageID to the bucket
//** return hash index in directroy
//************************************************************
unsigned BufMgr::hash(PageId PID){
    
    return (unsigned)(HASH_a*PID+HASH_b)%HTSIZE;
}

//*************************************************************
//** search the frame number through hash directory
//** return frame number, if -1 NOT FOUND
//************************************************************
int BufMgr::SearchPage(PageId PID){
    
    int frameNUM = -1;
    unsigned index = hash(PID);
   // if(PID == 23) hashPrinting(index,directory,"before search");
    //cout<<"inside: "<< index <<" "<<directory.at(index).size()<<endl;

    for(unsigned i =0; i<directory.at(index).size(); i++){
	//cout<<i<<" dir["<<index<<"] size "<<directory.at(index).size()<<endl;
        if(directory.at(index).at(i).first==PID){
            return directory.at(index).at(i).second;
        }
    }
   
//    if(frameNUM==-1){
//        cout<<bufErrMsgs[3]<<" pid is"<<PID<<endl;
//    }
    
    return frameNUM;
}

//*************************************************************
//** add a index in hash table after insert a page
//** return status, if already in buffer, return DONE
//************************************************************
Status BufMgr::HashAdd(PageId PID, unsigned frameNUM){
    
    Status status = OK;
    unsigned index = hash(PID);
    
    if(SearchPage(PID)!=-1){  // if already exists
        cout<<bufErrMsgs[1]<<endl;
        return DONE;
    }

    directory.at(index).push_back(make_pair(PID, frameNUM));
   // if(PID == 23) hashPrinting(index,directory,"after adding");
    return status;
}

void hashPrinting(unsigned index,vector< vector<pair<PageId,unsigned> > > directory,char* loc){
    for(unsigned i =0; i<directory.at(index).size(); i++){
cout<<directory.at(index).at(i).first<<' '<<directory.at(index).at(i).second<<endl;

        }
//cout<<loc<<" dir["<<index<<"] size"<<directory.at(index).size()<<endl;
    
}
//*************************************************************
//** delete the index in hash table after delete a page
//** return status
//************************************************************
Status BufMgr::HashDelete(PageId PID){
    
    Status status = OK;
    unsigned index = hash(PID);
    unsigned i =0;


    for(i =0; i<directory.at(index).size(); i++){
        if(directory.at(index).at(i).first==PID){
//cout<<"bfeore delete: "<< directory.at(index).size()<<"dir index "<<index<<endl;
            directory.at(index).erase(directory.at(index).begin()+i);
           // cout<<"after delete: "<< directory.at(index).size()<<endl;
            break;
        }
    }
     //hashPrinting(index,directory,"after delete");
    if(i>directory.at(index).size()){ // if not exist
	//cout<<directory.at(index).size()<<' '<<PID<<' '<<index<<endl;
        cout<<bufErrMsgs[2]<<endl;
        return DONE;
    }
    
    return status;
}




//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
	this->flushAllPages();
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  // put your code here
        // Check if this page is in buffer pool, otherwise
        // find a frame for this page, read in and pin it.
        // also write out the old page if it's dirty before reading
        // if emptyPage==TRUE, then actually no read is done to bring
        // the page
//printf("pinnnnnnnnnnn\n");
	Status status;
	int frame = SearchPage(PageId_in_a_DB);
	if( frame != -1){	
		//printf("already in\n");
		bufDescr[frame].pin_count ++;
		page = &bufPool[frame];
	return OK;
	}
	else {
		int Love_Hate;
		unsigned MRU_LRU_index;
		status = findReplaceFrame(Love_Hate, MRU_LRU_index, frame);
		//printf("frame victim %d\n",frame);
		if(status != OK)
			return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERFULL );

		if(bufDescr[frame].page_number != INVALID_PAGE){
			//update directory
    //hashPrinting(2,directory,"before delete");
    //hashPrinting(3,directory,"before delete");
			HashDelete(bufDescr[frame].page_number);
    //hashPrinting(2,directory,"after delete");
    //hashPrinting(3,directory,"after delete");
		}
		if(bufDescr[frame].dirtybit == 1){
			//write back
			status = MINIBASE_DB->write_page(bufDescr[frame].page_number, &bufPool[frame]);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
		}
//hashPrinting(3,directory,"pinned");
		//update directory
		HashAdd(PageId_in_a_DB,frame);
//hashPrinting(3,directory,"hashadded");
		//pin the new page first before reading it in
		bufDescr[frame].page_number = PageId_in_a_DB;
		bufDescr[frame].pin_count = 1;
		bufDescr[frame].dirtybit = 0;
		if(emptyPage == 0){
			//read in something
			status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[frame]);	
			if(status != OK)
			{//undo if error occurs
cout<<"error in db reading"<<endl;
				bufDescr[frame].page_number = INVALID_PAGE;
				bufDescr[frame].dirtybit = 0;
				bufDescr[frame].pin_count--;
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
			}
		}
		//actual return page
		page = &bufPool[frame];
//hashPrinting(3,directory,"pageassigned");
	//update replacer policy
	if(Love_Hate == 1)//mru
		MRUlist.erase(MRUlist.begin() + MRU_LRU_index);
	else if(Love_Hate ==0)//lru
		LRUlist.erase(LRUlist.begin() + MRU_LRU_index);
	//printf("pin page %d at frame %d\n",PageId_in_a_DB,frame);
	//frame = SearchPage(PageId_in_a_DB);
	//cout<<frame<<endl;
	//hashPrinting(3,directory,"pinned");
  return OK;
	}

}//end pinPage

//*************************************************************
//** If a page is needed for replacement, the buffer manager
//** selects from the list of hated pages first and then from
//** the loved pages if no hated   ones exist.
//** return Love_Hate = 0: LRUlist, Love_Hate = 1: MRUlist
//** return MRU_LRU_index, frameID
//************************************************************
Status BufMgr::findReplaceFrame(int &Love_Hate, unsigned &MRU_LRU_index, int &frameID)
{
    
    // search hated pages firstly, MRUlist, FILO
    for(int i=(int)MRUlist.size()-1; i>=0; i--){  // must use int, otherwise unsigned number >0 forever
        unsigned frame =  MRUlist.at(i);
        if(bufDescr.at(frame).pin_count==0){  // not in use else where
            Love_Hate = 1;
            MRU_LRU_index = i;
            frameID = frame;
            return OK;
        }
    }
    
    // search loved page secondly if no hated frame available, LRUlist, FIFO
    for(unsigned i =0; i<LRUlist.size(); i++){
        unsigned frame = LRUlist.at(LRUlist.size() - i - 1);
        if(bufDescr.at(frame).pin_count==0){  // not in use else where
            Love_Hate = 0;
            MRU_LRU_index = LRUlist.size() - i - 1;
            frameID = frame;
            return OK;
        }
    }
    
    
    // no frame available, all pined some other places
    return DONE;
    
}


//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
  // put your code here

	//printf("going inside\n");
	int frame = SearchPage(page_num);



	if( frame != -1){	
		//printf("frame %d already in\n",frame);
		bufDescr[frame].pin_count --;
		if(frame < 0 || frame >= (int)numBuffers)
                	return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
		if(bufDescr[frame].pin_count < 0 || bufDescr[frame].page_number == INVALID_PAGE)
			return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERPAGENOTPINNED);
	}
	else{
		return MINIBASE_FIRST_ERROR( BUFMGR, HASHNOTFOUND);
		}

	if(bufDescr[frame].pin_count == 0){
		bufDescr[frame].dirtybit = dirty;
		if(hate == FALSE){//go to lrulist
			    for(unsigned i =0; i<MRUlist.size(); i++){
				unsigned mru_frame = MRUlist.at(i);
				if(mru_frame == (unsigned)frame){  // love overcomes hate
					MRUlist.erase(MRUlist.begin() + i);
       				 }
  			    }
			    for(unsigned i =0; i<LRUlist.size(); i++){
				unsigned lru_frame = LRUlist.at(i);
				if(lru_frame == (unsigned)frame){  // already loved
					LRUlist.erase(LRUlist.begin() + i);
       				 }
  			    }
			    LRUlist.insert(LRUlist.begin(),frame);
		}
		else {//go to mru list
			    for(unsigned i =0; i<LRUlist.size(); i++){
				unsigned lru_frame = LRUlist.at(i);
				if(lru_frame == (unsigned)frame){  // already loved, do nothing
					return OK;
       				 }
  			    }
			    for(unsigned i =0; i<MRUlist.size(); i++){
				unsigned mru_frame = MRUlist.at(i);
				if(mru_frame == (unsigned)frame){  // already hated
					MRUlist.erase(MRUlist.begin() + i);
       				 }
  			    }
			    MRUlist.push_back(frame);
		}
	}
/*
	for(unsigned i =0; i<LRUlist.size(); i++){
	cout<<LRUlist.at(i)<<' ';
	}
	cout<<endl;
	for(unsigned i =0; i<MRUlist.size(); i++){
	cout<<MRUlist.at(i)<<' ';
	}
	cout<<endl;
*/
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
        // call DB object to allocate a run of new pages and 
        // find a frame in the buffer pool for the first page
        // and pin it. If buffer is full, ask DB to deallocate 
        // all these pages and return error
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany ) {
  // put your code here
	Status status;
	status = MINIBASE_DB->allocate_page(firstPageId, howmany);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
	status = pinPage(firstPageId, firstpage, TRUE);
	if(status != OK){
		MINIBASE_DB->deallocate_page(firstPageId, howmany);
		return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
		}
	
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
        // User should call this method if it needs to delete a page
        // this routine will call DB to deallocate the page 

Status BufMgr::freePage(PageId globalPageId){
	int frame = SearchPage(globalPageId);

	if( frame != -1){	
		//printf("frame %d already in\n",frame);
		if(frame < 0 || frame >= (int)numBuffers)
                	return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
		if(bufDescr[frame].pin_count > 0 || bufDescr[frame].page_number == INVALID_PAGE)
			return DONE;
		MINIBASE_DB->deallocate_page(globalPageId);
			return OK;
	}
	return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  // put your code here
    
    Status status = OK;
    
    int frame = SearchPage(pageid);
    
    if(frame!=-1) {
        if(frame < 0 || frame >= (int)numBuffers)
            return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
        
        if(bufDescr[frame].pin_count > 0 || bufDescr[frame].page_number == INVALID_PAGE)
            return DONE;
        
        if (bufDescr[frame].dirtybit==true){
            status = MINIBASE_DB->write_page(bufDescr[frame].page_number, &bufPool[frame]);
            if(status != OK)
                return MINIBASE_CHAIN_ERROR(BUFMGR, status);
        }
    }
    
    
    return status;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  
    Status status = OK;
    
    for(unsigned i =0; i< bufDescr.size(); i++){
        if(bufDescr[i].pin_count > 0)
            continue;
        
        if (bufDescr[i].dirtybit==true){
            status = MINIBASE_DB->write_page(bufDescr[i].page_number, &bufPool[i]);
            if(status != OK)
                return MINIBASE_CHAIN_ERROR(BUFMGR, status);
        }
    }
    
    return status;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  //put your code here

pinPage(PageId_in_a_DB,page,emptyPage);

  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  
    unsigned result = 0;
    for(unsigned i =0; i< bufDescr.size(); i++){
        if(bufDescr[i].pin_count == 0){
            result++;
        }
    }
    
    return result;
}
