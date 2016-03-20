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

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
    
    this->numBuffers = numbuf;
    this->bufPool = new Page[numBuffers];
    this->replacer_holder = replacer;
   //vector<int> bufDescr2(4,100);
    for(int i =0; i<numbuf; i++){
        descriptors Pagedescr;
        Pagedescr.page_number = INVALID_PAGE;
        Pagedescr.dirtybit = false;
        Pagedescr.pin_count = 0;
	//cout<<bufDescr.size()<<endl;
	//bufDescr2.push_back(1);
        bufDescr.push_back(Pagedescr);
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
    cout<<index<<' '<<directory.capacity()<<endl;
    
    for(unsigned i =0; i<directory.at(index).size(); i++){
        if(directory.at(index).at(i).first==PID){
            return directory.at(index).at(i).second;
        }
    }
    
    if(frameNUM==-1){
        cout<<bufErrMsgs[3]<<endl;
    }
    
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
    return status;
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
            directory.at(index).erase(directory.at(index).begin()+i);
            break;
        }
    }
    
    if(i>=directory.at(index).size()){ // if not exist
        cout<<bufErrMsgs[2]<<endl;
        return DONE;
    }
    
    return status;
}




//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  // put your code here
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
printf("pinnnnnnnnnnn\n");
	Status status;
	int frame = SearchPage(PageId_in_a_DB);
	if( frame != -1){	
		printf("already in\n");
		bufDescr[frame].pin_count ++;
		//inform replacer of this pin
		replacer_holder->pin(frame);
		//actual return page
		page = &bufPool[frame];
	}
	else {
		int victim = replacer_holder->pick_victim();
		if(victim == -1)
			return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERFULL );
		if(bufDescr[victim].page_number != INVALID_PAGE){
			//update directory
			HashDelete(bufDescr[victim].page_number);
		}
		if(bufDescr[victim].dirtybit == 1){
			//write back
			status = MINIBASE_DB->write_page(bufDescr[victim].page_number, &bufPool[victim]);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
		}
		//update directory
		HashAdd(PageId_in_a_DB,victim);
		//pin the new page first before reading it in
		bufDescr[victim].page_number = PageId_in_a_DB;
		bufDescr[victim].pin_count = 1;
		bufDescr[victim].dirtybit = 0;
		//inform replacer of this pin
		replacer_holder->pin(victim);
		if(emptyPage == 0){
			//read in something
			status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[victim]);	
			if(status != OK)
			{//undo if error occurs
				bufDescr[victim].page_number = INVALID_PAGE;
				bufDescr[victim].dirtybit = 0;
				bufDescr[victim].pin_count--;
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
			}
		}
		//actual return page
		page = &bufPool[victim];
	}
  return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  // put your code here
  return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  //put your code here
  return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  //put your code here
printf("here i am1\n");
pinPage(PageId_in_a_DB,page,emptyPage);
printf("here i am2\n");
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
  //put your code here
  return 0;
}
