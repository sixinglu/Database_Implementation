///////////////////////////////////////////////////////////////////////////////
/////////////  The Header File for the Buffer Manager /////////////////////////
///////////////////////////////////////////////////////////////////////////////


#ifndef BUF_H
#define BUF_H

#include "db.h"
#include "page.h"
#include "new_error.h"
#include <vector>
#include <queue>
#include <math.h>

#define NUMBUF 20   
// Default number of frames, artifically small number for ease of debugging.

#define HTSIZE 7
// Hash Table size

#define HASH_a 1
#define HASH_b 1


using namespace std;

/*******************ALL BELOW are purely local to buffer Manager********/

// You could add more enums for internal errors in the buffer manager.
enum bufErrCodes  {HASHMEMORY, HASHDUPLICATEINSERT, HASHREMOVEERROR, HASHNOTFOUND, QMEMORYERROR, QEMPTY, INTERNALERROR, 
			BUFFERFULL, BUFMGRMEMORYERROR, BUFFERPAGENOTFOUND, BUFFERPAGENOTPINNED, BUFFERPAGEPINNED};

class descriptors{
public:
    PageId page_number;
    int pin_count;
    bool dirtybit;  // true is dirty
};

//
class Replacer;
//{
//
//  public:
//    virtual int pin( int frameNo );
//    virtual int unpin( int frameNo );
//    virtual int free( int frameNo );
//    virtual int pick_victim() = 0;     // Must pin the returned frame.
//    virtual const char *name() = 0;
//    virtual void info();
//
//    unsigned getNumUnpinnedBuffers();
//
//  protected:
//    Replacer();
//    virtual ~Replacer();
//
//    enum STATE {Available, Referenced, Pinned};
//
//    BufMgr *mgr;
//    friend class BufMgr;
//    virtual void setBufferManager( BufMgr *mgr );
//
//}; // may not be necessary as described below in the constructor

class BufMgr {

private: 
   unsigned int    numBuffers;
   // the followings private numbers are defined according to requirement on 2016-03-10
    
   vector<descriptors> bufDescr;  // each one in bufDescr maps to each element in bufPool
    
   // vector of <page number, frame number>
   vector< vector<pair<PageId,unsigned> > > directory;  // the hash table, the reason I do not use array and linked list is vector is easy to add and remove elments (no need to release also), plus I love pair
   
   // LRU list of <frame number>, peek top, pop using queue. remove the according one in MRU by earse
   vector< unsigned > LRUlist;  // loved pages
    
   // MRU list of <frame number>, peek top, pop using vector. remove the according one in LRU by earse
   vector< unsigned > MRUlist;  // hated pages
    
    // return hash index in directroy
    unsigned hash(PageId PID);
    
    // return frame number
    int SearchPage(PageId PID);
    
    // add a page into directory
    Status HashAdd(PageId PID, unsigned frameNUM);
    
    // delete a page in the directory
    Status HashDelete(PageId PID);
    
    // love/hate replacement policy. remember to flush if dirty
    // if return DONE, there is no availble frame
    Status findReplaceFrame(int &Love_Hate, unsigned &MRU_LRU_index, int &frameID);
    
   // fill in this area
public:

    //vector< vector<pair<PageId,unsigned> > > directory; // debug

    Page* bufPool; // The actual buffer pool       the index is fram number

    BufMgr (int numbuf, Replacer *replacer = 0); 
   	// Initializes a buffer manager managing "numbuf" buffers.
	// Disregard the "replacer" parameter for now. In the full 
  	// implementation of minibase, it is a pointer to an object
	// representing one of several buffer pool replacement schemes.

    ~BufMgr();           // Flush all valid dirty pages to disk

    Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage);
        // Check if this page is in buffer pool, otherwise
        // find a frame for this page, read in and pin it.
        // also write out the old page if it's dirty before reading
        // if emptyPage==TRUE, then actually no read is done to bring
        // the page

    Status unpinPage(PageId globalPageId_in_a_DB, int dirty, int hate);
        // hate should be TRUE if the page is hated and FALSE otherwise
        // if pincount>0, decrement it and if it becomes zero,
        // put it in a group of replacement candidates.
        // if pincount=0 before this call, return error.

    Status newPage(PageId& firstPageId, Page*& firstpage, int howmany=1); 
        // call DB object to allocate a run of new pages and 
        // find a frame in the buffer pool for the first page
        // and pin it. If buffer is full, ask DB to deallocate 
        // all these pages and return error

    Status freePage(PageId globalPageId); 
        // User should call this method if it needs to delete a page
        // this routine will call DB to deallocate the page 

    Status flushPage(PageId pageid);
        // Used to flush a particular page of the buffer pool to disk
        // Should call the write_page method of the DB class

    Status flushAllPages();
	// Flush all pages of the buffer pool to disk, as per flushPage.

    /*** Methods for compatibility with project 1 ***/
    Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename);
	// Should be equivalent to the above pinPage()
	// Necessary for backward compatibility with project 1

    Status unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename);
	// Should be equivalent to the above unpinPage()
	// Necessary for backward compatibility with project 1
    
    unsigned int getNumBuffers() const { return numBuffers; }
	// Get number of buffers

    unsigned int getNumUnpinnedBuffers();
	// Get number of unpinned buffers
};

#endif
