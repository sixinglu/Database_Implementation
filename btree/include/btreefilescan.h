/*
 * btreefilescan.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 *
 */
 
#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H

#include "btfile.h"

// errors from this class should be defined in btfile.h

class BTreeFileScan : public IndexFileScan {
public:
    friend class BTreeFile;

    // get the next record
    Status get_next(RID & rid, void* keyptr);

    // delete the record currently scanned
    Status delete_current();

    int keysize(); // size of the key

    // destructor
    ~BTreeFileScan();
private:
    
    PageId leftmostPage;
    PageId rightmostPage;
    
    RID leftmostRID;
    RID rightmostRID;
    
    PageId currentPage;
    RID currRID;

// for delete current
    PageId prevPage;
    RID prevRID; 
    PageId temp_leftmostPage;
    RID temp_leftmostRID;
    
    int Keysize;

    bool getFirst;
    bool delFirst;

bool usedUp;
};

#endif
