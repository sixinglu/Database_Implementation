/*
 * btfile.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
 
#ifndef _BTREE_H
#define _BTREE_H

#include "btindex_page.h"
#include "btleaf_page.h"
#include "index.h"
#include "btreefilescan.h"
#include "bt.h"

// Define your error code for B+ tree here
// enum btErrCodes  {...}

// header page do not contain key or RID informaion, only used for grad the first index or leaf
class HeaderPage{
public:
    //PageId headerpageID;
    PageId rootPageID;      // compare the rootPageId to tell if the page is root
    AttrType keytype;
    nodetype PageType;
    int keysize;
};

class BTreeFile: public IndexFile
{
  public:
    
    BTreeFile(Status& status, const char *filename);
    // an index with given filename should already exist,
    // this opens it.
    
    BTreeFile(Status& status, const char *filename, const AttrType keytype, const int keysize);
    // if index exists, open it; else create it.
    
    ~BTreeFile();
    // closes index
    
    Status destroyFile();
    // destroy entire index file, including the header page and the file entry
    
    Status insert(const void *key, const RID rid);
    // insert <key,rid> into appropriate leaf page
    
    Status Delete(const void *key, const RID rid);
    // delete leaf entry <key,rid> from the appropriate leaf
    // you need not implement merging of pages when occupancy
    // falls below the minimum threshold (unless you want extra credit!)
    
    IndexFileScan *new_scan(const void *lo_key = NULL, const void *hi_key = NULL);
    // create a scan with given keys
    // Cases:
    //      (1) lo_key = NULL, hi_key = NULL
    //              scan the whole index
    //      (2) lo_key = NULL, hi_key!= NULL
    //              range scan from min to the hi_key
    //      (3) lo_key!= NULL, hi_key = NULL
    //              range scan from the lo_key to max
    //      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
    //              exact match ( might not unique)
    //      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
    //              range scan from lo_key to hi_key

    int keysize();
    
  private:
    
    HeaderPage headerpage;
    
    // recursively search, return leaf PageID
    // called in Insert_helper()
    Status Search_index(PageId& result, PageId currPage, const void *key);
    
    // recursively search the parent of the split target, child could be index/leaf
    // called in Insert_helper()
    Status Search_parent(const PageId child, PageId currPage, const void *childkey, PageId& parent);
    
    // almost the same with get_page_no, but we also need to know parent RID when search child
    // not needed anymore, move former part into new node, need no update
    //Status get_parent_child(SortedPage* currPage, const void *key, AttrType key_type, PageId & pageNo, RID &parentRid);
    
    // split the page into two page, copyup (leaf), pushup(index) key
    Status Split(PageId splitTarget, PageId &rightchild, void *upkey);
    
    // may recursively insert, split, search_index
    Status Insert_helper(PageId insertLoc, const void *key, Datatype datatype, nodetype createdtype);
    
};

#endif
