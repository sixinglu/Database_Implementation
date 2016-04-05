/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>
#include <cstring>

#include "bt.h"

using namespace std;
/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
  
    // AttrType only be  attrString or attrInteger
    if(t==attrString){
        return strcmp( ((Keytype*)key1)->charkey,((Keytype*)key2)->charkey );
    }
    else if(t==attrInteger){
        return ((Keytype*)key1)->intkey - ((Keytype*)key2)->intkey;
    }
    else{
        return FAIL;
    }
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
    // get key size
    int keylenth = get_key_length(key, key_type);
    
    // get data size
    *pentry_len = get_key_data_length(key, key_type, ndtype);
    
    // memcpy
    memcpy(&target->key, key, keylenth);
    memcpy(&target->data, &data, *pentry_len-keylenth);
  
    return;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
    if(psource==NULL){
        return;
    }
    
    // if this node is index node
    if(ndtype==INDEX){
        memcpy(targetkey, &psource->key, entry_len - sizeof(PageId));
        memcpy(targetdata, &psource->data, sizeof(PageId));
    }
    else if(ndtype==LEAF){  // if this node is leaf node
        memcpy(targetkey, &psource->key, entry_len - sizeof(RID));
        memcpy(targetdata, &psource->data, sizeof(RID));
    }
    
    return;
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
    int keylenth = 0;
    
    // calculate key size
    if(key_type==attrString){
        keylenth = sizeof( ((Keytype*)key)->charkey );
    }
    else if(key_type==attrInteger){
        keylenth = sizeof( ((Keytype*)key)->intkey );
    }
    
    return keylenth;
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
    int datalenth = 0;
    int keylenth = 0;
    
    // get key size
    keylenth = get_key_length(key, key_type);
    
    // calculate data size
    if(ndtype==INDEX){
        datalenth = sizeof(PageId);
    }
    else if(ndtype==LEAF){
        datalenth = sizeof(RID);
    }
    
    return keylenth + datalenth;
}
