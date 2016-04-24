
#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:

void testPrint(HeapFile* file1);
enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};
struct _rec {
	int	key;
	char	filler[4];
};
static error_string_table ErrTable( JOINS, ErrMsgs );

// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         s               // Status of constructor
){
	// fill in the body
    Status status = OK;
    HeapFile* file1 = new HeapFile(filename1,status);
    HeapFile* file2 = new HeapFile(filename2,status);
    
    char tmp_name1[] = "tmp1";
    char tmp_name2[] = "tmp2";
    Sort* s_tmp =    new Sort(filename1,tmp_name1, len_in1,in1,t1_str_sizes,join_col_in1,order,amt_of_mem, s );
    s_tmp =    new Sort(filename2,tmp_name2, len_in1,in1,t1_str_sizes,join_col_in1,order,amt_of_mem, s );
    
    HeapFile* sorted1 = new HeapFile(tmp_name1,status);
 //   testPrint(sorted1);
    HeapFile* sorted2 = new HeapFile(tmp_name2,status);
  //  testPrint(sorted2);
    HeapFile* output = new HeapFile(filename3,status);
    
    Scan*	f1_scan = sorted1->openScan(status);
    Scan*   f2_scan = sorted2->openScan(status);
    RID	rid1,rid2;
    int len1,len2;
    char rec1[sizeof(struct _rec)], rec2[sizeof(struct _rec)], rec3[2 * sizeof(struct _rec)];
    Status f1_hasnext = f1_scan->getNext(rid1, rec1, len1);
    Status f2_hasnext = f2_scan->getNext(rid2, rec2, len2);

    while(f1_hasnext == OK && f2_hasnext == OK){
        // the smaller one move on
        int result = tupleCmp(rec1, rec2);
        while(result > 0 && f2_hasnext == OK){
            f2_hasnext = f2_scan->getNext(rid2, rec2, len2);
            result = tupleCmp(rec1, rec2);
            //append f2
        }
        while(result < 0 && f1_hasnext == OK){
            //append f1
            f1_hasnext = f1_scan->getNext(rid1, rec1, len1);
            result = tupleCmp(rec1, rec2);
        }
        //nested loop stage
        RID back2 = rid2;
        while(result == 0 && f1_hasnext == OK){
            // f1 step further one
            rid2 = back2;
            
            while(result == 0 && f2_hasnext == OK){
                //f2 step one
                RID uselessRID;
                memcpy(rec3,rec1,len1);
                memcpy(&rec3[len1],rec2,len2);
                output->insertRecord(rec3, len1+len2, uselessRID);
                f2_hasnext = f2_scan->getNext(rid2, rec2, len2);
                //append f2
                result = tupleCmp(rec1, rec2);
            }
            f2_scan->position(back2);
            f2_hasnext = f2_scan->getNext(rid2, rec2, len2);
            f1_hasnext = f1_scan->getNext(rid1, rec1, len1);
            result = tupleCmp(rec1, rec2);
        }
    }
 
    sorted1->deleteFile();
    sorted2->deleteFile();
    s = OK;
    delete file1;
    delete file2;
    delete s_tmp;
}

// sortMerge destructor
sortMerge::~sortMerge()
{
	// fill in the body
}
void testPrint(HeapFile* file1){
	Status s;
        Scan*	scan = file1->openScan(s);
	assert(s == OK);
	int len;
	RID	rid;
	char	rec[sizeof(struct _rec)*2];
	cout << endl;
	for (s = scan->getNext(rid, rec, len); s == OK; s = scan->getNext(rid, rec, len)) 
	{
	  cout << (*((struct _rec*)&rec)).key << " ";
	}

	delete scan;
}
