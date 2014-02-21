#ifdef __APPLE__
#  define off64_t off_t
#  define fopen64 fopen
#endif

#include <iostream>
#include <stdio.h>
#include <exception>
#include <stdlib.h>
#include <vector>
#include <assert.h>
#include "TrecReader.h"
#include "exhaustiveOR.h"
#include "DocidOriented_BMW.h"

using namespace std;

int main ()
{
  const string finf = "/Users/Adam57/Documents/Trec_data/Trec06_RawIndx/8_1.inf";
  const string findex = "/Users/Adam57/Documents/Trec_data/Trec06_RawIndx/8_1.dat";
  const string flex = "/Users/Adam57/Documents/Trec_data/Trec06_RawIndx/word_file";
  const string fdoclength = "/Users/Adam57/Documents/Trec_data/Trec06_RawIndx/doclen_file";
  const string rootpath = "/Users/Adam57/Documents/1_27_2014/Qual/data/";

TrecReader Reader(findex,finf,flex,fdoclength,CONSTS::MAXD);
unsigned int* pages;
pages = Reader.loaddoclen();

// ExhaustiveOR Exhaustive(pages);
// PostingOriented_BMW PO_BMW(pages);
DocidOriented_BMW DO_BMW(pages);

std::vector<QpResult> results;
results.reserve(10);
// BasicList daily_B("daily",3);
// BasicList planet_B("planet",1);

// RawIndexList daily_R(daily_B);
// RawIndexList planet_R(daily_B);

// Reader.loadRawListIn(daily_R);
// Reader.loadRawListIn(planet_R);

RawIndexList first_R = Reader.load_raw_list("daily",3);
RawIndexList second_R = Reader.load_raw_list("planet",1);
RawIndexList third_R = Reader.load_raw_list("kwhr",23726);
// RawIndexList first_R = Reader.load_raw_list("sbrefa",23754);
// RawIndexList second_R = Reader.load_raw_list("kwhr",23726);
CompressedList first_C(first_R);  
CompressedList second_C(second_R); 
CompressedList third_C(third_R); 
// first_C.serializeToFS(rootpath);
// second_C.serializeToFS(rootpath);
lptr first_lptr(first_C);
lptr second_lptr(second_C);
lptr third_lptr(third_C);
lptrArray lps;
lps.push_back(&first_lptr);
lps.push_back(&second_lptr);
lps.push_back(&third_lptr);


/*Docid Oriented preparation starts*/
for (int i=0; i<lps.size(); i++){
  RawIndexList Raw_List = lps_to_RawIndexList(lps[i], pages);
  injectBlocker(lps[i]->gen, Raw_List);
}

for (int i=0; i<lps.size(); i++) {
      int bits;
      bitOracle(lps[i]->unpadded_list_length, bits); // obtain the number of bits we need for the docID oriented block size based on the list length - Variable block selection schema
      int num_blocks = (CONSTS::MAXD>>bits) + 1;
      if (lps[i]->unpadded_list_length < (1<<15)) {  // all lists with length less than 32768 are computed on the fly and we measure the time
        std::vector<float> max_array (num_blocks, 0.0);
        on_the_fly_max_array_generation(lps[i], max_array, bits, pages); //repeating works with gen.generatemax function in injectBlocker only for lists less than 32768 (since here we need to record the time for on the fly generation) 
      }
}
/*Docid Oriented preparation ends*/


clock_t init, final;
init=clock();

/*Docid Oriented BMW output*/
PriorityArray<QpResult> resultsHeap = DO_BMW(lps,10);
resultsHeap.sortData();
  cout<<"Top 10: "<<endl;
  for(int i=0;i<10;i++)
    cout<<"Did: "<<resultsHeap.getV()[i].did<<" Score: "<<resultsHeap.getV()[i].score<<endl;
/*Docid Oriented BMW output*/

// Exhaustive(lps, 10, &results[0]);
// PO_BMW(lps,10,&results[0]);

final=clock()-init;
cout <<"Time used: "<<(double)final / ((double)CLOCKS_PER_SEC)<<endl;
cout<<"Top 10: "<<endl;
for(int i=0;i<10;i++)
    cout<<"Did: "<<results[i].did<<" Score: "<<results[i].score<<endl;

return 0;
}
