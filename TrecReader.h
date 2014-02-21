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
#include <string.h>
#include "ListIterator.h"

using namespace std;

class TrecReader{
public:
  TrecReader(){}
  TrecReader(const std::string& info_path, const std::string& index_path, const std::string& lex_path, const std::string& doclen_path, int docn);
  // Usage: Given the term and its term_id, return a RawIndexList structure (vector of scores, dids, freqs) - padded
  RawIndexList load_raw_list(const std::string& term, size_t wid);
  void loadRawListIn(RawIndexList& tList);
  void load_doclength();
  unsigned int* loaddoclen(const char* fname=CONSTS::doclenFileName.c_str());
  RawIndexList getRawList(const std::string& term, size_t wid);
  ~TrecReader(void);
  //void load_doclength();
private:
  FILE *findex; //8_1.dat
  FILE *finf;   //8_1.inf
  FILE *flex;   //word_file
  FILE *fdocl;  //doc_len
  int infn; //store the number of terms in the entire collection
  int docn; //store the number of docs in the entire collection, 25205179
  unsigned int* inf_buffer;  //store the four entries of every inverted lists, the first one is wordid, the second one is listsize, the other two, unknown, the info is from 8_1.inf
  unsigned int* hold_buffer; //store the inverted lists of a wordid, the format is wordid, freq, wordid, freq... In constructor, initialize to docn*2, safe size.
  unsigned int* inf_prefix_sum; //store the offset of each inverted list
  unsigned int* doclen;      //store the doclen of each doc

};
