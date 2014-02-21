#include "TrecReader.h"
#include <unistd.h>
/*
inline FILE* safeFopen(const std::string& path) {
	FILE* res = fopen64(path.c_str(), "r");
	if(!res)
		CERR << "failed to open a file at:" << path  << EFATAL;
	return res;
}
*/

TrecReader::TrecReader(const std::string& info_path, const std::string& index_path, const std::string& lex_path, const std::string& doclen_path, int _docn){

  findex = fopen64(info_path.c_str(), "r"); //8_1.dat
  finf = fopen64(index_path.c_str(), "r"); //8_1.inf
  flex = fopen64(lex_path.c_str(), "r");   //word_file
  fdocl = fopen64(doclen_path.c_str(), "r"); //doc_len
  
  // first int contains the # of terms in the entire collection
  fread(&infn,sizeof(int),1,finf);
  cout<<"The number of Terms: "<<infn<<endl;

  //load all the terms and their correponding sizes into inf_buffer
  inf_buffer = new unsigned int[4*infn];
  fread(inf_buffer, sizeof(int), 4*infn, finf);

  //Do prefix sum 
  inf_prefix_sum = new unsigned int[infn+1];
  inf_prefix_sum[0] = 0;
  for(size_t i=0;i<4*infn;i+=4)
     inf_prefix_sum[i/4+1] = (2*inf_buffer[i+1]*sizeof(unsigned int))+inf_prefix_sum[i/4]; //inf_buffer[i+1] corresponds to listsize, multiply sizeof(int), which is 2*sizeof(size_t)

  docn = _docn; // CONSTS::MAXD Total # of documents in collection
  doclen = new unsigned int[docn + 256]; // padding 256
  hold_buffer = new unsigned int[docn * 2]; // hold inverted lists

  load_doclength();
  cout<<"doclength is loaded"<<endl;
  //some hash func that map wordid to the entry in array inf_buffer, we use only the beginning two entries for every four entries
  /*
  int wid = 3;
  assert(wid==inf_buffer[4*(wid-1)]);
  size_t listsize = inf_buffer[4*(wid-1)+1];
  cout<<"termid: "<<wid<<endl;
  cout<<"listsize: "<<listsize<<endl;
  */

}

RawIndexList TrecReader::getRawList(const std::string& term, size_t wid){
  BasicList blTerm(term,wid);
  RawIndexList riTerm (blTerm);
  loadRawListIn(riTerm); //now it has the docids and freqs
  //debug check what it prints and doublecheck, should be the same as in the unpadded postings file
  //COUT1<<term<<": unpadded length: "<<riTerm.lengthOfList<<Log::endl;
  //unsigned int unpadded_list_length = riTerm.lengthOfList;

  // add dummy entries at the end to next multiple of CHUNK_SIZE
    for (size_t i = 0; (i == 0) || ((riTerm.lengthOfList&(CONSTS::BS-1)) != 0); ++i)     {
      riTerm.doc_ids.push_back(CONSTS::MAXD + i);
      riTerm.freq_s.push_back(1);
      ++riTerm.lengthOfList;
    }

    riTerm.rankWithBM25(doclen); // basic version was this but changed with the following one so we compute the correct max score
    //riTerm.rankWithBM25(doclen, unpadded_list_length);
    // Note: the list is ranked and the scores are in riTerm.scores, maxScore for list is set as well
  //COUT1<<term<<":"<<riTerm.maxScoreOfList<<Log::endl;

  return riTerm;
}

void  TrecReader::loadRawListIn(RawIndexList& tList) {
//size_t TrecReader::getList(const char* term, int wid, std::vector<size_t>& doc, std::vector<size_t>& fre) {
  size_t wid = tList.termId;
  assert(wid == inf_buffer[ 4 * (wid-1) ]);
  size_t listsize = inf_buffer[4*(wid-1) + 1];
  fseek(findex,inf_prefix_sum[wid-1],SEEK_SET); //jump to the right position
  if( (listsize*2)!= fread( hold_buffer, sizeof(int), listsize*2, findex ))
        cout << "can not read " << endl; //<<listsize*2
  tList.doc_ids.reserve(listsize+CONSTS::BS);
  tList.freq_s.reserve(listsize+CONSTS::BS);

  for(int i = 0 ;i < listsize; i++){
    tList.doc_ids.push_back(hold_buffer[2*i] - 1);
    tList.freq_s.push_back(hold_buffer[2*i + 1]);
    // cout<<"doc_id: "<<int(hold_buffer[2*i] - 1)<<" freq: "<<int(hold_buffer[2*i + 1])<<endl;
    // sleep(1);
  }
  tList.lengthOfList = listsize;
}

void TrecReader::load_doclength(){
  if( fread(doclen,sizeof(int), docn, fdocl) != docn )
    cout << "can not read doclength" << endl;

  for(int i = 0; i< 256; i++)    //padding 256
    doclen[ i + docn ] = 0;
}

unsigned int* TrecReader::loaddoclen(const char* fname/*=CONSTS::doclenFileName.c_str()*/)  {
  int docn = CONSTS::MAXD;
  unsigned int* pages;
  pages = new unsigned int[ docn + 128];

  FILE *fdoclength = fopen(fname,"r");
  if( fdoclength == NULL)
    cout <<" doc length file is missing "<< endl;

  if( fread(pages,sizeof(int), docn, fdoclength) != docn )
    cout<<"wrong doc len "<< endl;

  fclose(fdoclength);

  for(int i =0;i<128 ; i++)
    pages[docn + i] = docn/2;

  return pages;
}

RawIndexList TrecReader::load_raw_list(const std::string& term, size_t wid) {
  // arguments: term, term_id
  BasicList basic_list_Term(term, wid);
  RawIndexList Raw_list(basic_list_Term);
  // Get docids, freqs for specific term
  loadRawListIn(Raw_list);
  // Rank all docids
  Raw_list.rankWithBM25(doclen);

  // set unpadded list length
  Raw_list.unpadded_list_length = Raw_list.lengthOfList;

  // add dummy entries at the end to next multiple of CHUNK_SIZE
  // Docids = MAXD + 1, freqs = 1, scores = 0.0f
    for (size_t i = 0; (i == 0) || ((Raw_list.lengthOfList&(CONSTS::BS-1)) != 0); ++i)     {
      Raw_list.doc_ids.push_back(CONSTS::MAXD + i);
      Raw_list.freq_s.push_back(1);
      Raw_list.scores.push_back(0.0f);
      ++Raw_list.lengthOfList;
    }

  return Raw_list;
}

TrecReader::~TrecReader(void)
{
	fclose(findex);
	fclose(finf);
	fclose(flex);
	fclose(fdocl);
  delete[] doclen;
  delete[] inf_buffer;
  delete[] hold_buffer;
  delete[] inf_prefix_sum;
}

