#ifndef _MYFILE_H_
#define _MYFILE_H_

#include "SqlProxy.h"
#include <vector>
#include <algorithm>
#include <iterator>
#include "pfor.h"
#include "BlockGens.h"
#include "profiling.h"

class onDemandCpool {
	// vecUInt data;
	std::string term;

	static size_t capacity;
	static size_t counter;
	static std::vector<onDemandCpool*> storage;
	static termsMap termsToInd;
	inline void setData(unsigned int* d, size_t sz) {
		data.clear();
		data.reserve(sz);
		for(size_t i=0; i<sz; ++i)
			data.push_back(d[i]);
	}

public:
	vecUInt data;
	onDemandCpool()  {}
	~onDemandCpool() {evictData(); }

	void evictData();
	static void initPool(size_t cap);
	size_t errorCheck();
	inline void setName(const std::string& off) { term = off; }
	inline const std::string& getName() { return term ;}
	//the data must be materialized before using this operator!
	inline unsigned int& operator[](size_t i) { return data[i]; }
	inline size_t getSize() { return data.size(); }
	unsigned int materialize( const std::vector<std::string>& peers);
};


class BasicList {
public:
	std::string term;
	size_t termId;
	unsigned int lengthOfList;				// the padded list length
	unsigned int unpadded_list_length;		// needed only for layering (unpadded list length)
	unsigned int original_unpadded_list_length; // for layering, useless but we need to remove it from prepare_list - trecreader
	float maxScoreOfList; //optional
	BasicList(const std::string& t="", size_t id=0) : term(t), termId(id) {}
};

class RawIndexList : public BasicList{
public:
	vecUInt doc_ids;
	vecUInt freq_s;
	std::vector<float> scores; //optional
	explicit RawIndexList(const BasicList& rhs=BasicList()) :BasicList(rhs) { }
	void rankWithBM25(unsigned int* doclen); //will fill the scores
	void rankWithBM25(unsigned int* doclen, unsigned int& unpadded_list_length); // compute the correct max score of list
};

class CompressedList : public BasicList {
public:
	size_t BS;
	vecUInt maxDidPerBlock;		// the max value for each chunk
	std::vector<float> DEPmaxScorePerBlock;
	vecUInt sizePerBlock;		// the size for each chunk
	vecUInt flag;			// the flag for each chunk
	vecUInt compressedVector;	// the compressed data
	onDemandCpool cpool;
	size_t cpool_leng;	// the length of cpool
	size_t info_leng;		// the length of info

	CompressedList() :BS(0) {}
	CompressedList(const RawIndexList& rl, const size_t bs=CONSTS::BS);
	void compressDocsAndFreqs(const RawIndexList& rl);
	void serializeToFS(const std::string& rootPath);
	// void serializeToDb(const std::string& rootPath, SqlProxy&);
};

class BaseLptr : public CompressedList{
public:
	int checkblock;
	int currMax;
	unsigned int *off;		// the running pointer
	unsigned int *decmpPtr;		// another running pointer
	unsigned int block; 	// the current block number
	unsigned int elem;
	int did;
	int dflag;
	int fflag;
	unsigned int dbuf[CONSTS::BS];		// buffer for storing the decoded docid
	unsigned int fbuf[CONSTS::BS];		// buffer for storing the decoded freq
	float pre;

  	// Quantization
	float quantile;

///*
	// Layering
	bool is_essential;		  			// if term is essential
	bool has_layers;		  			// if term has layers true
	unsigned int list_id;				// if term has layers, then list_id
	int layer_index;					// their layered id, otherwise
	int layer_status;
	//int metainfo;						// metainfo needed during early termination

	inline void reset() { block = 0; dflag = 1; fflag = 0; elem = 0; checkblock = 0;}

	inline BaseLptr() { reset(); }
	void open(const std::string term, const unsigned int lengthOfLst);

	// Usage: reset all pointers so that we can re-use list
	inline void reset_list() {
		block = 0;
		dflag = 1;
		fflag = 0;
		elem = 0;
		checkblock = 0;
		// off = &(cpool[0]);
		off = &(compressedVector[0]);
		currMax = maxDidPerBlock[0];
		// cout<<"huh?"<<endl;
		decmpPtr = pack_decode(dbuf, off, flag[block]&65535, 0); //flag[block]&65535 corresponds to doc postion in compressedvector 
		// decmpPtr = pack_decode(dbuf, off, flag[block]&65535);
		did = dbuf[0];
	}
};

// structure for a pointer within an inverted list
class lptr : public BaseLptr {
public:
	class comparator {
	public:
		inline bool operator()(lptr* a, lptr* b) { return a->did<b->did;} //to sort by list length -- aka comparelps
	};

	class scorecomparator { // ADDON
		public:
			inline bool operator()(lptr* a, lptr* b) { return a->maxScoreOfList<b->maxScoreOfList;} // sort by score (function for sort())
		};

	#define CALC_SCORE(f,did) pre * (float)(f) / ((float)(f + 2 * ( 0.25 + 0.75 * (float)(did)/(float)(130) ) ) )
	SingleHashBlocker gen;
	unsigned int maskOn;
	unsigned int maskOff;
	unsigned int indexInSorted;
	lptr() {}
	lptr(CompressedList& cl);

	inline float calcScore(float f, int did) { return CALC_SCORE(f,did);}

	inline float DEPgetMaxScoreOfDeepBlock() { return DEPmaxScorePerBlock[block]; }

	// used for the MaxscoreBlocks
	inline float getMaxScoreOfBlock() {	return DEPmaxScorePerBlock[checkblock]; }

	//move the pointer to the right block
	inline int DEPshallowMove(int did) MYNOINLINE {
		while (did > (int)maxDidPerBlock[checkblock]) 		//++total_move;
			++checkblock;
		return maxDidPerBlock[checkblock] + 1 ;
	}

	// get the frequency of the current posting
	inline int getFreq() MYNOINLINE	{
		if (fflag < 1)  {
			decmpPtr = pack_decode(fbuf,decmpPtr, flag[block]>>16); //flag[block]>>16 corresponds to freq postion in compressedvector
			fflag = 1;
		}
		return(fbuf[elem]+1);
	}

	// skip blocks until you come to the right one
	inline void skipToDidBlockAndDecode(int d) MYNOINLINE	{
		//profiler::getInstance().stepCounter(CONSTS::SKIPS);
		off += (sizePerBlock[block++]+1);
		while (d > (int)maxDidPerBlock[block]) {
			off += (sizePerBlock[block++]+1);
			//profiler::getInstance().stepCounter(CONSTS::SKIPSB);
		}
		currMax=maxDidPerBlock[block]; //we keep track of current max to avoid max[block] in nextGEQ -- it is not cache friendly...
		decmpPtr = pack_decode(dbuf, off, flag[block]&65535, maxDidPerBlock[block-1]); //decode new block, (modify version pford decode)adding the last docid in last block(which is also the max docid in last block) to the first docid in this block
		elem = 0; //point to first element in that block
		did = dbuf[0]; //get the first did IN THE DECODED block
		dflag = 1;
		fflag = 0;

	}

	// move list pointer to first posting with docID >= d */ //
	inline unsigned int nextGEQ(int d) MYNOINLINE	{
		//PROFILER(CONSTS::NEXTGEQ);
		if (d > currMax) // check if we need to skip to a new block of size BS
			skipToDidBlockAndDecode(d);
		for(elem=elem+1; d>did; ++elem) //try to go to the right element
			did +=dbuf[elem]; //prefix summing
		--elem;

		return did;
	}
};

class lptrArray : public std::vector<lptr*> {
public:
	lptrArray() {}
	inline void sort() {
		std::sort(begin(),end(),lptr::comparator()); //TODO: speedup
		for(unsigned int i=0; i<size(); ++i){
			// cout<<"Sorting: "<<i<<endl;
			this->operator[](i)->indexInSorted = i;
		}
	}

	inline void sort(const int start, const int end) { // TODO handle error if put pivot out of range
		std::sort(begin() + start, begin() + end + 1, lptr::comparator());
		for (unsigned int i=start; i<end+1; ++i)
			this->operator[](i)->indexInSorted = i;
	}

	// ADDON
	inline void sortbyscore() {
		std::sort(begin(),end(),lptr::scorecomparator());
		for(unsigned int i=0; i<size(); ++i)
			this->operator[](i)->indexInSorted = i;
	}

	inline void sortbyscore(const int pivot) { // TODO handle error if put pivot out of range
		std::sort(begin(),begin() + pivot + 1,lptr::scorecomparator());
		for(unsigned int i=0; i<pivot + 1; ++i)
			this->operator[](i)->indexInSorted = i;
	}

	inline float getListMaxScore(size_t i) { return this->operator [](i)->maxScoreOfList; }

	inline void popdown(int i) 	{
		lptr *temp = this->operator [](i);
		int j = i + 1;
		while( j < this->size() && temp->did >= ( (this->operator [](j))->did) )	{
			this->operator [](j-1) = this->operator [](j); //swap
			this->operator [](j)->indexInSorted = j-1;
			++j;
		}
		this->operator [](j - 1) = temp;
		this->operator [](j-1)->indexInSorted = j-1;
	}
};

template<typename T>
void injectBlocker(T& blocker, RawIndexList& rilist ) {
	T gen(rilist.doc_ids); // creating a singlehashblocker object, initialized with rilist's doc_ids
	gen.generateBlocks(); //actually did nothing here, empty func.
	gen.generateMaxes(rilist.scores);
	blocker = gen; // singlehashblocker within class lptr is initialized here
}


inline RawIndexList lps_to_RawIndexList(lptr*& lps, unsigned int* pages) {
  BasicList B_Term(lps->term, lps->termId);
  RawIndexList Raw_list(B_Term);
  int total_blocks = (lps->lengthOfList/CONSTS::BS); // lengtoflist is the padded one, so if we divide by bs the result is always an integer value

  //debug
  //std::cout << lps->maxScoreOfList <<" " << lps->did << " \t freq: " << lps->getFreq() << "\tscore: " << lps->calcScore(lps->getFreq(), pages[lps->did]) <<  " and element: " << lps->elem << " and curMax " << lps->currMax << std::endl;

  // for all docids of lps (lengthofList has plus the padded ones, but it's ok)
  // increase by one so that in the next round, the method skipToDidBlockAndDecode to decode the next block
  for (int blocks=0; blocks<total_blocks; blocks++) {
    // for all blocks except the first one, decode block
    if ( blocks != 0) {
      // decode block
      lps->skipToDidBlockAndDecode(lps->currMax+1);
    }

    // cout<<"size: "<<Raw_list.doc_ids.size()<<endl;
    // cout<<"size: "<<Raw_list.freq_s.size()<<endl;
    // cout<<"size: "<<Raw_list.scores.size()<<endl;

    // int test;
    // cin >> test;

    Raw_list.doc_ids.push_back(lps->did);
    Raw_list.freq_s.push_back(lps->getFreq());
    Raw_list.scores.push_back(lps->calcScore(lps->getFreq(), pages[lps->did])); //the scores stores every docid's corrsponding score

    // for all dids of block, compute did (prefix sum), and push to vector
    for (lps->elem+=1; lps->did<lps->currMax; lps->elem++) {
      // cout<<"elem#: "<<lps->elem<<endl;
      lps->did +=lps->dbuf[lps->elem];
      // fill vectors
      Raw_list.doc_ids.push_back(lps->did);
      // cout<<"docid: "<<lps->did<<endl;
      int freq = lps->getFreq();
      Raw_list.freq_s.push_back(freq);
      // cout<<"freq: "<<freq<<endl;
      Raw_list.scores.push_back(lps->calcScore(freq, pages[lps->did]));
    }
  }
  
  // optional: fix padded scores to 0.0f
  // for (int i=lps->unpadded_list_length; i<lps->lengthOfList; i++)
  //   Raw_list.scores.at(i) = (0.0f);
  // reset pointers, so that we can use the lptr structure
  lps->reset_list();
  return Raw_list;
}

/* Generate a block max array given a lptr
   Input: lptr of a specific term and max array to store the block maxes
   Output: vector of block maxes
   Note: Assuming we initialized pages[] that that we have the document length of all documents */
inline void on_the_fly_max_array_generation(lptr*& lps, std::vector<float>& max_array, int& bits, unsigned int* pages) {
	int total_blocks = (lps->lengthOfList/CONSTS::BS); // max blocks based on pfd compression
	int cur_block_number = 0; // hash block number
	float score = 0.0f;

	// for all docids of lps less than CONSTS::MAXD, so as not to count dids out of range and junk scores
	// increase by one so that in the next round, the method skipToDidBlockAndDecode to decode the next block
	for (int blocks=0; blocks<total_blocks; blocks++) {
		// for all blocks except the first one, decode block
		if ( blocks != 0) {
			// decode block
			lps->skipToDidBlockAndDecode(lps->currMax+1);
		}

		// if docid in docid range
		if (lps->did < CONSTS::MAXD) {
			// get block number
			cur_block_number = lps->did >> bits;
			// compute score
			score = lps->calcScore(lps->getFreq(), pages[lps->did]);
			// check if we need to update current value of block
			if ( Fcompare(max_array[cur_block_number], score) == -1 )
				max_array[cur_block_number] = score;
		} else
			break;

		// for all dids of block, compute did (prefix sum), and push to vector
		for (lps->elem+=1; lps->did<lps->currMax; lps->elem++) {
			lps->did +=lps->dbuf[lps->elem];

			// if docid in docid range
			if (lps->did < CONSTS::MAXD) {
				// get block number
				cur_block_number = lps->did >> bits;
				// compute score
				score = lps->calcScore(lps->getFreq(), pages[lps->did]);
				// check if we need to update current value of block
				if ( Fcompare(max_array[cur_block_number], score) == -1 )
					max_array[cur_block_number] = score;
			} else
				break;
		}
	}

	// reset pointers, so that we can use the lptr structure again in query processing (assuming that was a preprocessing step)
	lps->reset_list();
}

/* Usage: Given the list length output the block bits used for DocID-Oriented Block-Max Indexes */
inline void bitOracle(unsigned int& length, int& lenBits) {
	lenBits = intlog2(length);
	std::vector<int> Bucket (18, 0);
	Bucket.at(0) = 10; // 2^7
	Bucket.at(1) = 10; // 2^8
	Bucket.at(2) = 10; // 2^9
	Bucket.at(3) = 10; // 2^10
	Bucket.at(4) = 6; // 2^11 --
	Bucket.at(5) = 6; // 2^12 --
	Bucket.at(6) = 7; // 2^13 -
	Bucket.at(7) = 7; // 2^14 --
	Bucket.at(8) = 7; // 2^15 --
	Bucket.at(9) = 8; // 2^16 -
	Bucket.at(10) = 8; // 2^17 -
	Bucket.at(11) = 7; // 2^18
	Bucket.at(12) = 6; // 2^19 --
	Bucket.at(13) = 6; // 2^20 --
	Bucket.at(14) = 6; // 2^21 --
	Bucket.at(15) = 6; // 2^22 --
	Bucket.at(16) = 6; // 2^23 --
	Bucket.at(17) = 6; // 2^24 --

	// pick the right bucket
	int B_counter = 0;
	for (int i=7; i<25; i++) {
		if (lenBits<i) {
			lenBits = Bucket.at(B_counter);
			break;
		} else
			++B_counter;
	}
}

#endif