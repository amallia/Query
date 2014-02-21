#ifndef EXHAUSTIVEOR_H_
#define EXHAUSTIVEOR_H_

#include "PostingOriented_BMW.h"

class ExhaustiveOR{
private:
	unsigned int* pages;  //doc length

public:
	ExhaustiveOR(unsigned int* pgs) : pages(pgs) {}
	void operator()(lptrArray& lps, const int topk, QpResult* r);
};

#endif /* EXHAUSTIVEOR_H_ */
