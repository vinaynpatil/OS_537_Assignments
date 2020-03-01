#include "rand.h"

unsigned int seed_value = 1;

int xv6_rand (void){
    unsigned int x = seed_value;
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
    seed_value = x;
	return x % XV6_RAND_MAX;
}

void xv6_srand (unsigned int seed){
    seed_value = seed;
}
