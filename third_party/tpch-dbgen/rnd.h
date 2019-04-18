/*
* $Id: rnd.h,v 1.4 2006/08/01 04:13:17 jms Exp $
*
* Revision History
* ===================
* $Log: rnd.h,v $
* Revision 1.4  2006/08/01 04:13:17  jms
* fix parallel generation
*
* Revision 1.3  2006/07/31 17:23:09  jms
* fix to parallelism problem
*
* Revision 1.2  2005/01/03 20:08:59  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:47  jms
* re-establish external server
*
* Revision 1.1.1.1  2003/08/08 21:50:34  jms
* recreation after CVS crash
*
* Revision 1.3  2003/08/08 21:35:26  jms
* first integration of rng64 for o_custkey and l_partkey
*
* Revision 1.2  2003/08/07 17:58:34  jms
* Convery RNG to 64bit space as preparation for new large scale RNG
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
/*
 * rnd.h -- header file for use withthe portable random number generator
 * provided by Frank Stephens of Unisys
 */

/* function protypes */
DSS_HUGE            NextRand    PROTO((DSS_HUGE));
DSS_HUGE            UnifInt     PROTO((DSS_HUGE, DSS_HUGE, long));

// HYRISE: Commented out because they'd be global - and never used
//static long     nA = 16807;     /* the multiplier */
//static long     nM = 2147483647;/* the modulus == 2^31 - 1 */
//static long     nQ = 127773;    /* the quotient nM / nA */
//static long     nR = 2836;      /* the remainder nM % nA */

extern double dM;

/*
 * macros to control RNG and assure reproducible multi-stream
 * runs without the need for seed files. Keep track of invocations of RNG
 * and always round-up to a known per-row boundary.
 */
/*
 * preferred solution, but not initializing correctly
 */
#define VSTR_MAX(len)	(long)(len / 5 + (len % 5 == 0)?0:1 + 1)
extern seed_t     Seed[MAX_STREAM + 1];