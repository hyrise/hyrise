/*
* $Id: driver.c,v 1.7 2008/09/24 22:35:21 jms Exp $
*
* Revision History
* ===================
* $Log: driver.c,v $
* Revision 1.7  2008/09/24 22:35:21  jms
* remove build number header
*
* Revision 1.6  2008/09/24 22:30:29  jms
* remove build number from default header
*
* Revision 1.5  2008/03/21 17:38:39  jms
* changes for 2.6.3
*
* Revision 1.4  2006/04/26 23:01:10  jms
* address update generation problems
*
* Revision 1.3  2005/10/28 02:54:35  jms
* add release.h changes
*
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:46  jms
* re-establish external server
*
* Revision 1.5  2004/04/07 20:17:29  jms
* bug #58 (join fails between order/lineitem)
*
* Revision 1.4  2004/02/18 16:26:49  jms
* 32/64 bit changes for overflow handling needed additional changes when ported back to windows
*
* Revision 1.3  2004/01/22 05:49:29  jms
* AIX porting (AIX 5.1)
*
* Revision 1.2  2004/01/22 03:54:12  jms
* 64 bit support changes for customer address
*
* Revision 1.1.1.1  2003/08/08 21:50:33  jms
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
/* main driver for dss banchmark */

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#include "config.h"
#include "release.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))
#include <process.h>
#pragma warning(disable:4201)
#pragma warning(disable:4214)
#pragma warning(disable:4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#endif

#include "dss.h"
#include "dsstypes.h"
#include "tpch_dbgen.h"


extern int optind, opterr;
extern char *optarg;
DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif
#ifdef RNG_TEST
extern seed_t Seed[];
#endif
static int bTableSet = 0;


/*
* general table descriptions. See dss.h for details on structure
* NOTE: tables with no scaling info are scaled according to
* another table
*
*
* the following is based on the tdef structure defined in dss.h as:
* typedef struct
* {
* char     *name;            -- name of the table; 
*                               flat file output in <name>.tbl
* long      base;            -- base scale rowcount of table; 
*                               0 if derived
* int       (*loader) ();    -- function to present output
* long      (*gen_seed) ();  -- functions to seed the RNG
* int       child;           -- non-zero if there is an associated detail table
* unsigned long vtotal;      -- "checksum" total 
* }         tdef;
*
*/

/*
* seed generation functions; used with '-O s' option
*/
long sd_cust (int child, DSS_HUGE skip_count);
long sd_line (int child, DSS_HUGE skip_count);
long sd_order (int child, DSS_HUGE skip_count);
long sd_part (int child, DSS_HUGE skip_count);
long sd_psupp (int child, DSS_HUGE skip_count);
long sd_supp (int child, DSS_HUGE skip_count);
long sd_order_line (int child, DSS_HUGE skip_count);
long sd_part_psupp (int child, DSS_HUGE skip_count);

tdef tdefs[] =
{
	{"part.tbl", "part table", 200000,
		NULL, sd_part, PSUPP, 0},
	{"partsupp.tbl", "partsupplier table", 200000,
		NULL, sd_psupp, NONE, 0},
	{"supplier.tbl", "suppliers table", 10000,
		NULL, sd_supp, NONE, 0},
	{"customer.tbl", "customers table", 150000,
		NULL, sd_cust, NONE, 0},
	{"orders.tbl", "order table", 150000 * ORDERS_PER_CUST,
		NULL, sd_order, LINE, 0},
	{"lineitem.tbl", "lineitem table", 150000 * ORDERS_PER_CUST,
		NULL, sd_line, NONE, 0},
	{"orders.tbl", "orders/lineitem tables", 150000 * ORDERS_PER_CUST,
		NULL, sd_order, LINE, 0},
	{"part.tbl", "part/partsupplier tables", 200000,
		NULL, sd_part, PSUPP, 0},
	{"nation.tbl", "nation table", NATIONS_COUNT,
		NULL, NO_LFUNC, NONE, 0},
	{"region.tbl", "region table", REGIONS_COUNT,
		NULL, NO_LFUNC, NONE, 0},
};

