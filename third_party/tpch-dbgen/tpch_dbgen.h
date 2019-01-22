#pragma once

/**
 * tpch_dbgen.c/h among other things contain the data originating from dists.dss and are therefore subject to the TPCH
 * license (see LICENSE)
 */

#define NATIONS_COUNT 25
#define REGIONS_COUNT 5

typedef struct
{
  long      weight;
  char     *text;
  long      len;
}         set_member;

typedef struct
{
  int      count;
  int      max;
  set_member *list;
  long *permute;
}         distribution;


extern distribution nations;
extern distribution regions;
extern distribution o_priority_set;
extern distribution l_instruct_set;
extern distribution l_smode_set;
extern distribution l_category_set;
extern distribution l_rflag_set;
extern distribution c_mseg_set;
extern distribution colors;
extern distribution p_types_set;
extern distribution p_cntr_set;

/* distributions that control text generation */
extern distribution articles;
extern distribution nouns;
extern distribution adjectives;
extern distribution adverbs;
extern distribution prepositions;
extern distribution verbs;
extern distribution terminators;
extern distribution auxillaries;
extern distribution np;
extern distribution vp;
extern distribution grammar;

void dbgen_reset_seeds();

