#ifndef RA_SIGAR_H
#define RA_SIGAR_H

#include "minilang.h"

ml_value_t *ra_sigar_init(void *Data, int Count, ml_value_t **Args);
ml_value_t *ra_kill_process(void *Data, int Count, ml_value_t **Args);

#endif
