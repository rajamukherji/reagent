#ifndef RA_EVENTS_H
#define RA_EVENTS_H

#include "minilang.h"
#include <time.h>

typedef struct ra_event_t ra_event_t;
typedef struct ra_action_t ra_action_t;

void ra_action_enqueue(ml_value_t *Function, int Count, ml_value_t **Args);

ra_event_t *ra_event_create(ml_value_t *Function, int Count, ml_value_t **Args, struct timespec *Time, int Recur);
void ra_event_adjust(ra_event_t *Event, struct timespec *Time);
void ra_event_delete(ra_event_t *Event);
void ra_events_init();
void *ra_events_loop(void *Data);

#endif
