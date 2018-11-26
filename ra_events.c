#include "ra_events.h"
#include <pthread.h>
#include <gc.h>
#include <stdio.h>
#include <math.h>

#define new(T) ((T *)GC_MALLOC(sizeof(T)))
#define anew(T, N) ((T *)GC_MALLOC((N) * sizeof(T)))
#define snew(N) ((char *)GC_MALLOC_ATOMIC(N))
#define xnew(T, N, U) ((T *)GC_MALLOC(sizeof(T) + (N) * sizeof(U)))

#define TIME_GREATER(Time1, Time2) (Time1->tv_sec == Time2->tv_sec ? Time1->tv_nsec > Time2->tv_nsec : Time1->tv_sec > Time2->tv_sec)

struct ra_event_t {
	const ml_type_t *Type;
	ra_event_t *Next;
	ml_value_t *Function;
	ml_value_t **Args;
	int Count, Recur;
	struct timespec Time[];
};

struct ra_action_t {
	ra_action_t *Next;
	ml_value_t *Function;
	ml_value_t **Args;
	int Count;
};

ml_type_t RaEventT[1] = {{
	MLAnyT, "event",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

static ra_event_t *Events = 0;
static ra_action_t *Actions = 0, **ActionSlot = &Actions, *ActionCache = 0;
static int Running = 1;

static pthread_mutex_t EventsLock[1] = {PTHREAD_MUTEX_INITIALIZER};
static pthread_cond_t ActionAvailable[1] = {PTHREAD_COND_INITIALIZER};

ra_event_t *ra_event_create(ml_value_t *Function, int Count, ml_value_t **Args, struct timespec *Time, int Recur) {
	ra_event_t *Event = xnew(ra_event_t, Recur ? 2 : 1, struct timespec);
	Event->Type = RaEventT;
	Event->Function = Function;
	Event->Count = Count;
	Event->Args = Args;
	if ((Event->Recur = Recur)) {
		clock_gettime(CLOCK_REALTIME, Event->Time);
		Event->Time[1] = Time[0];
	} else {
		Event->Time[0] = Time[0];
	}
	pthread_mutex_lock(EventsLock);
	ra_event_t **Slot = &Events;
	while (Slot[0] && TIME_GREATER(Time, Slot[0]->Time)) Slot = &Slot[0]->Next;
	Event->Next = Slot[0];
	Slot[0] = Event;
	pthread_cond_signal(ActionAvailable);
	pthread_mutex_unlock(EventsLock);
	return Event;
}

void ra_event_adjust(ra_event_t *Event, struct timespec *Time) {
	pthread_mutex_lock(EventsLock);
	Event->Time[0] = Time[0];
	ra_event_t **Slot = &Events;
	while (Slot[0] && Slot[0] != Event) Slot = &Slot[0]->Next;
	if (Slot[0] == Event) Slot[0] = Event->Next;
	Slot = &Events;
	while (Slot[0] && TIME_GREATER(Time, Slot[0]->Time)) Slot = &Slot[0]->Next;
	Event->Next = Slot[0];
	Slot[0] = Event;
	pthread_cond_signal(ActionAvailable);
	pthread_mutex_unlock(EventsLock);
}

static ml_value_t *ra_event_adjust_callback(void *Data, int Count, ml_value_t **Args) {
	ra_event_t *Event = (ra_event_t *)Args[0];
	struct timespec Time[1];
	clock_gettime(CLOCK_REALTIME, Time);
	if (Args[1]->Type == MLIntegerT) {
		Time->tv_sec += ml_integer_value(Args[1]);
	} else if (Args[1]->Type == MLRealT) {
		double Whole, Frac = modf(ml_real_value(Args[1]), &Whole);
		Time->tv_sec += Whole;
		Time->tv_nsec += Frac * 1000000000.0;
	}
	ra_event_adjust(Event, Time);
	return Args[0];
}

void ra_event_cancel(ra_event_t *Event) {
	pthread_mutex_lock(EventsLock);
	ra_event_t **Slot = &Events;
	while (Slot[0] && Slot[0] != Event) Slot = &Slot[0]->Next;
	if (Slot[0] == Event) Slot[0] = Event->Next;
	pthread_cond_signal(ActionAvailable);
	pthread_mutex_unlock(EventsLock);
}

static ml_value_t *ra_event_cancel_callback(void *Data, int Count, ml_value_t **Args) {
	ra_event_t *Event = (ra_event_t *)Args[0];
	ra_event_cancel(Event);
	return MLNil;
}

void ra_action_enqueue(ml_value_t *Function, int Count, ml_value_t **Args) {
	pthread_mutex_lock(EventsLock);
	ra_action_t *Action = ActionCache ?: new(ra_action_t);
	ActionCache = Action->Next;
	Action->Next = 0;
	Action->Function = Function;
	Action->Count = Count;
	Action->Args = Args;
	ActionSlot[0] = Action;
	ActionSlot = &Action->Next;
	pthread_cond_signal(ActionAvailable);
	pthread_mutex_unlock(EventsLock);
}

void ra_events_init() {
	ml_method_by_name("adjust", 0, ra_event_adjust_callback, RaEventT, MLNumberT, 0);
	ml_method_by_name("cancel", 0, ra_event_cancel_callback, RaEventT, 0);
}

void *ra_events_loop(void *Data) {
	pthread_mutex_lock(EventsLock);
	while (Running) {
		ra_action_t *Action;
		while ((Action = Actions)) {
			Actions = 0;
			ActionSlot = &Actions;
			while (Action) {
				ra_action_t *NextAction = Action->Next;
				pthread_mutex_unlock(EventsLock);
				ml_value_t *Result = ml_call(Action->Function, Action->Count, Action->Args);
				if (Result->Type == MLErrorT) {
					printf("\e[31mError: %s\n\e[0m", ml_error_message(Result));
					const char *Source;
					int Line;
					for (int I = 0; ml_error_trace(Result, I, &Source, &Line); ++I) printf("\e[31m\t%s:%d\n\e[0m", Source, Line);
				}
				pthread_mutex_lock(EventsLock);
				Action->Next = ActionCache;
				ActionCache = Action;
				Action = NextAction;
			}
		}
		ra_event_t *Event = Events;
		if (Event) {
			struct timespec Time[1];
			clock_gettime(CLOCK_REALTIME, Time);
			if (TIME_GREATER(Time, Event->Time)) {
				Events = Event->Next;
				pthread_mutex_unlock(EventsLock);
				ml_value_t *Result = ml_call(Event->Function, Event->Count, Event->Args);
				if (Result->Type == MLErrorT) {
					printf("\e[31mError: %s\n\e[0m", ml_error_message(Result));
					const char *Source;
					int Line;
					for (int I = 0; ml_error_trace(Result, I, &Source, &Line); ++I) printf("\e[31m\t%s:%d\n\e[0m", Source, Line);
				}
				pthread_mutex_lock(EventsLock);
				if (Event->Recur && Result == MLNil) {
					Event->Time->tv_sec += Event->Time[1].tv_sec;
					Event->Time->tv_nsec += Event->Time[1].tv_nsec;
					ra_event_t **Slot = &Events;
					while (Slot[0] && TIME_GREATER(Time, Slot[0]->Time)) Slot = &Slot[0]->Next;
					Event->Next = Slot[0];
					Slot[0] = Event;
				}
			} else {
				pthread_cond_timedwait(ActionAvailable, EventsLock, Event->Time);
			}
		} else {
			pthread_cond_wait(ActionAvailable, EventsLock);
		}
	}
	pthread_mutex_unlock(EventsLock);
	return 0;
}
