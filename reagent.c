#include "reagent.h"
#include "minilang.h"
#include "ml_file.h"
#include "stringmap.h"
#include "ra_schema.h"
#include "ra_events.h"
//#include "ra_sigar.h"
#include <stdio.h>
#include <gc.h>
#include <string.h>
#include <pthread.h>
#include <math.h>

#define new(T) ((T *)GC_MALLOC(sizeof(T)))
#define anew(T, N) ((T *)GC_MALLOC((N) * sizeof(T)))
#define snew(N) ((char *)GC_MALLOC_ATOMIC(N))
#define xnew(T, N, U) ((T *)GC_MALLOC(sizeof(T) + (N) * sizeof(U)))

static stringmap_t Globals[1] = {STRINGMAP_INIT};

static ml_value_t *reagent_get_global(void *Data, const char *Name) {
	return stringmap_search(Globals, Name) ?: ml_error("NameError", "Identifier %s is not defined", Name);
}

static ml_value_t *print(void *Data, int Count, ml_value_t **Args) {
	ml_value_t *StringMethod = ml_method("string");
	for (int I = 0; I < Count; ++I) {
		ml_value_t *Result = Args[I];
		if (Result->Type != MLStringT) {
			Result = ml_call(StringMethod, 1, &Result);
			if (Result->Type == MLErrorT) return Result;
			if (Result->Type != MLStringT) return ml_error("ResultError", "string method did not return string");
		}
		fputs(ml_string_value(Result), stdout);
	}
	fflush(stdout);
	return MLNil;
}

static ml_value_t *after(void *Data, int Count, ml_value_t **Args) {
	if (Count < 2) return ml_error("ParamError", "at least one argument required");
	struct timespec Time[1];
	clock_gettime(CLOCK_REALTIME, Time);
	if (Args[0]->Type == MLIntegerT) {
		Time->tv_sec += ml_integer_value(Args[0]);
	} else if (Args[0]->Type == MLRealT) {
		double Whole, Frac = modf(ml_real_value(Args[1]), &Whole);
		Time->tv_sec += Whole;
		Time->tv_nsec += Frac * 1000000000.0;
	} else {
		return ml_error("ParamError", "time delay must be a number");
	}
	ml_value_t **CallbackArgs = 0;
	if (Count > 2) {
		CallbackArgs = anew(ml_value_t *, Count - 2);
		memcpy(CallbackArgs, Args + 2, (Count - 2) * sizeof(ml_value_t *));
	}
	return (ml_value_t *)ra_event_create(Args[1], Count - 2, CallbackArgs, Time, 0);
}

static ml_value_t *every(void *Data, int Count, ml_value_t **Args) {
	if (Count < 2) return ml_error("ParamError", "at least one argument required");
	struct timespec Time[2];
	if (Args[0]->Type == MLIntegerT) {
		Time->tv_sec = ml_integer_value(Args[0]);
		Time->tv_nsec = 0;
	} else if (Args[0]->Type == MLRealT) {
		double Whole, Frac = modf(ml_real_value(Args[1]), &Whole);
		Time->tv_sec = Whole;
		Time->tv_nsec = Frac * 1000000000.0;
	} else {
		return ml_error("ParamError", "time delay must be a number");
	}
	ml_value_t **CallbackArgs = 0;
	if (Count > 2) {
		CallbackArgs = anew(ml_value_t *, Count - 2);
		memcpy(CallbackArgs, Args + 2, (Count - 2) * sizeof(ml_value_t *));
	}
	return (ml_value_t *)ra_event_create(Args[1], Count - 2, CallbackArgs, Time, 1);
}

int main(int Argc, const char **Argv) {
	GC_init();
	ml_init(reagent_get_global);
	ml_file_init();
	ra_schema_init();
	ra_events_init();
	stringmap_insert(Globals, "print", ml_function(0, print));
	stringmap_insert(Globals, "after", ml_function(0, after));
	stringmap_insert(Globals, "every", ml_function(0, every));
	stringmap_insert(Globals, "open", ml_function(0, ml_file_open));
	//stringmap_insert(Globals, "sigar_init", ml_function(0, ra_sigar_init));
	//stringmap_insert(Globals, "kill_process", ml_function(0, ra_kill_process));
	if (Argc > 1) {
		ml_value_t *Closure = ml_load(reagent_get_global, Globals, Argv[1]);
		if (Closure->Type == MLErrorT) {
			printf("\e[31mError: %s\n\e[0m", ml_error_message(Closure));
			const char *Source;
			int Line;
			for (int I = 0; ml_error_trace(Closure, I, &Source, &Line); ++I) printf("\e[31m\t%s:%d\n\e[0m", Source, Line);
			exit(1);
		}
		ml_value_t *Result = ml_call(Closure, 0, 0);
		if (Result->Type == MLErrorT) {
			printf("\e[31mError: %s\n\e[0m", ml_error_message(Result));
			const char *Source;
			int Line;
			for (int I = 0; ml_error_trace(Result, I, &Source, &Line); ++I) printf("\e[31m\t%s:%d\n\e[0m", Source, Line);
			exit(1);
		}
	}
	pthread_t DispatchThread[1];
	GC_pthread_create(DispatchThread, 0, ra_events_loop, 0);
	ml_console(reagent_get_global, Globals);
}
