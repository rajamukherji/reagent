#include "ra_sigar.h"
#include "ra_schema.h"
#include "ra_events.h"
#include <string.h>
#include <sigar.h>
#include <gc.h>

#define new(T) ((T *)GC_MALLOC(sizeof(T)))
#define anew(T, N) ((T *)GC_MALLOC((N) * sizeof(T)))
#define snew(N) ((char *)GC_MALLOC_ATOMIC(N))
#define xnew(T, N, U) ((T *)GC_MALLOC(sizeof(T) + (N) * sizeof(U)))

static sigar_t *Sigar;
static ra_schema_t *ProcessSchema;
static ra_schema_index_t *ProcessSchemaPidIndex;

enum {
	PS_FIELD_PID,
	PS_FIELD_NAME,
	PS_FIELD_STATE,
	PS_FIELD_PARENT,
	PS_FIELD_PRIORITY,
	PS_FIELD_NICE,
	PS_FIELD_PROCESSOR,
	PS_FIELD_THREADS,
	PS_FIELD_START_TIME,
	PS_FIELD_USER_TIME,
	PS_FIELD_SYS_TIME,
	PS_FIELD_TOTAL_TIME,
	PS_FIELD_LAST_TIME,
	PS_FIELD_CPU_PERCENT,
	PS_MEM_SIZE,
	PS_MEM_RESIDENT,
	PS_MEM_SHARE,
	PS_MINOR_FAULTS,
	PS_MAJOR_FAULTS,
	PS_PAGE_FAULTS,
	NUM_PS_FIELDS
};

static const char *ProcessSchemaFieldNames[NUM_PS_FIELDS] = {
	"Pid",
	"Name",
	"State",
	"Parent",
	"Priority",
	"Nice",
	"Processor",
	"Threads",
	"StartTime",
	"UserTime",
	"SysTime",
	"TotalTime",
	"LastTime",
	"CpuPercent",
	"MemSize",
	"MemResident",
	"MemShare",
	"MinorFaults",
	"MajorFaults",
	"PageFaults"
};

static ra_schema_field_t *ProcessSchemaFields[NUM_PS_FIELDS];

ml_value_t *ra_update_processes(void *Data, int Count, ml_value_t **Args) {
	static ra_instance_t **OldInstances;
	static int NumOldInstances = 0;
	sigar_proc_list_t ProcList[1];
	if (sigar_proc_list_get(Sigar, ProcList) != SIGAR_OK) {
		return ml_error("SigarError", "failed to get process list");
	}
	ra_instance_t **NewInstances = anew(ra_instance_t *, ProcList->number);
	for (int I = 0; I < ProcList->number; ++I) {
		sigar_pid_t Pid = ProcList->data[I];
		ml_value_t *Values[NUM_PS_FIELDS];
		Values[PS_FIELD_PID] = ml_integer(Pid);
		sigar_proc_state_t ProcState[1];
		sigar_proc_cpu_t ProcCpu[1];
		sigar_proc_mem_t ProcMem[1];
		if (sigar_proc_state_get(Sigar, Pid, ProcState) != SIGAR_OK) continue;
		if (sigar_proc_cpu_get(Sigar, Pid, ProcCpu) != SIGAR_OK) continue;
		if (sigar_proc_mem_get(Sigar, Pid, ProcMem) != SIGAR_OK) continue;
		int NameLength = strlen(ProcState->name);
		char *Name = snew(NameLength + 1);
		strcpy(Name, ProcState->name);
		Values[PS_FIELD_NAME] = ml_string(Name, -1);
		Values[PS_FIELD_STATE] = ml_string(&ProcState->state, 1);
		Values[PS_FIELD_PARENT] = ml_integer(ProcState->ppid);
		Values[PS_FIELD_NICE] = ml_integer(ProcState->nice);
		Values[PS_FIELD_PROCESSOR] = ml_integer(ProcState->processor);
		Values[PS_FIELD_THREADS] = ml_integer(ProcState->processor);
		Values[PS_FIELD_START_TIME] = ml_integer(ProcCpu->start_time);
		Values[PS_FIELD_USER_TIME] = ml_integer(ProcCpu->user);
		Values[PS_FIELD_SYS_TIME] = ml_integer(ProcCpu->sys);
		Values[PS_FIELD_TOTAL_TIME] = ml_integer(ProcCpu->total);
		Values[PS_FIELD_LAST_TIME] = ml_integer(ProcCpu->last_time);
		Values[PS_FIELD_CPU_PERCENT] = ml_real(ProcCpu->percent);
		Values[PS_MEM_SIZE] = ml_real(ProcMem->size);
		Values[PS_MEM_RESIDENT] = ml_real(ProcMem->resident);
		Values[PS_MEM_SHARE] = ml_real(ProcMem->share);
		Values[PS_MINOR_FAULTS] = ml_integer(ProcMem->minor_faults);
		Values[PS_MAJOR_FAULTS] = ml_integer(ProcMem->major_faults);
		Values[PS_PAGE_FAULTS] = ml_integer(ProcMem->page_faults);
		ra_instance_t *Instance = ra_schema_index_search(ProcessSchemaPidIndex, Values);
		if (Instance) {
			ra_instance_update(Instance, NUM_PS_FIELDS - 1, ProcessSchemaFields + 1, Values + 1);
			for (int J = 0; J < NumOldInstances; ++J) {
				if (OldInstances[J] == Instance) {
					OldInstances[J] = 0;
					break;
				}
			}
		} else {
			Instance = ra_instance_create(ProcessSchema, NUM_PS_FIELDS, ProcessSchemaFields, Values, 0);
		}
		NewInstances[I] = Instance;
	}
	for (int J = 0; J < NumOldInstances; ++J) if (OldInstances[J]) ra_instance_delete(OldInstances[J]);
	OldInstances = NewInstances;
	NumOldInstances = ProcList->number;
	sigar_proc_list_destroy(Sigar, ProcList);
	return Nil;
}

ml_value_t *ra_kill_process(void *Data, int Count, ml_value_t **Args) {
	if (Count < 1 || Args[0]->Type != IntegerT) return ml_error("ParamError", "kill_process called with invalid parameters");
	sigar_pid_t Pid = ml_integer_value(Args[0]);
	sigar_proc_kill(Pid, 9);
	return Nil;
}

ml_value_t *ra_sigar_init(void *Data, int Count, ml_value_t **Args) {
	if (Count < 1 || Args[0]->Type != IntegerT) return ml_error("ParamError", "sigar_init called with invalid parameters");
	sigar_open(&Sigar);
	ProcessSchema = ra_schema_by_name("process") ?: ra_schema_create("process", 0);
	if (!ProcessSchema) return ml_error("SigarError", "process schema must be created before using sigar");
	for (int I = 0; I < NUM_PS_FIELDS; ++I) {
		ProcessSchemaFields[I] = ra_schema_field_by_name(ProcessSchema, ProcessSchemaFieldNames[I]) ?: ra_schema_value_field_create(ProcessSchema, ProcessSchemaFieldNames[I]);
	}
	const char *FieldNames[] = {"Pid", 0};
	ProcessSchemaPidIndex = ra_schema_index_by_names(ProcessSchema, FieldNames) ?: ra_schema_index_create(ProcessSchema, FieldNames);
	struct timespec Time[1];
	Time->tv_sec = ml_integer_value(Args[0]);
	Time->tv_nsec = 0;
	ra_event_create(ml_function(0, ra_update_processes), 0, 0, Time, 1);
	return Nil;
}

