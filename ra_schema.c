#include "ra_schema.h"
#include "ra_events.h"
#include <gc.h>
#include <string.h>
#include <stdio.h>

#define new(T) ((T *)GC_MALLOC(sizeof(T)))
#define anew(T, N) ((T *)GC_MALLOC((N) * sizeof(T)))
#define snew(N) ((char *)GC_MALLOC_ATOMIC(N))
#define xnew(T, N, U) ((T *)GC_MALLOC(sizeof(T) + (N) * sizeof(U)))

typedef enum { VALUE_FIELD, COMPUTED_FIELD, CONSTANT_FIELD, INSTANCE_FIELD } schema_field_type_t;

struct ra_schema_field_t {
	const char *Name;
	schema_field_type_t Type;
	union {
		int Index;
		struct {
			ml_value_t *Function;
			int NumFields;
			ra_schema_field_t *Fields[];
		};
		ml_value_t *Constant;
	};
};

struct ra_schema_t {
	const ml_type_t *Type;
	const char *Name;
	ra_schema_t *Parent;
	ra_instance_t *Head, *Tail;
	ra_schema_listener_t *Listeners;
	stringmap_t Fields[1];
	stringmap_t Indices[1];
	int InstanceSize, NumListeners, MaxListeners;
};

struct ra_schema_index_node_t {
	ra_schema_index_node_t *Left, *Right;
	ra_instance_t *Instance;
	long Hash;
	int Depth;
};

struct ra_schema_index_t {
	const ml_type_t *Type;
	ra_schema_index_t *Parent;
	ra_schema_index_node_t *Root;
	ra_schema_field_t **Fields;
	int NumFields, NumInstances;
};

struct ra_instance_t {
	const ml_type_t *Type;
	ra_schema_t *Schema;
	ra_schema_listener_t *Listeners;
	ra_instance_t *Next, *Prev;
	int NumListeners, MaxListeners;
	ml_value_t *Values[];
};

ml_type_t RaSchemaT[1] = {{
	MLAnyT, "schema",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_type_t RaSchemaIndexT[1] = {{
	MLAnyT, "schema index",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_type_t RaInstanceT[1] = {{
	MLAnyT, "instance",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

static ml_value_t *CompareMethod;
static stringmap_t Schemas[1] = {STRINGMAP_INIT};
ra_schema_field_t *InstanceField;

static inline long ra_instance_hash(int NumValues, ml_value_t **Values) {
	long Hash = 347981;
	for (int I = 0; I < NumValues; ++I) Hash = ((Hash << 5) + Hash) + ml_hash(Values[I]);
	return Hash;
}

inline ml_value_t *ra_instance_field_by_field(ra_instance_t *Instance, ra_schema_field_t *Field) {
	switch (Field->Type) {
	case VALUE_FIELD:
		return Instance->Values[Field->Index];
	case COMPUTED_FIELD: {
		ml_value_t *Args[Field->NumFields];
		for (int I = 0; I < Field->NumFields; ++I) Args[I] = ra_instance_field_by_field(Instance, Field->Fields[I]);
		return ml_call(Field->Function, Field->NumFields, Args);
	}
	case CONSTANT_FIELD:
		return Field->Constant;
	case INSTANCE_FIELD:
		return (ml_value_t *)Instance;
	default:
		return ml_error("SchemaError", "internal error");
	}
}

ml_value_t *ra_instance_field_by_name(ra_instance_t *Instance, const char *Name) {
	ra_schema_field_t *Field = (ra_schema_field_t *)stringmap_search(Instance->Schema->Fields, Name);
	if (!Field) return ml_error("SchemaError", "schema %s has no field %s.", Instance->Schema->Name, Name);
	return ra_instance_field_by_field(Instance, Field);
}

static int ra_schema_index_node_balance(ra_schema_index_node_t *Node) {
	int Delta = 0;
	if (Node->Left) Delta = Node->Left->Depth;
	if (Node->Right) Delta -= Node->Right->Depth;
	return Delta;
}

static void ra_schema_index_node_update_depth(ra_schema_index_node_t *Node) {
	int Depth = 0;
	if (Node->Left) Depth = Node->Left->Depth;
	if (Node->Right && Depth < Node->Right->Depth) Depth = Node->Right->Depth;
	Node->Depth = Depth + 1;
}

static void ra_schema_index_node_rotate_left(ra_schema_index_node_t **Slot) {
	ra_schema_index_node_t *Ch = Slot[0]->Right;
	Slot[0]->Right = Slot[0]->Right->Left;
	Ch->Left = Slot[0];
	ra_schema_index_node_update_depth(Slot[0]);
	Slot[0] = Ch;
	ra_schema_index_node_update_depth(Slot[0]);
}

static void ra_schema_index_node_rotate_right(ra_schema_index_node_t **Slot) {
	ra_schema_index_node_t *Ch = Slot[0]->Left;
	Slot[0]->Left = Slot[0]->Left->Right;
	Ch->Right = Slot[0];
	ra_schema_index_node_update_depth(Slot[0]);
	Slot[0] = Ch;
	ra_schema_index_node_update_depth(Slot[0]);
}

static void ra_schema_index_node_rebalance(ra_schema_index_node_t **Slot) {
	int Delta = ra_schema_index_node_balance(Slot[0]);
	if (Delta == 2) {
		if (ra_schema_index_node_balance(Slot[0]->Left) < 0) ra_schema_index_node_rotate_left(&Slot[0]->Left);
		ra_schema_index_node_rotate_right(Slot);
	} else if (Delta == -2) {
		if (ra_schema_index_node_balance(Slot[0]->Right) > 0) ra_schema_index_node_rotate_right(&Slot[0]->Right);
		ra_schema_index_node_rotate_left(Slot);
	}
}

static ra_instance_t *ra_schema_index_insert_instance_internal(ra_schema_index_t *Index, ra_schema_index_node_t **Slot, long Hash, ml_value_t **Values, ra_instance_t *Instance) {
	if (!Slot[0]) {
		++Index->NumInstances;
		ra_schema_index_node_t *Node = Slot[0] = new(ra_schema_index_node_t);
		Node->Depth = 1;
		Node->Hash = Hash;
		Node->Instance = Instance;
		return 0;
	}
	int Compare;
	if (Hash < Slot[0]->Hash) {
		Compare = -1;
	} else if (Hash > Slot[0]->Hash) {
		Compare = 1;
	} else {
		ml_value_t *Args[2];
		for (int I = 0; I < Index->NumFields; ++I) {
			Args[0] = Values[I];
			Args[1] = ra_instance_field_by_field(Slot[0]->Instance, Index->Fields[I]);
			ml_value_t *Result = ml_call(CompareMethod, 2, Args);
			if (Result->Type == MLIntegerT) Compare = ml_integer_value(Result);
			if (Compare) break;
		}
	}
	if (!Compare) {
		ra_instance_t *Old = Slot[0]->Instance;
		Slot[0]->Instance = Instance;
		return Old;
	} else {
		ra_instance_t *Old = ra_schema_index_insert_instance_internal(Index, Compare < 0 ? &Slot[0]->Left : &Slot[0]->Right, Hash, Values, Instance);
		ra_schema_index_node_rebalance(Slot);
		ra_schema_index_node_update_depth(Slot[0]);
		return Old;
	}
}

static int ra_schema_index_insert_callback(const char *IndexName, ra_schema_index_t *Index, ra_instance_t *Instance) {
	ml_value_t *Values[Index->NumFields];
	for (int I = 0; I < Index->NumFields; ++I) Values[I] = ra_instance_field_by_field(Instance, Index->Fields[I]);
	long Hash = ra_instance_hash(Index->NumFields, Values);
	do {
		ra_schema_index_insert_instance_internal(Index, &Index->Root, Hash, Values, Instance);
		Index = Index->Parent;
	} while(Index);
	return 0;
}

static int ra_schema_field_copy_callback(const char *Name, ra_schema_field_t *Field, ra_schema_t *Schema) {
	stringmap_insert(Schema->Fields, Name, Field);
	return 0;
}

static int ra_schema_index_copy_callback(const char *Name, ra_schema_index_t *Parent, ra_schema_t *Schema) {
	ra_schema_index_t *Index = new(ra_schema_index_t);
	Index->Type = RaSchemaIndexT;
	Index->Parent = Parent;
	Index->Fields = Parent->Fields;
	Index->NumFields = Parent->NumFields;
	stringmap_insert(Schema->Indices, Name, Index);
	return 0;
}

ra_schema_t *ra_schema_create(const char *Name, ra_schema_t *Parent) {
	ra_schema_t *Schema = new(ra_schema_t);
	Schema->Type = RaSchemaT;
	Schema->Name = Name;
	Schema->Parent = Parent;
	Schema->Fields[0] = STRINGMAP_INIT;
	Schema->Indices[0] = STRINGMAP_INIT;
	if (Parent) {
		stringmap_foreach(Parent->Fields, Schema, (void *)ra_schema_field_copy_callback);
		stringmap_foreach(Parent->Indices, Schema, (void *)ra_schema_index_copy_callback);
		Schema->InstanceSize = Parent->InstanceSize;
	}
	stringmap_insert(Schemas, Name, Schema);
	return Schema;
}

ra_schema_t *ra_schema_by_name(const char *Name) {
	return (ra_schema_t *)stringmap_search(Schemas, Name);
}

ra_schema_field_t *ra_schema_value_field_create(ra_schema_t *Schema, const char *Name) {
	ra_schema_field_t *Field = new(ra_schema_field_t);
	Field->Name = Name;
	Field->Type = VALUE_FIELD;
	Field->Index = Schema->InstanceSize++;
	stringmap_insert(Schema->Fields, Name, Field);
	return Field;
}

ra_schema_field_t *ra_schema_computed_field_create(ra_schema_t *Schema, const char *Name, ml_value_t *Function, const char **FieldNames) {
	int NumFields = 0;
	while (FieldNames[NumFields]) ++NumFields;
	ra_schema_field_t *Field = xnew(ra_schema_field_t, NumFields, ra_schema_field_t *);
	Field->Name = Name;
	Field->Type = COMPUTED_FIELD;
	Field->Function = Function;
	Field->NumFields = NumFields;
	for (int I = 0; I < NumFields; ++I) Field->Fields[I] = (ra_schema_field_t *)stringmap_search(Schema->Fields, FieldNames[I]);
	stringmap_insert(Schema->Fields, Name, Field);
	return Field;
}

ra_schema_field_t *ra_schema_constant_field_create(ra_schema_t *Schema, const char *Name, ml_value_t *Constant) {
	ra_schema_field_t *Field = new(ra_schema_field_t);
	Field->Name = Name;
	Field->Type = CONSTANT_FIELD;
	Field->Constant = Constant;
	stringmap_insert(Schema->Fields, Name, Field);
	return Field;
}

ra_schema_field_t *ra_schema_field_by_name(ra_schema_t *Schema, const char *Name) {
	return (ra_schema_field_t *)stringmap_search(Schema->Fields, Name);
}

ra_schema_index_t *ra_schema_index_create(ra_schema_t *Schema, const char **FieldNames) {
	ra_schema_index_t *Index = new(ra_schema_index_t);
	Index->Type = RaSchemaIndexT;
	Index->Parent = 0;
	int NumFields = 0;
	int IndexNameLength = 0;
	for (const char **FieldName = FieldNames; FieldName[0]; ++FieldName) {
		++NumFields;
		IndexNameLength += 1 + strlen(FieldName[0]);
	}
	Index->NumFields = NumFields;
	ra_schema_field_t **Fields = Index->Fields = anew(ra_schema_field_t *, NumFields);
	char *IndexName = snew(IndexNameLength), *P = IndexName;
	for (int I = 0; I < NumFields; ++I) {
		Fields[I] = ra_schema_field_by_name(Schema, FieldNames[I]) ?: ra_schema_value_field_create(Schema, FieldNames[I]);
		P = stpcpy(P, FieldNames[I]);
		*P++ = ' ';
	}
	P[-1] = 0;
	stringmap_insert(Schema->Indices, IndexName, Index);
	for (ra_instance_t *Instance = Schema->Head; Instance; Instance = Instance->Next) {
		ml_value_t *Values[NumFields];
		for (int I = 0; I < NumFields; ++I) Values[I] = ra_instance_field_by_field(Instance, Fields[I]);
		long Hash = ra_instance_hash(NumFields, Values);
		ra_schema_index_insert_instance_internal(Index, &Index->Root, Hash, Values, Instance);
	}
	return Index;
}

ra_schema_index_t *ra_schema_index_by_names(ra_schema_t *Schema, const char **FieldNames) {
	int IndexNameLength = 0;
	for (const char **FieldName = FieldNames; FieldName[0]; ++FieldName) {
		IndexNameLength += 1 + strlen(FieldName[0]);
	}
	char IndexName[IndexNameLength], *P = IndexName;
	for (const char **FieldName = FieldNames; FieldName[0]; ++FieldName) {
		P = stpcpy(P, FieldName[0]);
		*P++ = ' ';
	}
	P[-1] = 0;
	return (ra_schema_index_t *)stringmap_search(Schema->Indices, IndexName);
}

ra_instance_t *ra_schema_index_search(ra_schema_index_t *Index, ml_value_t **Values) {
	ra_schema_index_node_t *Node = Index->Root;
	long Hash = ra_instance_hash(Index->NumFields, Values);
	while (Node) {
		int Compare;
		if (Hash < Node->Hash) {
			Compare = -1;
		} else if (Hash > Node->Hash) {
			Compare = 1;
		} else {
			ml_value_t *Args[2];
			for (int I = 0; I < Index->NumFields; ++I) {
				Args[0] = Values[I];
				Args[1] = ra_instance_field_by_field(Node->Instance, Index->Fields[I]);
				ml_value_t *Result = ml_call(CompareMethod, 2, Args);
				if (Result->Type == MLIntegerT) Compare = ml_integer_value(Result);
				if (Compare) break;
			}
		}
		if (!Compare) {
			return Node->Instance;
		} else {
			Node = Compare < 0 ? Node->Left : Node->Right;
		}
	}
	return 0;
}

struct ra_schema_listener_t {
	ra_listener_t *Parent;
	ra_schema_listener_t *Next;
	ra_schema_index_t *Index;
	ml_value_t *Target;
	union {
		ml_value_t *IndexFunction;
		ml_value_t **IndexValues;
	};
	ra_schema_field_t **SelectedFields;
	int NumSelectedFields, Negated, Created;
};

struct ra_listener_t {
	const ml_type_t *Type;
	ml_value_t *Callback;
	int NumSelectedFields, NumSchemas;
	ra_schema_listener_t Schemas[];
};

ml_type_t RaListenerT[1] = {{
	MLAnyT, "listener",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

static void ra_listener_remove(ra_listener_t *Listener) {
	for (int I = 0; I < Listener->NumSchemas; ++I) {
		ra_schema_listener_t *SchemaListener = &Listener->Schemas[I];
		ra_schema_listener_t **Slot;
		if (SchemaListener->Target->Type == RaInstanceT) {
			Slot = &((ra_instance_t *)SchemaListener->Target)->Listeners;
		} else {
			Slot = &((ra_schema_t *)SchemaListener->Target)->Listeners;
		}
		while (Slot[0] && Slot[0] != SchemaListener) Slot = &Slot[0]->Next;
		if (Slot[0] == SchemaListener) Slot[0] = SchemaListener->Next;
	}
}

static int ra_schema_listener_apply(ra_schema_listener_t *Listener, ml_value_t **FieldValues, int FieldsStart, ra_schema_listener_t *Initial, ra_instance_t *Instance) {
	ml_value_t *IndexList = ml_call(Listener->IndexFunction, FieldsStart, FieldValues);
	ml_value_t *IndexValues[ml_list_length(IndexList)];
	ml_list_to_array(IndexList, IndexValues);
	if (Listener == Initial) {
		ra_schema_index_t *Index = Listener->Index;
		ml_value_t *Args[2];
		for (int I = 0; I < Index->NumFields; ++I) {
			Args[0] = IndexValues[I];
			Args[1] = ra_instance_field_by_field(Instance, Index->Fields[I]);
			ml_value_t *Result = ml_call(CompareMethod, 2, Args);
			if (Result->Type == MLIntegerT && ml_integer_value(Result) != 0) {
				Instance = 0;
				break;
			}
		}
	} else {
		Instance = ra_schema_index_search(Listener->Index, IndexValues);
	}
	if (Listener->Negated) {
		return !Instance;
	} else if (Instance == 0) {
		return 0;
	} else {
		for (int I = 0; I < Listener->NumSelectedFields; ++I) FieldValues[FieldsStart] = ra_instance_field_by_field(Instance, Listener->SelectedFields[I]);
		return 1;
	}
}

static void ra_listener_apply_instance(ra_listener_t *Listener, ra_instance_t *Instance, ra_schema_listener_t *Initial, ra_instance_t *InitialInstance) {
	ml_value_t **FieldValues = anew(ml_value_t *, Listener->NumSelectedFields);
	int FieldsStart = Listener->Schemas[0].NumSelectedFields;
	for (int I = 0; I < FieldsStart; ++I) FieldValues[I] = ra_instance_field_by_field(Instance, Listener->Schemas[0].SelectedFields[I]);
	for (int I = 1; I < Listener->NumSchemas; ++I) {
		if (!ra_schema_listener_apply(Listener->Schemas + I, FieldValues, FieldsStart, Initial, InitialInstance)) return;
		FieldsStart += Listener->Schemas[I].NumSelectedFields;
	}
	ra_action_enqueue(Listener->Callback, Listener->NumSelectedFields, FieldValues);
}

static void ra_listener_apply_schema(ra_listener_t *Listener, ra_schema_t *Schema, ra_schema_listener_t *Initial, ra_instance_t *InitialInstance) {
	ra_instance_t *Instance = Schema->Head;
	while (Instance) {
		ra_instance_t *Next = Instance->Next;
		ra_listener_apply_instance(Listener, Instance, Initial, InitialInstance);
		Instance = Next;
	}
}

int ra_schema_foreach(ra_schema_t *Schema, void *Data, int (*callback)(ra_instance_t *Instance, void *Data)) {
	ra_instance_t *Instance = Schema->Head;
	while (Instance) {
		ra_instance_t *Next = Instance->Next;
		int Result = callback(Instance, Data);
		if (Result) return Result;
		Instance = Next;
	}
	return 0;
}

ra_instance_t *ra_instance_create(ra_schema_t *Schema, int NumFields, ra_schema_field_t **Fields, ml_value_t **Values, int Signal) {
	ra_instance_t *Instance = xnew(ra_instance_t, Schema->InstanceSize, ml_value_t);
	Instance->Type = RaInstanceT;
	Instance->Schema = Schema;
	for (int I = 0; I < Schema->InstanceSize; ++I) Instance->Values[I] = MLNil;
	for (int I = 0; I < NumFields; ++I) {
		ra_schema_field_t *Field = Fields[I];
		if (Field->Type == VALUE_FIELD) {
			Instance->Values[Field->Index] = Values[I];
		} else {
			return (ra_instance_t *)ml_error("SchemaError", "attempting to initialize read-only field %s", Field->Name);
		}
	}
	if (!Signal) {
		for (ra_schema_t *Parent = Schema; Parent; Parent = Parent->Parent) {
			stringmap_foreach(Parent->Indices, Instance, (void *)ra_schema_index_insert_callback);
		}
		ra_instance_t *Next = Instance->Next = Schema->Head;
		Schema->Head = Instance;
		if (Next) {
			Next->Prev = Instance;
		} else {
			Schema->Tail = Instance;
		}
	}
	while (Schema) {
		ra_schema_listener_t **SchemaListenerSlot = &Schema->Listeners;
		ra_schema_listener_t *SchemaListener = SchemaListenerSlot[0];
		while (SchemaListener) {
			ra_listener_t *Listener = SchemaListener->Parent;
			ra_schema_index_t *Index = SchemaListener->Index;
			if (SchemaListener == Listener->Schemas) {
				if (Index) {
					ml_value_t *Args[2];
					for (int I = 0; I < Index->NumFields; ++I) {
						Args[0] = SchemaListener->IndexValues[I];
						Args[1] = ra_instance_field_by_field(Instance, Index->Fields[I]);
						ml_value_t *Result = ml_call(CompareMethod, 2, Args);
						if (Result->Type == MLIntegerT && ml_integer_value(Result) != 0) {
							SchemaListenerSlot = &SchemaListener->Next;
							goto next;
						}
					}
					SchemaListenerSlot[0] = SchemaListener->Next;
					SchemaListener->Next = Instance->Listeners;
					Instance->Listeners = SchemaListener;
					SchemaListener->Target = (ml_value_t *)Instance;
				} else {
					SchemaListenerSlot = &SchemaListener->Next;
				}
				if (!Listener->Schemas[0].Negated) ra_listener_apply_instance(Listener, Instance, 0, 0);
			} else {
				SchemaListenerSlot = &SchemaListener->Next;
				if (!Listener->Schemas[0].Negated) ra_listener_apply_schema(Listener, Schema, SchemaListener, Instance);
			}
		next:
			SchemaListener = SchemaListenerSlot[0];
		}
		Schema = Schema->Parent;
	}
	return Instance;
}

ra_instance_t *ra_instance_update(ra_instance_t *Instance, int NumFields, ra_schema_field_t **Fields, ml_value_t **Values) {
	for (int I = 0; I < NumFields; ++I) {
		ra_schema_field_t *Field = Fields[I];
		if (Field->Type == VALUE_FIELD) {
			Instance->Values[Field->Index] = Values[I];
		} else {
			return (ra_instance_t *)ml_error("SchemaError", "attempting to initialize read-only field %s", Field->Name);
		}
	}
	ra_schema_t *Schema = Instance->Schema;
	for (ra_schema_listener_t *SchemaListener = Instance->Listeners; SchemaListener; SchemaListener = SchemaListener->Next) {
		ra_listener_t *Listener = SchemaListener->Parent;
		if (SchemaListener == Listener->Schemas) {
			if (!Listener->Schemas[0].Negated && !Listener->Schemas[0].Created) ra_listener_apply_instance(Listener, Instance, 0, 0);
		} else {
			if (!Listener->Schemas[0].Negated && !Listener->Schemas[0].Created) ra_listener_apply_schema(Listener, Schema, SchemaListener, Instance);
		}
	}
	while (Schema) {
		ra_schema_listener_t **SchemaListenerSlot = &Schema->Listeners;
		ra_schema_listener_t *SchemaListener = SchemaListenerSlot[0];
		while (SchemaListener) {
			ra_listener_t *Listener = SchemaListener->Parent;
			ra_schema_index_t *Index = SchemaListener->Index;
			if (SchemaListener == Listener->Schemas) {
				if (Index) {
					ml_value_t *Args[2];
					for (int I = 0; I < Index->NumFields; ++I) {
						Args[0] = SchemaListener->IndexValues[I];
						Args[1] = ra_instance_field_by_field(Instance, Index->Fields[I]);
						ml_value_t *Result = ml_call(CompareMethod, 2, Args);
						if (Result->Type == MLIntegerT && ml_integer_value(Result) != 0) {
							SchemaListenerSlot = &SchemaListener->Next;
							goto next;
						}
					}
					SchemaListenerSlot[0] = SchemaListener->Next;
					SchemaListener->Next = Instance->Listeners;
					Instance->Listeners = SchemaListener;
					SchemaListener->Target = (ml_value_t *)Instance;
				} else {
					SchemaListenerSlot = &SchemaListener->Next;
				}
				if (!Listener->Schemas[0].Negated && !Listener->Schemas[0].Created) ra_listener_apply_instance(Listener, Instance, 0, 0);
			} else {
				SchemaListenerSlot = &SchemaListener->Next;
				if (!Listener->Schemas[0].Negated && !Listener->Schemas[0].Created) ra_listener_apply_schema(Listener, Schema, SchemaListener, Instance);
			}
		next:
			SchemaListener = SchemaListenerSlot[0];
		}
		Schema = Schema->Parent;
	}
	return Instance;
}

static void ra_schema_index_remove_depth_helper(ra_schema_index_node_t *Node) {
	if (Node) {
		ra_schema_index_remove_depth_helper(Node->Right);
		ra_schema_index_node_update_depth(Node);
	}
}

static ra_instance_t *ra_schema_index_remove_instance_internal(ra_schema_index_t *Index, ra_schema_index_node_t **Slot, long Hash, ml_value_t **Values, ra_instance_t *Instance) {
	if (!Slot[0]) return 0;
	int Compare;
	if (Instance == Slot[0]->Instance) {
		Compare = 0;
	} else if (Hash < Slot[0]->Hash) {
		Compare = -1;
	} else if (Hash > Slot[0]->Hash) {
		Compare = 1;
	} else {
		ml_value_t *Args[2];
		for (int I = 0; I < Index->NumFields; ++I) {
			Args[0] = Values[I];
			Args[1] = ra_instance_field_by_field(Slot[0]->Instance, Index->Fields[I]);
			ml_value_t *Result = ml_call(CompareMethod, 2, Args);
			if (Result->Type == MLIntegerT) Compare = ml_integer_value(Result);
			if (Compare) break;
		}
	}
	ra_instance_t *Removed = 0;
	if (!Compare) {
		--Index->NumInstances;
		Removed = Slot[0]->Instance;
		if (Slot[0]->Left && Slot[0]->Right) {
			ra_schema_index_node_t **Y = &Slot[0]->Left;
			while (Y[0]->Right) Y = &Y[0]->Right;
			Slot[0]->Hash = Y[0]->Hash;
			Slot[0]->Instance = Y[0]->Instance;
			Y[0] = Y[0]->Left;
			ra_schema_index_remove_depth_helper(Slot[0]->Left);
		} else if (Slot[0]->Left) {
			Slot[0] = Slot[0]->Left;
		} else if (Slot[0]->Right) {
			Slot[0] = Slot[0]->Right;
		} else {
			Slot[0] = 0;
		}
	} else {
		Removed = ra_schema_index_remove_instance_internal(Index, Compare < 0 ? &Slot[0]->Left : &Slot[0]->Right, Hash, Values, Instance);
	}
	if (Slot[0]) {
		ra_schema_index_node_update_depth(Slot[0]);
		ra_schema_index_node_rebalance(Slot);
	}
	return Removed;
}

typedef struct ra_instance_deletion_t {
	ra_schema_index_t *OriginalIndex;
	ra_instance_t *Instance;
} ra_instance_deletion_t;

static int ra_schema_index_remove_instance_callback(const char *IndexName, ra_schema_index_t *Index, ra_instance_deletion_t *Deletion) {
	if (Index != Deletion->OriginalIndex) {
		ml_value_t *Values[Index->NumFields];
		for (int I = 0; I < Index->NumFields; ++I) Values[I] = ra_instance_field_by_field(Deletion->Instance, Index->Fields[I]);
		long Hash = ra_instance_hash(Index->NumFields, Values);
		ra_schema_index_remove_instance_internal(Index, &Index->Root, Hash, Values, Deletion->Instance);
	}
	return 0;
}

static ra_instance_t *ra_schema_index_remove_instance(ra_schema_index_t *Index, ml_value_t **Values) {
	long Hash = ra_instance_hash(Index->NumFields, Values);
	ra_instance_t *Instance = ra_schema_index_remove_instance_internal(Index, &Index->Root, Hash, Values, 0);
	if (Instance) {
		ra_instance_deletion_t Deletion[1] = {{Index, Instance}};
		for (ra_schema_t *Schema = Instance->Schema; Schema; Schema = Schema->Parent) {
			stringmap_foreach(Schema->Indices, Deletion, (void *)ra_schema_index_remove_instance_callback);
		}
	}
	ra_schema_t *Schema = Instance->Schema;
	for (ra_schema_listener_t *SchemaListener = Instance->Listeners; SchemaListener; SchemaListener = SchemaListener->Next) {
		ra_listener_t *Listener = SchemaListener->Parent;
		if (SchemaListener == Listener->Schemas) {
			if (Listener->Schemas[0].Negated) ra_listener_apply_instance(Listener, Instance, 0, 0);
		} else {
			if (Listener->Schemas[0].Negated) ra_listener_apply_schema(Listener, Schema, 0, 0);
		}
	}
	while (Schema) {
		ra_schema_listener_t **SchemaListenerSlot = &Schema->Listeners;
		ra_schema_listener_t *SchemaListener = SchemaListenerSlot[0];
		while (SchemaListener) {
			ra_listener_t *Listener = SchemaListener->Parent;
			ra_schema_index_t *Index = SchemaListener->Index;
			if (SchemaListener == Listener->Schemas) {
				if (Index) {
					ml_value_t *Args[2];
					for (int I = 0; I < Index->NumFields; ++I) {
						Args[0] = SchemaListener->IndexValues[I];
						Args[1] = ra_instance_field_by_field(Instance, Index->Fields[I]);
						ml_value_t *Result = ml_call(CompareMethod, 2, Args);
						if (Result->Type == MLIntegerT && ml_integer_value(Result) != 0) {
							SchemaListenerSlot = &SchemaListener->Next;
							goto next;
						}
					}
					SchemaListenerSlot[0] = SchemaListener->Next;
					SchemaListener->Next = Instance->Listeners;
					Instance->Listeners = SchemaListener;
					SchemaListener->Target = (ml_value_t *)Instance;
					ra_listener_remove(Listener);
				} else {
					SchemaListenerSlot = &SchemaListener->Next;
				}
				if (Listener->Schemas[0].Negated) ra_listener_apply_instance(Listener, Instance, 0, 0);
			} else {
				SchemaListenerSlot = &SchemaListener->Next;
				if (Listener->Schemas[0].Negated) ra_listener_apply_schema(Listener, Schema, 0, 0);
			}
		next:
			SchemaListener = SchemaListenerSlot[0];
		}
		Schema = Schema->Parent;
	}
	return Instance;
}

void ra_instance_delete(ra_instance_t *Instance) {
	ra_instance_deletion_t Deletion[1] = {{0, Instance}};
	for (ra_schema_t *Schema = Instance->Schema; Schema; Schema = Schema->Parent) {
		stringmap_foreach(Schema->Indices, Deletion, (void *)ra_schema_index_remove_instance_callback);
	}
	ra_schema_t *Schema = Instance->Schema;
	if (Instance->Prev) {
		Instance->Prev->Next = Instance->Next;
	} else {
		Schema->Head = Instance->Next;
	}
	if (Instance->Next) {
		Instance->Next->Prev = Instance->Prev;
	} else {
		Schema->Tail = Instance->Prev;
	}
	for (ra_schema_listener_t *SchemaListener = Instance->Listeners; SchemaListener; SchemaListener = SchemaListener->Next) {
		ra_listener_t *Listener = SchemaListener->Parent;
		if (SchemaListener == Listener->Schemas) {
			if (Listener->Schemas[0].Negated) ra_listener_apply_instance(Listener, Instance, 0, 0);
		} else {
			if (Listener->Schemas[0].Negated) ra_listener_apply_schema(Listener, Schema, 0, 0);
		}
	}
	ra_schema_listener_t **SchemaListenerSlot = &Schema->Listeners;
	ra_schema_listener_t *SchemaListener = SchemaListenerSlot[0];
	while (SchemaListener) {
		ra_listener_t *Listener = SchemaListener->Parent;
		ra_schema_index_t *Index = SchemaListener->Index;
		if (SchemaListener == Listener->Schemas) {
			if (Index) {
				ml_value_t *Args[2];
				for (int I = 0; I < Index->NumFields; ++I) {
					Args[0] = SchemaListener->IndexValues[I];
					Args[1] = ra_instance_field_by_field(Instance, Index->Fields[I]);
					ml_value_t *Result = ml_call(CompareMethod, 2, Args);
					if (Result->Type == MLIntegerT && ml_integer_value(Result) != 0) {
						SchemaListenerSlot = &SchemaListener->Next;
						goto next;
					}
				}
				SchemaListenerSlot[0] = SchemaListener->Next;
				SchemaListener->Next = Instance->Listeners;
				Instance->Listeners = SchemaListener;
			} else {
				SchemaListenerSlot = &SchemaListener->Next;
			}
			if (Listener->Schemas[0].Negated) ra_listener_apply_instance(Listener, Instance, 0, 0);
		} else {
			SchemaListenerSlot = &SchemaListener->Next;
			if (Listener->Schemas[0].Negated) ra_listener_apply_schema(Listener, Schema, 0, 0);
		}
	next:
		SchemaListener = SchemaListenerSlot[0];
	}
}

ml_value_t *ra_listener_create_callback(ra_listener_template_t *Template, int Count, ml_value_t **Args) {
	ra_listener_t *Listener = xnew(ra_listener_t, Template->NumSchemas, ra_schema_listener_t);
	Listener->Type = RaListenerT;
	ra_schema_listener_t *SchemaListener = &Listener->Schemas[0];
	ra_schema_listener_template_t *SchemaTemplate = &Template->Schemas[0];
	SchemaListener->Parent = Listener;
	ra_schema_t *Schema = SchemaTemplate->Schema;
	ra_schema_index_t *Index = SchemaListener->Index = SchemaTemplate->Index;
	SchemaListener->SelectedFields = SchemaTemplate->SelectedFields;
	Listener->NumSelectedFields += (SchemaListener->NumSelectedFields = SchemaTemplate->NumSelectedFields);
	if (Index) {
		ra_instance_t *Instance = ra_schema_index_search(Index, Args);
		if (!Instance) {
			ml_value_t **IndexValues = SchemaListener->IndexValues = anew(ml_value_t *, Index->NumFields);
			memcpy(IndexValues, Args, Index->NumFields * sizeof(ml_value_t *));
		}
		Args += Index->NumFields;
		if (Instance) {
			SchemaListener->Target = (ml_value_t *)Instance;
			SchemaListener->Next = Instance->Listeners;
			Instance->Listeners = SchemaListener;
		} else {
			SchemaListener->Target = (ml_value_t *)Schema;
			SchemaListener->Next = Schema->Listeners;
			Schema->Listeners = SchemaListener;
		}
	} else {
		SchemaListener->Target = (ml_value_t *)Schema;
		SchemaListener->Next = Schema->Listeners;
		Schema->Listeners = SchemaListener;
	}
	SchemaListener->Negated = SchemaTemplate->Negated;
	SchemaListener->Created = SchemaTemplate->Created;
	for (int I = 1; I < Template->NumSchemas; ++I) {
		ra_schema_listener_t *SchemaListener = &Listener->Schemas[1];
		ra_schema_listener_template_t *SchemaTemplate = &Template->Schemas[1];
		SchemaListener->Parent = Listener;
		SchemaListener->Index = SchemaTemplate->Index;
		SchemaListener->SelectedFields = SchemaTemplate->SelectedFields;
		Listener->NumSelectedFields += (SchemaListener->NumSelectedFields = SchemaTemplate->NumSelectedFields);
		SchemaListener->Negated = SchemaTemplate->Negated;
		SchemaListener->Created = SchemaTemplate->Created;
		SchemaListener->IndexFunction = *Args++;
	}
	Listener->NumSchemas = Template->NumSchemas;
	Listener->Callback = *Args;
	return (ml_value_t *)Listener;
}

static ml_value_t *ra_listener_delete_callback(void *Data, int Count, ml_value_t **Args) {
	ra_listener_t *Listener = (ra_listener_t *)Args[0];
	ra_listener_remove(Listener);
	return MLNil;
}

ml_value_t *ra_instance_create_callback(ra_instance_template_t *Template, int Count, ml_value_t **Args) {
	if (Count != Template->NumFields) return ml_error("SchemaError", "expected %d fields but only received %d", Template->NumFields, Count);
	return (ml_value_t *)ra_instance_create(Template->Schema, Template->NumFields, Template->Fields, Args, 0);
}

ml_value_t *ra_instance_signal_callback(ra_instance_template_t *Template, int Count, ml_value_t **Args) {
	if (Count != Template->NumFields) return ml_error("SchemaError", "expected %d fields but only received %d", Template->NumFields, Count);
	return (ml_value_t *)ra_instance_create(Template->Schema, Template->NumFields, Template->Fields, Args, 1);
}

ml_value_t *ra_index_instance_exists_callback(ra_schema_index_t *Index, int Count, ml_value_t **Args) {
	if (Count != Index->NumFields) return ml_error("SchemaError", "expected %d fields but only received %d", Index->NumFields, Count);
	return (ml_value_t *)ra_schema_index_search(Index, Args) ?: MLNil;
}

ml_value_t *ra_index_instance_update_callback(ra_schema_field_t **Fields, int Count, ml_value_t **Args) {
	if (Args[0] == MLNil) return ml_error("SchemaError", "instance not found");
	ra_instance_t *Instance = (ra_instance_t *)Args[0];
	return (ml_value_t *)ra_instance_update(Instance, Count - 1, Fields, Args + 1);
}

ml_value_t *ra_index_instance_delete_callback(ra_schema_index_t *Index, int Count, ml_value_t **Args) {
	if (Count != Index->NumFields) return ml_error("SchemaError", "expected %d fields but only received %d", Index->NumFields, Count);
	return (ml_value_t *)ra_schema_index_remove_instance(Index, Args) ?: MLNil;
}

static ml_value_t *ra_instance_delete_callback(void *Data, int Count, ml_value_t **Args) {
	ra_instance_t *Instance = (ra_instance_t *)Args[0];
	ra_instance_delete(Instance);
	return MLNil;
}

static ml_value_t *ra_instance_index_callback(void *Data, int Count, ml_value_t **Args) {
	ra_instance_t *Instance = (ra_instance_t *)Args[0];
	const char *FieldName = ml_string_value(Args[1]);
	ra_schema_field_t *Field = (ra_schema_field_t *)stringmap_search(Instance->Schema->Fields, FieldName);
	if (!Field) return ml_error("FieldError", "field %s not found in schema %s", FieldName, Instance->Schema->Name);
	switch (Field->Type) {
	case VALUE_FIELD:
		return ml_reference(&Instance->Values[Field->Index]);
	case COMPUTED_FIELD: {
		ml_value_t *Args[Field->NumFields];
		for (int I = 0; I < Field->NumFields; ++I) Args[I] = ra_instance_field_by_field(Instance, Field->Fields[I]);
		return ml_call(Field->Function, Field->NumFields, Args);
	}
	case CONSTANT_FIELD:
		return Field->Constant;
	case INSTANCE_FIELD:
		return (ml_value_t *)Instance;
	default:
		return ml_error("SchemaError", "internal error");
	}
}

void ra_schema_init() {
	CompareMethod = ml_method("?");
	ml_method_by_name("delete", 0, ra_listener_delete_callback, RaListenerT, 0);
	ml_method_by_name("delete", 0, ra_instance_delete_callback, RaInstanceT, 0);
	ml_method_by_name("[]", 0, ra_instance_index_callback, RaInstanceT, MLStringT, 0);
	InstanceField = new(ra_schema_field_t);
	InstanceField->Type = INSTANCE_FIELD;
}
