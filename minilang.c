#include "minilang.h"
#include "ra_schema.h"
#include "sha256.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <gc.h>
#include <gc/gc_typed.h>
#include <setjmp.h>
#include <ctype.h>
#include <regex.h>
#include "linenoise.h"
#include "stringmap.h"

#define MAX_TRACE 16
#define new(T) ((T *)GC_MALLOC(sizeof(T)))
#define anew(T, N) ((T *)GC_MALLOC((N) * sizeof(T)))
#define snew(N) ((char *)GC_MALLOC_ATOMIC(N))
#define xnew(T, N, U) ((T *)GC_MALLOC(sizeof(T) + (N) * sizeof(U)))

typedef struct ml_reference_t ml_reference_t;
typedef struct ml_integer_t ml_integer_t;
typedef struct ml_real_t ml_real_t;
typedef struct ml_string_t ml_string_t;
typedef struct ml_list_t ml_list_t;
typedef struct ml_tree_t ml_tree_t;
typedef struct ml_object_t ml_object_t;
typedef struct ml_property_t ml_property_t;
typedef struct ml_closure_t ml_closure_t;
typedef struct ml_method_t ml_method_t;
typedef struct ml_error_t ml_error_t;

typedef struct ml_frame_t ml_frame_t;
typedef struct ml_inst_t ml_inst_t;

typedef struct ml_list_node_t ml_list_node_t;
typedef struct ml_tree_node_t ml_tree_node_t;

typedef struct ml_source_t ml_source_t;

struct ml_source_t {
	const char *Name;
	int Line;
};

struct ml_t {
	void *Data;
	ml_getter_t Get;
	ml_value_t *Error;
	jmp_buf OnError;
};

long ml_default_hash(ml_value_t *Value) {
	long Hash = 5381;
	for (const char *P = Value->Type->Name; P[0]; ++P) Hash = ((Hash << 5) + Hash) + P[0];
	return Hash;
}

ml_value_t *ml_default_call(ml_value_t *Value, int Count, ml_value_t **Args) {
	return ml_error("TypeError", "value is not callable");
}

ml_value_t *ml_default_deref(ml_value_t *Ref) {
	return Ref;
}

ml_value_t *ml_default_assign(ml_value_t *Ref, ml_value_t *Value) {
	return ml_error("TypeError", "value is not assignable");
}

ml_value_t *ml_default_next(ml_value_t *Iter) {
	return ml_error("TypeError", "%s is not iterable", Iter->Type->Name);
}

ml_value_t *ml_default_key(ml_value_t *Iter) {
	return MLNil;
}

ml_value_t *CompareMethod;
ml_value_t *AppendMethod;

ml_type_t MLAnyT[1] = {{
	NULL, "any",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

static ml_value_t *ml_nil_to_string(void *Data, int Count, ml_value_t **Args) {
	return ml_string("nil", 3);
}

ml_type_t MLNilT[1] = {{
	MLAnyT, "nil",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t MLNil[1] = {{MLNilT}};

static ml_value_t *ml_some_to_string(void *Data, int Count, ml_value_t **Args) {
	return ml_string("some", 3);
}

ml_type_t MLSomeT[1] = {{
	MLAnyT, "nil",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t MLSome[1] = {{MLSomeT}};

long ml_hash(ml_value_t *Value) {
	Value = Value->Type->deref(Value);
	return Value->Type->hash(Value);
}

ml_value_t *ml_call(ml_value_t *Value, int Count, ml_value_t **Args) {
	return Value->Type->call(Value, Count, Args);
}

ml_value_t *ml_inline(ml_value_t *Value, int Count, ...) {
	ml_value_t *Args[Count];
	va_list List;
	va_start(List, Count);
	for (int I = 0; I < Count; ++I) Args[I] = va_arg(List, ml_value_t *);
	va_end(List);
	return Value->Type->call(Value, Count, Args);
}

static ml_value_t *ml_function_call(ml_value_t *Value, int Count, ml_value_t **Args) {
	ml_function_t *Function = (ml_function_t *)Value;
	return (Function->Callback)(Function->Data, Count, Args);
}

ml_type_t MLFunctionT[1] = {{
	MLAnyT, "function",
	ml_default_hash,
	ml_function_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_function(void *Data, ml_callback_t Callback) {
	ml_function_t *Function = fnew(ml_function_t);
	Function->Type = MLFunctionT;
	Function->Data = Data;
	Function->Callback = Callback;
	GC_end_stubborn_change(Function);
	return (ml_value_t *)Function;
}

ml_type_t MLNumberT[1] = {{
	MLAnyT, "number",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

struct ml_integer_t {
	const ml_type_t *Type;
	long Value;
};

struct ml_real_t {
	const ml_type_t *Type;
	double Value;
};

struct ml_string_t {
	const ml_type_t *Type;
	const char *Value;
	int Length;
};

static long ml_integer_hash(ml_value_t *Value) {
	ml_integer_t *Integer = (ml_integer_t *)Value;
	return Integer->Value;
}

ml_type_t MLIntegerT[1] = {{
	MLNumberT, "integer",
	ml_integer_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_integer(long Value) {
	ml_integer_t *Integer = fnew(ml_integer_t);
	Integer->Type = MLIntegerT;
	Integer->Value = Value;
	GC_end_stubborn_change(Integer);
	return (ml_value_t *)Integer;
}

int ml_is_integer(ml_value_t *Value) {
	return Value->Type == MLIntegerT;
}

long ml_integer_value(ml_value_t *Value) {
	return ((ml_integer_t *)Value)->Value;
}

static long ml_real_hash(ml_value_t *Value) {
	ml_real_t *Real = (ml_real_t *)Value;
	return (long)Real->Value;
}

ml_type_t MLRealT[1] = {{
	MLNumberT, "real",
	ml_real_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_real(double Value) {
	ml_real_t *Real = fnew(ml_real_t);
	Real->Type = MLRealT;
	Real->Value = Value;
	GC_end_stubborn_change(Real);
	return (ml_value_t *)Real;
}

int ml_is_real(ml_value_t *Value) {
	return Value->Type == MLRealT;
}

double ml_real_value(ml_value_t *Value) {
	return ((ml_real_t *)Value)->Value;
}

static long ml_string_hash(ml_value_t *Value) {
	ml_string_t *String = (ml_string_t *)Value;
	long Hash = 5381;
	for (int I = 0; I < String->Length; ++I) Hash = ((Hash << 5) + Hash) + String->Value[I];
	return Hash;
}

static ml_value_t *ml_string_index(void *Data, int Count, ml_value_t **Args) {
	ml_string_t *String = (ml_string_t *)Args[0];
	int Index = ((ml_integer_t *)Args[1])->Value;
	if (Index <= 0) Index += String->Length + 1;
	if (Index <= 0) return MLNil;
	if (Index > String->Length) return MLNil;
	char *Chars = snew(2);
	Chars[0] = String->Value[Index - 1];
	Chars[1] = 0;
	return ml_string(Chars, 1);
}

static ml_value_t *ml_string_slice(void *Data, int Count, ml_value_t **Args) {
	ml_string_t *String = (ml_string_t *)Args[0];
	int Lo = ((ml_integer_t *)Args[1])->Value;
	int Hi = ((ml_integer_t *)Args[2])->Value;
	if (Lo <= 0) Lo += String->Length + 1;
	if (Hi <= 0) Hi += String->Length + 1;
	if (Lo <= 0) return MLNil;
	if (Hi > String->Length + 1) return MLNil;
	if (Hi < Lo) return MLNil;
	int Length = Hi - Lo;
	char *Chars = snew(Length + 1);
	memcpy(Chars, String->Value + Lo - 1, Length);
	Chars[Length] = 0;
	return ml_string(Chars, Length);
}

const char *ml_string_value(ml_value_t *Value) {
	return ((ml_string_t *)Value)->Value;
}

int ml_string_length(ml_value_t *Value) {
	return ((ml_string_t *)Value)->Length;
}

static ml_value_t *ml_string_trim(void *Data, int Count, ml_value_t **Args) {
	const char *Start = ml_string_value(Args[0]);
	const char *End = Start + ml_string_length(Args[0]);
	while (Start < End && Start[0] <= ' ') ++Start;
	while (Start < End && End[-1] <= ' ') --End;
	int Length = End - Start;
	char *Chars = snew(Length + 1);
	memcpy(Chars, Start, Length);
	Chars[Length] = 0;
	return ml_string(Chars, Length);
}

static ml_value_t *ml_string_length_value(void *Data, int Count, ml_value_t **Args) {
	return ml_integer(((ml_string_t *)Args[0])->Length);
}

ml_value_t *ml_string_string_split(void *Data, int Count, ml_value_t **Args) {
	ml_value_t *Results = ml_list();
	const char *Subject = ml_string_value(Args[0]);
	const char *Pattern = ml_string_value(Args[1]);
	size_t Length = strlen(Pattern);
	for (;;) {
		const char *Next = strstr(Subject, Pattern);
		while (Next == Subject) {
			Subject += Length;
			Next = strstr(Subject, Pattern);
		}
		if (!Subject[0]) return Results;
		if (Next) {
			size_t MatchLength = Next - Subject;
			char *Match = snew(MatchLength + 1);
			memcpy(Match, Subject, MatchLength);
			Match[MatchLength] = 0;
			ml_list_append(Results, ml_string(Match, MatchLength));
			Subject = Next + Length;
		} else {
			ml_list_append(Results, ml_string(Subject, strlen(Subject)));
			break;
		}
	}
	return Results;
}

static ml_value_t *ml_string_find(void *Data, int Count, ml_value_t **Args) {
	const char *Haystack = ml_string_value(Args[0]);
	const char *Needle = ml_string_value(Args[1]);
	const char *Match = strstr(Haystack, Needle);
	if (Match) {
		return ml_integer(1 + Match - Haystack);
	} else {
		return MLNil;
	}
}

ml_value_t *ml_string_match(void *Data, int Count, ml_value_t **Args) {
	const char *Subject = ml_string_value(Args[0]);
	const char *Pattern = ml_string_value(Args[1]);
	regex_t Regex[1];
	int Error = regcomp(Regex, Pattern, REG_EXTENDED);
	if (Error) {
		size_t ErrorSize = regerror(Error, Regex, NULL, 0);
		char *ErrorMessage = snew(ErrorSize + 1);
		regerror(Error, Regex, ErrorMessage, ErrorSize);
		return ml_error("RegexError", ErrorMessage);
	}
	regmatch_t Matches[Regex->re_nsub];
	switch (regexec(Regex, Subject, Regex->re_nsub, Matches, 0)) {
	case REG_NOMATCH:
		regfree(Regex);
		return MLNil;
	case REG_ESPACE: {
		regfree(Regex);
		size_t ErrorSize = regerror(REG_ESPACE, Regex, NULL, 0);
		char *ErrorMessage = snew(ErrorSize + 1);
		regerror(Error, Regex, ErrorMessage, ErrorSize);
		return ml_error("RegexError", ErrorMessage);
	}
	default: {
		ml_value_t *Results = ml_list();
		for (int I = 0; I < Regex->re_nsub; ++I) {
			regoff_t Start = Matches[I].rm_so;
			if (Start >= 0) {
				size_t Length = Matches[I].rm_eo - Start;
				char *Chars = snew(Length + 1);
				memcpy(Chars, Subject + Start, Length);
				Chars[Length] = 0;
				ml_list_append(Results, ml_string(Chars, Length));
			} else {
				ml_list_append(Results, MLNil);
			}
		}
		regfree(Regex);
		return Results;
	}
	}
}

ml_value_t *ml_string_string_replace(void *Data, int Count, ml_value_t **Args) {
	const char *Subject = ml_string_value(Args[0]);
	int SubjectLength = ml_string_length(Args[0]);
	const char *Pattern = ml_string_value(Args[1]);
	const char *Replace = ml_string_value(Args[2]);
	int ReplaceLength = ml_string_length(Args[2]);
	regex_t Regex[1];
	int Error = regcomp(Regex, Pattern, REG_EXTENDED);
	if (Error) {
		size_t ErrorSize = regerror(Error, Regex, NULL, 0);
		char *ErrorMessage = snew(ErrorSize + 1);
		regerror(Error, Regex, ErrorMessage, ErrorSize);
		return ml_error("RegexError", ErrorMessage);
	}
	regmatch_t Matches[1];
	ml_stringbuffer_t Buffer[1] = {ML_STRINGBUFFER_INIT};
	for (;;) {
		switch (regexec(Regex, Subject, 1, Matches, 0)) {
		case REG_NOMATCH:
			if (SubjectLength) ml_stringbuffer_add(Buffer, Subject, SubjectLength);
			ml_string_t *String = fnew(ml_string_t);
			String->Type = MLStringT;
			String->Length = Buffer->Length;
			String->Value = ml_stringbuffer_get(Buffer);
			GC_end_stubborn_change(String);
			return (ml_value_t *)String;
		case REG_ESPACE: {
			regfree(Regex);
			size_t ErrorSize = regerror(REG_ESPACE, Regex, NULL, 0);
			char *ErrorMessage = snew(ErrorSize + 1);
			regerror(Error, Regex, ErrorMessage, ErrorSize);
			return ml_error("RegexError", ErrorMessage);
		}
		default: {
			regoff_t Start = Matches[0].rm_so;
			if (Start > 0) ml_stringbuffer_add(Buffer, Subject, Start);
			ml_stringbuffer_add(Buffer, Replace, ReplaceLength);
			Subject += Matches[0].rm_eo;
			SubjectLength -= Matches[0].rm_eo;
		}
		}
	}
	return 0;
}

ml_type_t MLStringT[1] = {{
	MLAnyT, "string",
	ml_string_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_string(const char *Value, int Length) {
	ml_string_t *String = fnew(ml_string_t);
	String->Type = MLStringT;
	String->Value = Value;
	String->Length = Length >= 0 ? Length : strlen(Value);
	GC_end_stubborn_change(String);
	return (ml_value_t *)String;
}

int ml_is_string(ml_value_t *Value) {
	return Value->Type == MLStringT;
}

static ml_value_t *ml_string_new(void *Data, int Count, ml_value_t **Args) {
	ml_stringbuffer_t Buffer[1] = {ML_STRINGBUFFER_INIT};
	for (int I = 0; I < Count; ++I) ml_inline(AppendMethod, 2, (ml_value_t *)Buffer, Args[I]);
	ml_string_t *String = fnew(ml_string_t);
	String->Type = MLStringT;
	String->Length = Buffer->Length;
	String->Value = ml_stringbuffer_get(Buffer);
	GC_end_stubborn_change(String);
	return (ml_value_t *)String;
}

static ml_function_t StringNew[1] = {{MLFunctionT, ml_string_new, NULL}};

typedef struct ml_method_node_t ml_method_node_t;

struct ml_method_node_t {
	ml_method_node_t *Child;
	ml_method_node_t *Next;
	const ml_type_t *Type;
	void *Data;
	ml_callback_t Callback;
};

struct ml_method_t {
	const ml_type_t *Type;
	const char *Name;
	ml_method_node_t Root[1];
};

static ml_method_node_t *ml_method_find(ml_method_node_t *Node, int Count, ml_value_t **Args) {
	if (Count == 0) return Node;
	for (const ml_type_t *Type = Args[0]->Type; Type; Type = Type->Parent) {
		for (ml_method_node_t *Test = Node->Child; Test; Test = Test->Next) {
			if (Test->Type == Type) {
				ml_method_node_t *Result = ml_method_find(Test, Count - 1, Args + 1);
				if (Result && Result->Callback) return Result;
			}
		}
	}
	return Node;
}

static long ml_method_hash(ml_value_t *Value) {
	ml_method_t *Method = (ml_method_t *)Value;
	long Hash = 5381;
	for (const char *P = Method->Name;P[0]; ++P) Hash = ((Hash << 5) + Hash) + P[0];
	return Hash;
}

ml_value_t *ml_method_call(ml_value_t *Value, int Count, ml_value_t **Args) {
	ml_method_t *Method = (ml_method_t *)Value;
	ml_method_node_t *Node = ml_method_find(Method->Root, Count, Args);
	if (Node->Callback) {
		return (Node->Callback)(Node->Data, Count, Args);
	} else {
		int Length = 4;
		for (int I = 0; I < Count; ++I) Length += strlen(Args[I]->Type->Name) + 2;
		char *Types = snew(Length);
		char *P = Types;
		for (int I = 0; I < Count; ++I) P = stpcpy(stpcpy(P, Args[I]->Type->Name), ", ");
		P[-2] = 0;
		return ml_error("MethodError", "no matching method found for %s(%s)", Method->Name, Types);
	}
}

ml_type_t MLMethodT[1] = {{
	MLFunctionT, "method",
	ml_method_hash,
	ml_method_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

static int NumMethods = 0;
static int MaxMethods = 2;
static ml_method_t **Methods;

ml_value_t *ml_method(const char *Name) {
	if (!Name) {
		ml_method_t *Method = new(ml_method_t);
		Method->Type = MLMethodT;
		Method->Name = Name;
		return (ml_value_t *)Method;
	}
	int Lo = 0, Hi = NumMethods - 1;
	while (Lo <= Hi) {
		int Mid = (Lo + Hi) / 2;
		int Cmp = strcmp(Name, Methods[Mid]->Name);
		if (Cmp < 0) {
			Hi = Mid - 1;
		} else if (Cmp > 0) {
			Lo = Mid + 1;
		} else {
			return (ml_value_t *)Methods[Mid];
		}
	}
	ml_method_t *Method = new(ml_method_t);
	Method->Type = MLMethodT;
	Method->Name = Name;
	ml_method_t **SourceMethods = Methods;
	ml_method_t **TargetMethods = Methods;
	if (++NumMethods > MaxMethods) {
		MaxMethods += 32;
		Methods = TargetMethods = anew(ml_method_t *, MaxMethods);
		for (int I = Lo; --I >= 0;) TargetMethods[I] = SourceMethods[I];
	}
	for (int I = NumMethods; I > Lo; --I) TargetMethods[I] = SourceMethods[I - 1];
	TargetMethods[Lo] = Method;
	return (ml_value_t *)Method;
}

void ml_method_by_name(const char *Name, void *Data, ml_callback_t Callback, ...) {
	ml_method_t *Method = (ml_method_t *)ml_method(Name);
	ml_method_node_t *Node = Method->Root;
	va_list Args;
	va_start(Args, Callback);
	ml_type_t *Type;
	while ((Type = va_arg(Args, ml_type_t *))) {
		ml_method_node_t **Slot = &Node->Child;
		while (Slot[0] && Slot[0]->Type != Type) Slot = &Slot[0]->Next;
		if (Slot[0]) {
			Node = Slot[0];
		} else {
			Node = Slot[0] = new(ml_method_node_t);
			Node->Type = Type;
		}

	}
	va_end(Args);
	Node->Data = Data;
	Node->Callback = Callback;
}

void ml_method_by_value(ml_value_t *Value, void *Data, ml_callback_t Callback, ...) {
	ml_method_t *Method = (ml_method_t *)Value;
	ml_method_node_t *Node = Method->Root;
	va_list Args;
	va_start(Args, Callback);
	ml_type_t *Type;
	while ((Type = va_arg(Args, ml_type_t *))) {
		ml_method_node_t **Slot = &Node->Child;
		while (Slot[0] && Slot[0]->Type != Type) Slot = &Slot[0]->Next;
		if (Slot[0]) {
			Node = Slot[0];
		} else {
			Node = Slot[0] = new(ml_method_node_t);
			Node->Type = Type;
		}

	}
	va_end(Args);
	Node->Data = Data;
	Node->Callback = Callback;
}

struct ml_reference_t {
	const ml_type_t *Type;
	ml_value_t **Address;
	ml_value_t *Value[];
};

static ml_value_t *ml_reference_deref(ml_value_t *Ref) {
	ml_reference_t *Reference = (ml_reference_t *)Ref;
	return Reference->Address[0];
}

static ml_value_t *ml_reference_assign(ml_value_t *Ref, ml_value_t *Value) {
	ml_reference_t *Reference = (ml_reference_t *)Ref;
	return Reference->Address[0] = Value;
}

ml_type_t MLReferenceT[1] = {{
	MLAnyT, "reference",
	ml_default_hash,
	ml_default_call,
	ml_reference_deref,
	ml_reference_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_reference(ml_value_t **Address) {
	ml_reference_t *Reference;
	if (Address == 0) {
		Reference = xnew(ml_reference_t, 1, ml_value_t *);
		Reference->Address = Reference->Value;
		Reference->Value[0] = MLNil;
	} else {
		Reference = new(ml_reference_t);
		Reference->Address = Address;
	}
	Reference->Type = MLReferenceT;
	return (ml_value_t *)Reference;
}

struct ml_list_t {
	const ml_type_t *Type;
	ml_list_node_t *Head, *Tail;
	int Length;
};

struct ml_list_node_t {
	ml_list_node_t *Next, *Prev;
	ml_value_t *Value;
};

int ml_list_length(ml_value_t *Value) {
	return ((ml_list_t *)Value)->Length;
}

void ml_list_to_array(ml_value_t *Value, ml_value_t **Array) {
	ml_list_t *List = (ml_list_t *)Value;
	for (ml_list_node_t *Node = List->Head; Node; Node = Node->Next) *Array++ = Node->Value;
}

static ml_value_t *ml_list_length_value(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	return ml_integer(List->Length);
}

static ml_value_t *ml_list_index(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	long Index = ((ml_integer_t *)Args[1])->Value;
	if (Index > 0) {
		for (ml_list_node_t *Node = List->Head; Node; Node = Node->Next) {
			if (--Index == 0) return ml_reference(&Node->Value);
		}
		return MLNil;
	} else {
		Index = -Index;
		for (ml_list_node_t *Node = List->Tail; Node; Node = Node->Prev) {
			if (--Index == 0) return ml_reference(&Node->Value);
		}
		return MLNil;
	}
}

static ml_value_t *ml_list_slice(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	long Index = ((ml_integer_t *)Args[1])->Value;
	long End = ((ml_integer_t *)Args[2])->Value;
	long Start = Index;
	if (Start <= 0) Start += List->Length + 1;
	if (End <= 0) End += List->Length + 1;
	if (Start <= 0 || End < Start || End > List->Length + 1) return MLNil;
	long Length = End - Start;
	ml_list_node_t *Source = 0;
	if (Index > 0) {
		for (ml_list_node_t *Node = List->Head; Node; Node = Node->Next) {
			if (--Index == 0) {
				Source = Node;
				break;
			}
		}
	} else {
		Index = -Index;
		for (ml_list_node_t *Node = List->Tail; Node; Node = Node->Prev) {
			if (--Index == 0) {
				Source = Node;
				break;
			}
		}
	}
	ml_list_t *Slice = (ml_list_t *)ml_list();
	Slice->Type = MLListT;
	Slice->Length = Length;
	ml_list_node_t **Slot = &Slice->Head, *Prev = 0, *Node = 0;
	while (--Length >= 0) {
		Node = Slot[0] = new(ml_list_node_t);
		Node->Prev = Prev;
		Node->Value = Source->Value;
		Slot = &Node->Next;
		Source = Source->Next;
		Prev = Node;
	}
	Slice->Tail = Node;
	return (ml_value_t *)Slice;
}

ml_type_t MLListT[1] = {{
	MLAnyT, "list",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_list() {
	ml_list_t *List = new(ml_list_t);
	List->Type = MLListT;
	return (ml_value_t *)List;
}

int ml_is_list(ml_value_t *Value) {
	return Value->Type == MLListT;
}

void ml_list_append(ml_value_t *List0, ml_value_t *Value) {
	ml_list_t *List = (ml_list_t *)List0;
	ml_list_node_t *Node = new(ml_list_node_t);
	Node->Value = Value;
	Node->Prev = List->Tail;
	if (List->Tail) {
		List->Tail->Next = Node;
	} else {
		List->Head = Node;
	}
	List->Tail = Node;
	List->Length += 1;
}

int ml_list_foreach(ml_value_t *Value, void *Data, int (*callback)(ml_value_t *, void *)) {
	ml_list_t *List = (ml_list_t *)Value;
	for (ml_list_node_t *Node = List->Head; Node; Node = Node->Next) {
		if (callback(Node->Value, Data)) return 1;
	}
	return 0;
}

static ml_value_t *ml_list_new(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = new(ml_list_t);
	List->Type = MLListT;
	ml_list_node_t **Slot = &List->Head;
	ml_list_node_t *Prev = NULL;
	for (int I = 0; I < Count; ++I) {
		ml_list_node_t *Node = Slot[0] = new(ml_list_node_t);
		Node->Value = Args[I];
		Node->Prev = Prev;
		Prev = Node;
		Slot = &Node->Next;
	}
	List->Tail = Prev;
	List->Length = Count;
	return (ml_value_t *)List;
}

static ml_function_t ListNew[1] = {{MLFunctionT, ml_list_new, NULL}};

struct ml_tree_t {
	const ml_type_t *Type;
	ml_tree_node_t *Root;
	int Size;
};

struct ml_tree_node_t {
	ml_tree_node_t *Left, *Right;
	ml_value_t *Key;
	ml_value_t *Value;
	long Hash;
	int Depth;
};

ml_value_t *ml_tree_search(ml_tree_t *Tree, ml_value_t *Key) {
	ml_tree_node_t *Node = Tree->Root;
	long Hash = ml_hash(Key);
	while (Node) {
		int Compare;
		if (Hash < Node->Hash) {
			Compare = -1;
		} else if (Hash > Node->Hash) {
			Compare = 1;
		} else {
			ml_value_t *Args[2] = {Key, Node->Key};
			ml_value_t *Result = ml_method_call(CompareMethod, 2, Args);
			if (Result->Type == MLIntegerT) {
				Compare = ((ml_integer_t *)Result)->Value;
			} else if (Result->Type == MLRealT) {
				Compare = ((ml_real_t *)Result)->Value;
			} else {
				return ml_error("CompareError", "comparison must return number");
			}
		}
		if (!Compare) {
			return Node->Value;
		} else {
			Node = Compare < 0 ? Node->Left : Node->Right;
		}
	}
	return MLNil;
}

static int ml_tree_balance(ml_tree_node_t *Node) {
	int Delta = 0;
	if (Node->Left) Delta = Node->Left->Depth;
	if (Node->Right) Delta -= Node->Right->Depth;
	return Delta;
}

static void ml_tree_update_depth(ml_tree_node_t *Node) {
	int Depth = 0;
	if (Node->Left) Depth = Node->Left->Depth;
	if (Node->Right && Depth < Node->Right->Depth) Depth = Node->Right->Depth;
	Node->Depth = Depth + 1;
}

static void ml_tree_rotate_left(ml_tree_node_t **Slot) {
	ml_tree_node_t *Ch = Slot[0]->Right;
	Slot[0]->Right = Slot[0]->Right->Left;
	Ch->Left = Slot[0];
	ml_tree_update_depth(Slot[0]);
	Slot[0] = Ch;
	ml_tree_update_depth(Slot[0]);
}

static void ml_tree_rotate_right(ml_tree_node_t **Slot) {
	ml_tree_node_t *Ch = Slot[0]->Left;
	Slot[0]->Left = Slot[0]->Left->Right;
	Ch->Right = Slot[0];
	ml_tree_update_depth(Slot[0]);
	Slot[0] = Ch;
	ml_tree_update_depth(Slot[0]);
}

static void ml_tree_rebalance(ml_tree_node_t **Slot) {
	int Delta = ml_tree_balance(Slot[0]);
	if (Delta == 2) {
		if (ml_tree_balance(Slot[0]->Left) < 0) ml_tree_rotate_left(&Slot[0]->Left);
		ml_tree_rotate_right(Slot);
	} else if (Delta == -2) {
		if (ml_tree_balance(Slot[0]->Right) > 0) ml_tree_rotate_right(&Slot[0]->Right);
		ml_tree_rotate_left(Slot);
	}
}

static ml_value_t *ml_tree_insert_internal(ml_tree_t *Tree, ml_tree_node_t **Slot, long Hash, ml_value_t *Key, ml_value_t *Value) {
	if (!Slot[0]) {
		++Tree->Size;
		ml_tree_node_t *Node = Slot[0] = new(ml_tree_node_t);
		Node->Depth = 1;
		Node->Hash = Hash;
		Node->Key = Key;
		Node->Value = Value;
		return NULL;
	}
	int Compare;
	if (Hash < Slot[0]->Hash) {
		Compare = -1;
	} else if (Hash > Slot[0]->Hash) {
		Compare = 1;
	} else {
		ml_value_t *Args[2] = {Key, Slot[0]->Key};
		ml_value_t *Result = ml_method_call(CompareMethod, 2, Args);
		if (Result->Type == MLIntegerT) {
			Compare = ((ml_integer_t *)Result)->Value;
		} else if (Result->Type == MLRealT) {
			Compare = ((ml_real_t *)Result)->Value;
		} else {
			return ml_error("CompareError", "comparison must return number");
		}
	}
	if (!Compare) {
		ml_value_t *Old = Slot[0]->Value;
		Slot[0]->Value = Value;
		return Old;
	} else {
		ml_value_t *Old = ml_tree_insert_internal(Tree, Compare < 0 ? &Slot[0]->Left : &Slot[0]->Right, Hash, Key, Value);
		ml_tree_rebalance(Slot);
		ml_tree_update_depth(Slot[0]);
		return Old;
	}
}

ml_value_t *ml_tree_insert(ml_tree_t *Tree, ml_value_t *Key, ml_value_t *Value) {
	return ml_tree_insert_internal(Tree, &Tree->Root, ml_hash(Key), Key, Value);
}

static void ml_tree_remove_depth_helper(ml_tree_node_t *Node) {
	if (Node) {
		ml_tree_remove_depth_helper(Node->Right);
		ml_tree_update_depth(Node);
	}
}

static ml_value_t *ml_tree_remove_internal(ml_tree_t *Tree, ml_tree_node_t **Slot, long Hash, ml_value_t *Key) {
	if (!Slot[0]) return MLNil;
	int Compare;
	if (Hash < Slot[0]->Hash) {
		Compare = -1;
	} else if (Hash > Slot[0]->Hash) {
		Compare = 1;
	} else {
		ml_value_t *Args[2] = {Key, Slot[0]->Key};
		ml_value_t *Result = ml_method_call(CompareMethod, 2, Args);
		if (Result->Type == MLIntegerT) {
			Compare = ((ml_integer_t *)Result)->Value;
		} else if (Result->Type == MLRealT) {
			Compare = ((ml_real_t *)Result)->Value;
		} else {
			return ml_error("CompareError", "comparison must return number");
		}
	}
	ml_value_t *Removed = MLNil;
	if (!Compare) {
		--Tree->Size;
		Removed = Slot[0]->Value;
		if (Slot[0]->Left && Slot[0]->Right) {
			ml_tree_node_t **Y = &Slot[0]->Left;
			while (Y[0]->Right) Y = &Y[0]->Right;
			Slot[0]->Key = Y[0]->Key;
			Slot[0]->Hash = Y[0]->Hash;
			Slot[0]->Value = Y[0]->Value;
			Y[0] = Y[0]->Left;
			ml_tree_remove_depth_helper(Slot[0]->Left);
		} else if (Slot[0]->Left) {
			Slot[0] = Slot[0]->Left;
		} else if (Slot[0]->Right) {
			Slot[0] = Slot[0]->Right;
		} else {
			Slot[0] = 0;
		}
	} else {
		Removed = ml_tree_remove_internal(Tree, Compare < 0 ? &Slot[0]->Left : &Slot[0]->Right, Hash, Key);
	}
	if (Slot[0]) {
		ml_tree_update_depth(Slot[0]);
		ml_tree_rebalance(Slot);
	}
	return Removed;
}

ml_value_t *ml_tree_remove(ml_tree_t *Tree, ml_value_t *Key) {
	return ml_tree_remove_internal(Tree, &Tree->Root, ml_hash(Key), Key);
}

static ml_value_t *ml_tree_index_get(void *Data, const char *Name) {
	ml_tree_t *Tree = (ml_tree_t *)Data;
	ml_value_t *Key = (ml_value_t *)Name;
	return ml_tree_search(Tree, Key);
}

static ml_value_t *ml_tree_index_set(void *Data, const char *Name, ml_value_t *Value) {
	ml_tree_t *Tree = (ml_tree_t *)Data;
	ml_value_t *Key = (ml_value_t *)Name;
	ml_tree_insert(Tree, Key, Value);
	return Value;
}

static ml_value_t *ml_tree_size(void *Data, int Count, ml_value_t **Args) {
	ml_tree_t *Tree = (ml_tree_t *)Args[0];
	return ml_integer(Tree->Size);
}

static ml_value_t *ml_tree_index(void *Data, int Count, ml_value_t **Args) {
	ml_tree_t *Tree = (ml_tree_t *)Args[0];
	if (Count < 1) return MLNil;
	ml_value_t *Key = Args[1];
	return ml_property(Tree, (const char *)Key, ml_tree_index_get, ml_tree_index_set, NULL, NULL);
}

static ml_value_t *ml_tree_delete(void *Data, int Count, ml_value_t **Args) {
	if (Count < 2) return MLNil;
	ml_tree_t *Tree = (ml_tree_t *)Args[0];
	ml_value_t *Key = Args[1];
	return ml_tree_remove(Tree, Key);
}

ml_type_t MLTreeT[1] = {{
	MLAnyT, "tree",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_tree() {
	ml_tree_t *Tree = new(ml_tree_t);
	Tree->Type = MLTreeT;
	return (ml_value_t *)Tree;
}

int ml_is_tree(ml_value_t *Value) {
	return Value->Type == MLTreeT;
}

static int ml_tree_node_foreach(ml_tree_node_t *Node, void *Data, int (*callback)(ml_value_t *, ml_value_t *, void *)) {
	if (callback(Node->Key, Node->Value, Data)) return 1;
	if (Node->Left && ml_tree_node_foreach(Node->Left, Data, callback)) return 1;
	if (Node->Right && ml_tree_node_foreach(Node->Right, Data, callback)) return 1;
	return 0;
}

int ml_tree_foreach(ml_value_t *Value, void *Data, int (*callback)(ml_value_t *, ml_value_t *, void *)) {
	ml_tree_t *Tree = (ml_tree_t *)Value;
	return Tree->Root ? ml_tree_node_foreach(Tree->Root, Data, callback) : 0;
}

static ml_value_t *ml_tree_new(void *Data, int Count, ml_value_t **Args) {
	ml_tree_t *Tree = new(ml_tree_t);
	Tree->Type = MLTreeT;
	for (int I = 0; I < Count; I += 2) ml_tree_insert(Tree, Args[I], Args[I + 1]);
	return (ml_value_t *)Tree;
}

static ml_function_t TreeNew[1] = {{MLFunctionT, ml_tree_new, NULL}};

struct ml_property_t {
	const ml_type_t *Type;
	void *Data;
	const char *Name;
	ml_getter_t Get;
	ml_setter_t Set;
	ml_getter_t Next;
	ml_getter_t Key;
};

static ml_value_t *ml_property_deref(ml_value_t *Ref) {
	ml_property_t *Property = (ml_property_t *)Ref;
	return (Property->Get)(Property->Data, Property->Name);
}

static ml_value_t *ml_property_assign(ml_value_t *Ref, ml_value_t *Value) {
	ml_property_t *Property = (ml_property_t *)Ref;
	if (Property->Set) {
		return (Property->Set)(Property->Data, Property->Name, Value);
	} else {
		return ml_error("TypeError", "value is not assignable");
	}
}

static ml_value_t *ml_property_next(ml_value_t *Iter) {
	ml_property_t *Property = (ml_property_t *)Iter;
	if (Property->Next) {
		return (Property->Next)(Property->Data, "next");
	} else {
		return ml_error("TypeError", "value is not iterable");
	}
}

static ml_value_t *ml_property_key(ml_value_t *Iter) {
	ml_property_t *Property = (ml_property_t *)Iter;
	if (Property->Key) {
		return (Property->Key)(Property->Data, "next");
	} else {
		return ml_error("TypeError", "value is not iterable");
	}
}

ml_type_t MLPropertyT[1] = {{
	MLAnyT, "property",
	ml_default_hash,
	ml_default_call,
	ml_property_deref,
	ml_property_assign,
	ml_property_next,
	ml_property_key
}};

ml_value_t *ml_property(void *Data, const char *Name, ml_getter_t Get, ml_setter_t Set, ml_getter_t Next, ml_getter_t Key) {
	ml_property_t *Property = new(ml_property_t);
	Property->Type = MLPropertyT;
	Property->Data = Data;
	Property->Name = Name;
	Property->Get = Get;
	Property->Set = Set;
	Property->Next = Next;
	Property->Key = Key;
	return (ml_value_t *)Property;
}

typedef struct ml_closure_info_t {
	ml_inst_t *Entry;
	int FrameSize;
	int NumParams, NumUpValues;
	unsigned char Hash[SHA256_BLOCK_SIZE];
} ml_closure_info_t;

struct ml_closure_t {
	const ml_type_t *Type;
	ml_closure_info_t *Info;
	ml_value_t *UpValues[];
};

void ml_closure_hash(ml_value_t *Value, unsigned char Hash[SHA256_BLOCK_SIZE]) {
	ml_closure_t *Closure = (ml_closure_t *)Value;
	memcpy(Hash, Closure->Info->Hash, SHA256_BLOCK_SIZE);
}

struct ml_frame_t {
	ml_inst_t *OnError;
	ml_value_t **UpValues;
	ml_value_t **Top;
	ml_value_t *Stack[];
};

typedef union {
	ml_inst_t *Inst;
	int Index;
	int Count;
	ml_value_t *Value;
	const char *Name;
	ml_closure_info_t *ClosureInfo;
	ra_schema_field_t *RaField;
} ml_param_t;

struct ml_inst_t {
	ml_inst_t *(*run)(ml_inst_t *Inst, ml_frame_t *Frame);
	ml_source_t Source;
	ml_param_t Params[];
};

static ml_value_t *ml_closure_call(ml_value_t *Value, int Count, ml_value_t **Args) {
	ml_closure_t *Closure = (ml_closure_t *)Value;
	ml_closure_info_t *Info = Closure->Info;
	ml_frame_t *Frame = xnew(ml_frame_t, Info->FrameSize, ml_value_t *);
	int NumParams = Info->NumParams;
	int VarArgs = 0;
	if (NumParams < 0) {
		VarArgs = 1;
		NumParams = ~NumParams;
	}
	if (Count > NumParams) Count = NumParams;
	for (int I = 0; I < Count; ++I) {
		ml_reference_t *Local = xnew(ml_reference_t, 1, ml_value_t *);
		Local->Type = MLReferenceT;
		Local->Address = Local->Value;
		ml_value_t *Value = Args[I];
		Local->Value[0] = Value->Type->deref(Value);
		Frame->Stack[I] = (ml_value_t *)Local;
	}
	for (int I = Count; I < NumParams; ++I) {
		ml_reference_t *Local = xnew(ml_reference_t, 1, ml_value_t *);
		Local->Type = MLReferenceT;
		Local->Address = Local->Value;
		Local->Value[0] = MLNil;
		Frame->Stack[I] = (ml_value_t *)Local;
	}
	if (VarArgs) {
		ml_reference_t *Local = xnew(ml_reference_t, 1, ml_value_t *);
		Local->Type = MLReferenceT;
		Local->Address = Local->Value;
		ml_list_t *Rest = new(ml_list_t);
		int Length = 0;
		ml_list_node_t **Next = &Rest->Head;
		ml_list_node_t *Prev = Next[0] = 0;
		for (int I = NumParams; I < Count; ++I) {
			ml_list_node_t *Node = new(ml_list_node_t);
			Node->Prev = Prev;
			Next[0] = Prev = Node;
			Next = &Node->Next;
			++Length;
		}
		Rest->Tail = Prev;
		Rest->Length = Length;
		Local->Value[0] = (ml_value_t *)Rest;
		Frame->Stack[NumParams] = (ml_value_t *)Local;
	}
	Frame->Top = Frame->Stack + NumParams + VarArgs;
	Frame->OnError = NULL;
	Frame->UpValues = Closure->UpValues;
	ml_inst_t *Inst = Closure->Info->Entry;
	while (Inst) Inst = Inst->run(Inst, Frame);
	ml_value_t *Result = Frame->Top[-1];
	return Result->Type->deref(Result);
}



ml_type_t MLClosureT[1] = {{
	MLFunctionT, "closure",
	ml_default_hash,
	ml_closure_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

struct ml_error_t {
	const ml_type_t *Type;
	const char *Error;
	const char *Message;
	ml_source_t Trace[MAX_TRACE];
};

ml_type_t MLErrorT[1] = {{
	MLAnyT, "error",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_type_t MLErrorValueT[1] = {{
	MLErrorT, "error_value",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

ml_value_t *ml_error(const char *Error, const char *Format, ...) {
	va_list Args;
	va_start(Args, Format);
	char *Message;
	vasprintf(&Message, Format, Args);
	va_end(Args);
	ml_error_t *Value = new(ml_error_t);
	Value->Type = MLErrorT;
	Value->Error = Error;
	Value->Message = Message;
	memset(Value->Trace, 0, sizeof(Value->Trace));
	return (ml_value_t *)Value;
}

static void ml_error_trace_add(ml_value_t *Value, ml_source_t Source) {
	ml_error_t *Error = (ml_error_t *)Value;
	for (int I = 0; I < MAX_TRACE; ++I) if (!Error->Trace[I].Name) {
		Error->Trace[I] = Source;
		return;
	}
}

int ml_is_error(ml_value_t *Value) {
	return Value->Type == MLErrorT;
}

const char *ml_error_type(ml_value_t *Value) {
	return ((ml_error_t *)Value)->Error;
}

const char *ml_error_message(ml_value_t *Value) {
	return ((ml_error_t *)Value)->Message;
}

static ml_value_t *ml_error_type_value(void *Data, int Count, ml_value_t **Args) {
	return ml_string(((ml_error_t *)Args[0])->Error, -1);
}

static ml_value_t *ml_error_message_value(void *Data, int Count, ml_value_t **Args) {
	return ml_string(((ml_error_t *)Args[0])->Message, -1);
}

int ml_error_trace(ml_value_t *Value, int Level, const char **Source, int *Line) {
	ml_error_t *Error = (ml_error_t *)Value;
	if (Level >= MAX_TRACE) return 0;
	if (!Error->Trace[Level].Name) return 0;
	Source[0] = Error->Trace[Level].Name;
	Line[0] = Error->Trace[Level].Line;
	return 1;
}

struct ml_stringbuffer_node_t {
	ml_stringbuffer_node_t *Next;
	char Chars[ML_STRINGBUFFER_NODE_SIZE];
};

static ml_stringbuffer_node_t *Cache = NULL;
static GC_descr StringBufferDesc = 0;
static pthread_mutex_t CacheMutex[1] = {PTHREAD_MUTEX_DEFAULT};

ssize_t ml_stringbuffer_add(ml_stringbuffer_t *Buffer, const char *String, size_t Length) {
	size_t Remaining = Length;
	ml_stringbuffer_node_t **Slot = &Buffer->Nodes;
	ml_stringbuffer_node_t *Node = Buffer->Nodes;
	if (Node) {
		while (Node->Next) Node = Node->Next;
		Slot = &Node->Next;
	}
	while (Buffer->Space < Remaining) {
		memcpy(Node->Chars + ML_STRINGBUFFER_NODE_SIZE - Buffer->Space, String, Buffer->Space);
		String += Buffer->Space;
		Remaining -= Buffer->Space;
		ml_stringbuffer_node_t *Next;
		pthread_mutex_lock(CacheMutex);
		if (Cache) {
			Next = Cache;
			Cache = Cache->Next;
			Next->Next = NULL;
		} else {
			Next = (ml_stringbuffer_node_t *)GC_malloc_explicitly_typed(sizeof(ml_stringbuffer_node_t), StringBufferDesc);
			//printf("Allocating stringbuffer: %d in total\n", ++NumStringBuffers);
		}
		pthread_mutex_unlock(CacheMutex);
		Node = Slot[0] = Next;
		Slot = &Node->Next;
		Buffer->Space = ML_STRINGBUFFER_NODE_SIZE;
	}
	memcpy(Node->Chars + ML_STRINGBUFFER_NODE_SIZE - Buffer->Space, String, Remaining);
	Buffer->Space -= Remaining;
	Buffer->Length += Length;
	return Length;
}

ssize_t ml_stringbuffer_addf(ml_stringbuffer_t *Buffer, const char *Format, ...) {
	char *String;
	va_list Args;
	va_start(Args, Format);
	size_t Length = vasprintf(&String, Format, Args);
	va_end(Args);
	return ml_stringbuffer_add(Buffer, String, Length);
}

char *ml_stringbuffer_get(ml_stringbuffer_t *Buffer) {
	char *String = snew(Buffer->Length + 1);
	if (Buffer->Length == 0) {
		String[0] = 0;
	} else {
		char *P = String;
		ml_stringbuffer_node_t *Node = Buffer->Nodes;
		while (Node->Next) {
			memcpy(P, Node->Chars, ML_STRINGBUFFER_NODE_SIZE);
			P += ML_STRINGBUFFER_NODE_SIZE;
			Node = Node->Next;
		}
		memcpy(P, Node->Chars, ML_STRINGBUFFER_NODE_SIZE - Buffer->Space);
		P += ML_STRINGBUFFER_NODE_SIZE - Buffer->Space;
		*P++ = 0;
		pthread_mutex_lock(CacheMutex);
		ml_stringbuffer_node_t **Slot = &Cache;
		while (Slot[0]) Slot = &Slot[0]->Next;
		Slot[0] = Buffer->Nodes;
		pthread_mutex_unlock(CacheMutex);
		Buffer->Nodes = NULL;
		Buffer->Length = Buffer->Space = 0;
	}
	return String;
}

ml_type_t MLStringBufferT[1] = {{
	MLAnyT, "stringbuffer",
	ml_default_hash,
	ml_default_call,
	ml_default_deref,
	ml_default_assign,
	ml_default_next,
	ml_default_key
}};

int ml_stringbuffer_foreach(ml_stringbuffer_t *Buffer, void *Data, int (*callback)(const char *, size_t, void *)) {
	ml_stringbuffer_node_t *Node = Buffer->Nodes;
	if (!Node) return 0;
	while (Node->Next) {
		if (callback(Node->Chars, ML_STRINGBUFFER_NODE_SIZE, Data)) return 1;
		Node = Node->Next;
	}
	return callback(Node->Chars, ML_STRINGBUFFER_NODE_SIZE - Buffer->Space, Data);
}

ml_value_t *stringify_nil(void *Data, int Count, ml_value_t **Args) {
	return MLNil;
}

ml_value_t *stringify_integer(void *Data, int Count, ml_value_t **Args) {
	ml_stringbuffer_t *Buffer = (ml_stringbuffer_t *)Args[0];
	ml_stringbuffer_addf(Buffer, "%d", ml_integer_value(Args[1]));
	return MLSome;
}

ml_value_t *stringify_real(void *Data, int Count, ml_value_t **Args) {
	ml_stringbuffer_t *Buffer = (ml_stringbuffer_t *)Args[0];
	ml_stringbuffer_addf(Buffer, "%f", ml_real_value(Args[1]));
	return MLSome;
}

ml_value_t *stringify_string(void *Data, int Count, ml_value_t **Args) {
	ml_stringbuffer_t *Buffer = (ml_stringbuffer_t *)Args[0];
	ml_stringbuffer_add(Buffer, ml_string_value(Args[1]), ml_string_length(Args[1]));
	return ml_string_length(Args[1]) ? MLSome : MLNil;
}

ml_value_t *stringify_method(void *Data, int Count, ml_value_t **Args) {
	ml_stringbuffer_t *Buffer = (ml_stringbuffer_t *)Args[0];
	ml_method_t *Method = (ml_method_t *)Args[1];
	ml_stringbuffer_add(Buffer, Method->Name, strlen(Method->Name));
	return MLSome;
}

ml_value_t *stringify_list(void *Data, int Count, ml_value_t **Args) {
	ml_stringbuffer_t *Buffer = (ml_stringbuffer_t *)Args[0];
	ml_list_node_t *Node = ((ml_list_t *)Args[1])->Head;
	if (Node) {
		ml_inline(AppendMethod, 2, Buffer, Node->Value);
		while ((Node = Node->Next)) {
			ml_stringbuffer_add(Buffer, " ", 1);
			ml_inline(AppendMethod, 2, Buffer, Node->Value);
		}
		return MLSome;
	} else {
		return MLNil;
	}
}

typedef struct {ml_stringbuffer_t *Buffer; int Space;} ml_stringify_context_t;

static int stringify_tree_value(ml_value_t *Key, ml_value_t *Value, ml_stringify_context_t *Ctx) {
	if (Ctx->Space) ml_stringbuffer_add(Ctx->Buffer, " ", 1);
	ml_inline(AppendMethod, 2, Ctx->Buffer, Key);
	if (Value != MLNil) {
		ml_stringbuffer_add(Ctx->Buffer, "=", 1);
		ml_inline(AppendMethod, 2, Ctx->Buffer, Value);
	}
	Ctx->Space = 1;
	return 0;
}

ml_value_t *stringify_tree(void *Data, int Count, ml_value_t **Args) {
	ml_stringify_context_t Ctx[1] = {(ml_stringbuffer_t *)Args[0], 0};
	ml_stringbuffer_t *Buffer = (ml_stringbuffer_t *)Args[0];
	ml_tree_foreach(Args[1], Ctx, (void *)stringify_tree_value);
	return ((ml_tree_t *)Args[1])->Size ? MLSome : MLNil;
}

#define ml_arith_method_integer(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _integer(void *Data, int Count, ml_value_t **Args) { \
		ml_integer_t *IntegerA = (ml_integer_t *)Args[0]; \
		return ml_integer(SYMBOL(IntegerA->Value)); \
	}

#define ml_arith_method_integer_integer(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _integer_integer(void *Data, int Count, ml_value_t **Args) { \
		ml_integer_t *IntegerA = (ml_integer_t *)Args[0]; \
		ml_integer_t *IntegerB = (ml_integer_t *)Args[1]; \
		return ml_integer(IntegerA->Value SYMBOL IntegerB->Value); \
	}

#define ml_arith_method_real(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _real(void *Data, int Count, ml_value_t **Args) { \
		ml_real_t *RealA = (ml_real_t *)Args[0]; \
		return ml_real(SYMBOL(RealA->Value)); \
	}

#define ml_arith_method_real_real(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _real_real(void *Data, int Count, ml_value_t **Args) { \
		ml_real_t *RealA = (ml_real_t *)Args[0]; \
		ml_real_t *RealB = (ml_real_t *)Args[1]; \
		return ml_real(RealA->Value SYMBOL RealB->Value); \
	}

#define ml_arith_method_real_integer(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _real_integer(void *Data, int Count, ml_value_t **Args) { \
		ml_real_t *RealA = (ml_real_t *)Args[0]; \
		ml_integer_t *IntegerB = (ml_integer_t *)Args[1]; \
		return ml_real(RealA->Value SYMBOL IntegerB->Value); \
	}

#define ml_arith_method_integer_real(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _integer_real(void *Data, int Count, ml_value_t **Args) { \
		ml_integer_t *IntegerA = (ml_integer_t *)Args[0]; \
		ml_real_t *RealB = (ml_real_t *)Args[1]; \
		return ml_real(IntegerA->Value SYMBOL RealB->Value); \
	}

#define ml_arith_method_number(NAME, SYMBOL) \
	ml_arith_method_integer(NAME, SYMBOL) \
	ml_arith_method_real(NAME, SYMBOL)

#define ml_arith_method_number_number(NAME, SYMBOL) \
	ml_arith_method_integer_integer(NAME, SYMBOL) \
	ml_arith_method_real_real(NAME, SYMBOL) \
	ml_arith_method_real_integer(NAME, SYMBOL) \
	ml_arith_method_integer_real(NAME, SYMBOL)

ml_arith_method_number(neg, -)
ml_arith_method_number_number(add, +)
ml_arith_method_number_number(sub, -)
ml_arith_method_number_number(mul, *)
ml_arith_method_number_number(div, /)

ml_arith_method_integer_integer(mod, %)

#define ml_comp_method_integer_integer(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _integer_integer(void *Data, int Count, ml_value_t **Args) { \
		ml_integer_t *IntegerA = (ml_integer_t *)Args[0]; \
		ml_integer_t *IntegerB = (ml_integer_t *)Args[1]; \
		return IntegerA->Value SYMBOL IntegerB->Value ? Args[1] : MLNil; \
	}

#define ml_comp_method_real_real(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _real_real(void *Data, int Count, ml_value_t **Args) { \
		ml_real_t *RealA = (ml_real_t *)Args[0]; \
		ml_real_t *RealB = (ml_real_t *)Args[1]; \
		return RealA->Value SYMBOL RealB->Value ? Args[1] : MLNil; \
	}

#define ml_comp_method_real_integer(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _real_integer(void *Data, int Count, ml_value_t **Args) { \
		ml_real_t *RealA = (ml_real_t *)Args[0]; \
		ml_integer_t *IntegerB = (ml_integer_t *)Args[1]; \
		return RealA->Value SYMBOL IntegerB->Value ? Args[1] : MLNil; \
	}

#define ml_comp_method_integer_real(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _integer_real(void *Data, int Count, ml_value_t **Args) { \
		ml_integer_t *IntegerA = (ml_integer_t *)Args[0]; \
		ml_real_t *RealB = (ml_real_t *)Args[1]; \
		return IntegerA->Value SYMBOL RealB->Value ? Args[1] : MLNil; \
	}

#define ml_comp_method_number_number(NAME, SYMBOL) \
	ml_comp_method_integer_integer(NAME, SYMBOL) \
	ml_comp_method_real_real(NAME, SYMBOL) \
	ml_comp_method_real_integer(NAME, SYMBOL) \
	ml_comp_method_integer_real(NAME, SYMBOL)

ml_comp_method_number_number(eq, ==)
ml_comp_method_number_number(neq, !=)
ml_comp_method_number_number(les, <)
ml_comp_method_number_number(gre, >)
ml_comp_method_number_number(leq, <=)
ml_comp_method_number_number(geq, >=)

static ml_integer_t One[1] = {{MLIntegerT, 1}};
static ml_integer_t NegOne[1] = {{MLIntegerT, -1}};
static ml_integer_t Zero[1] = {{MLIntegerT, 0}};

static ml_value_t *ml_compare_integer_integer(void *Data, int Count, ml_value_t **Args) {
	ml_integer_t *IntegerA = (ml_integer_t *)Args[0];
	ml_integer_t *IntegerB = (ml_integer_t *)Args[1];
	if (IntegerA->Value < IntegerB->Value) return (ml_value_t *)NegOne;
	if (IntegerA->Value > IntegerB->Value) return (ml_value_t *)One;
	return (ml_value_t *)Zero;
}

static ml_value_t *ml_compare_real_integer(void *Data, int Count, ml_value_t **Args) {
	ml_real_t *RealA = (ml_real_t *)Args[0];
	ml_integer_t *IntegerB = (ml_integer_t *)Args[1];
	if (RealA->Value < IntegerB->Value) return (ml_value_t *)NegOne;
	if (RealA->Value > IntegerB->Value) return (ml_value_t *)One;
	return (ml_value_t *)Zero;
}

static ml_value_t *ml_compare_integer_real(void *Data, int Count, ml_value_t **Args) {
	ml_integer_t *IntegerA = (ml_integer_t *)Args[0];
	ml_real_t *RealB = (ml_real_t *)Args[1];
	if (IntegerA->Value < RealB->Value) return (ml_value_t *)NegOne;
	if (IntegerA->Value > RealB->Value) return (ml_value_t *)One;
	return (ml_value_t *)Zero;
}

static ml_value_t *ml_compare_real_real(void *Data, int Count, ml_value_t **Args) {
	ml_real_t *RealA = (ml_real_t *)Args[0];
	ml_real_t *RealB = (ml_real_t *)Args[1];
	if (RealA->Value < RealB->Value) return (ml_value_t *)NegOne;
	if (RealA->Value > RealB->Value) return (ml_value_t *)One;
	return (ml_value_t *)Zero;
}

typedef struct ml_integer_range_t {
	const ml_type_t *Type;
	ml_integer_t *Current;
	long Step, Limit;
} ml_integer_range_t;

static ml_value_t *ml_integer_range_deref(ml_value_t *Ref) {
	ml_integer_range_t *Range = (ml_integer_range_t *)Ref;
	return (ml_value_t *)Range->Current;
}

static ml_value_t *ml_integer_range_next(ml_value_t *Ref) {
	ml_integer_range_t *Range = (ml_integer_range_t *)Ref;
	if (Range->Current->Value >= Range->Limit) {
		return MLNil;
	} else {
		Range->Current = (ml_integer_t *)ml_integer(Range->Current->Value + Range->Step);
		return Ref;
	}
}

ml_type_t MLIntegerIterT[1] = {{
	MLAnyT, "integer-range",
	ml_default_hash,
	ml_default_call,
	ml_integer_range_deref,
	ml_default_assign,
	ml_integer_range_next,
	ml_default_key
}};

static ml_value_t *ml_range_integer_integer(void *Data, int Count, ml_value_t **Args) {
	ml_integer_t *IntegerA = (ml_integer_t *)Args[0];
	ml_integer_t *IntegerB = (ml_integer_t *)Args[1];
	ml_integer_range_t *Range = new(ml_integer_range_t);
	Range->Type = MLIntegerIterT;
	Range->Current = IntegerA;
	Range->Limit = IntegerB->Value;
	Range->Step = 1;
	return (ml_value_t *)Range;
}

#define ml_methods_add_number_number(NAME, SYMBOL) \
	ml_method_by_name(#SYMBOL, NULL, ml_ ## NAME ## _integer_integer, MLIntegerT, MLIntegerT, NULL); \
	ml_method_by_name(#SYMBOL, NULL, ml_ ## NAME ## _real_real, MLRealT, MLRealT, NULL); \
	ml_method_by_name(#SYMBOL, NULL, ml_ ## NAME ## _real_integer, MLRealT, MLIntegerT, NULL); \
	ml_method_by_name(#SYMBOL, NULL, ml_ ## NAME ## _integer_real, MLIntegerT, MLRealT, NULL)

static ml_value_t *ml_integer_to_string(void *Data, int Count, ml_value_t **Args) {
	ml_integer_t *Integer = (ml_integer_t *)Args[0];
	ml_string_t *String = new(ml_string_t);
	String->Type = MLStringT;
	String->Length = asprintf((char **)&String->Value, "%ld", Integer->Value);
	return (ml_value_t *)String;
}

static ml_value_t *ml_real_to_string(void *Data, int Count, ml_value_t **Args) {
	ml_real_t *Real = (ml_real_t *)Args[0];
	ml_string_t *String = new(ml_string_t);
	String->Type = MLStringT;
	String->Length = asprintf((char **)&String->Value, "%f", Real->Value);
	return (ml_value_t *)String;
}

static ml_value_t *ml_identity(void *Data, int Count, ml_value_t **Args) {
	return Args[0];
}

static ml_value_t *ml_compare_string_string(void *Data, int Count, ml_value_t **Args) {
	ml_string_t *StringA = (ml_string_t *)Args[0];
	ml_string_t *StringB = (ml_string_t *)Args[1];
	int Compare = strcmp(StringA->Value, StringB->Value);
	if (Compare < 0) return (ml_value_t *)NegOne;
	if (Compare > 0) return (ml_value_t *)One;
	return (ml_value_t *)Zero;
}

#define ml_comp_method_string_string(NAME, SYMBOL) \
	static ml_value_t *ml_ ## NAME ## _string_string(void *Data, int Count, ml_value_t **Args) { \
		ml_string_t *StringA = (ml_string_t *)Args[0]; \
		ml_string_t *StringB = (ml_string_t *)Args[1]; \
		return strcmp(StringA->Value, StringB->Value) SYMBOL 0 ? Args[1] : MLNil; \
	}

ml_comp_method_string_string(eq, ==)
ml_comp_method_string_string(neq, !=)
ml_comp_method_string_string(les, <)
ml_comp_method_string_string(gre, >)
ml_comp_method_string_string(leq, <=)
ml_comp_method_string_string(geq, >=)

static ml_value_t *ml_compare_any_any(void *Data, int Count, ml_value_t **Args) {
	if (Args[0] < Args[1]) return ml_integer(-1);
	if (Args[0] > Args[1]) return ml_integer(1);
	return ml_integer(0);
}

typedef struct ml_list_iter_t {
	const ml_type_t *Type;
	ml_list_node_t *Node;
	long Index;
} ml_list_iter_t;

static ml_value_t *ml_list_iter_deref(ml_value_t *Ref) {
	ml_list_iter_t *Iter = (ml_list_iter_t *)Ref;
	return Iter->Node->Value;
}

static ml_value_t *ml_list_iter_assign(ml_value_t *Ref, ml_value_t *Value) {
	ml_list_iter_t *Iter = (ml_list_iter_t *)Ref;
	return Iter->Node->Value = Value;
}

static ml_value_t *ml_list_iter_next(ml_value_t *Ref) {
	ml_list_iter_t *Iter = (ml_list_iter_t *)Ref;
	if (Iter->Node->Next) {
		++Iter->Index;
		Iter->Node = Iter->Node->Next;
		return Ref;
	} else {
		return MLNil;
	}
}

static ml_value_t *ml_list_iter_key(ml_value_t *Ref) {
	ml_list_iter_t *Iter = (ml_list_iter_t *)Ref;
	return ml_integer(Iter->Index);
}

ml_type_t MLListIterT[1] = {{
	MLAnyT, "list-iterator",
	ml_default_hash,
	ml_default_call,
	ml_list_iter_deref,
	ml_list_iter_assign,
	ml_list_iter_next,
	ml_list_iter_key
}};

static ml_value_t *ml_list_values(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	if (List->Head) {
		ml_list_iter_t *Iter = new(ml_list_iter_t);
		Iter->Type = MLListIterT;
		Iter->Node = List->Head;
		Iter->Index = 1;
		return (ml_value_t *)Iter;
	} else {
		return MLNil;
	}
}

static ml_value_t *ml_list_push(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	ml_list_node_t **Slot = List->Head ? &List->Head->Prev : &List->Tail;
	ml_list_node_t *Next = List->Head;
	for (int I = Count; --I >= 1;) {
		ml_list_node_t *Node = Slot[0] = new(ml_list_node_t);
		Node->Value = Args[I];
		Node->Next = Next;
		Next = Node;
		Slot = &Node->Prev;
	}
	List->Head = Next;
	List->Length += Count - 1;
	return (ml_value_t *)List;
}

static ml_value_t *ml_list_put(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	ml_list_node_t **Slot = List->Tail ? &List->Tail->Next : &List->Head;
	ml_list_node_t *Prev = List->Tail;
	for (int I = 1; I < Count; ++I) {
		ml_list_node_t *Node = Slot[0] = new(ml_list_node_t);
		Node->Value = Args[I];
		Node->Prev = Prev;
		Prev = Node;
		Slot = &Node->Next;
	}
	List->Tail = Prev;
	List->Length += Count - 1;
	return (ml_value_t *)List;
}

static ml_value_t *ml_list_pop(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	ml_list_node_t *Node = List->Head;
	if (Node) {
		if (!(List->Head = Node->Next)) List->Tail = NULL;
		--List->Length;
		return Node->Value;
	} else {
		return MLNil;
	}
}

static ml_value_t *ml_list_pull(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	ml_list_node_t *Node = List->Tail;
	if (Node) {
		if (!(List->Tail = Node->Next)) List->Head = NULL;
		--List->Length;
		return Node->Value;
	} else {
		return MLNil;
	}
}

static ml_value_t *ml_list_add(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List1 = (ml_list_t *)Args[0];
	ml_list_t *List2 = (ml_list_t *)Args[1];
	ml_list_t *List = new(ml_list_t);
	List->Type = MLListT;
	ml_list_node_t **Slot = &List->Head;
	ml_list_node_t *Prev = NULL;
	for (ml_list_node_t *Node1 = List1->Head; Node1; Node1 = Node1->Next) {
		ml_list_node_t *Node = Slot[0] = new(ml_list_node_t);
		Node->Value = Node1->Value;
		Node->Prev = Prev;
		Prev = Node;
		Slot = &Node->Next;
	}
	for (ml_list_node_t *Node2 = List2->Head; Node2; Node2 = Node2->Next) {
		ml_list_node_t *Node = Slot[0] = new(ml_list_node_t);
		Node->Value = Node2->Value;
		Node->Prev = Prev;
		Prev = Node;
		Slot = &Node->Next;
	}
	List->Tail = Prev;
	List->Length = List1->Length + List2->Length;
	return (ml_value_t *)List;
}

static ml_value_t *ml_list_to_string(void *Data, int Count, ml_value_t **Args) {
	ml_list_t *List = (ml_list_t *)Args[0];
	if (!List->Length) return ml_string("[]", 2);
	ml_stringbuffer_t Buffer[1] = {ML_STRINGBUFFER_INIT};
	const char *Seperator = "[";
	int SeperatorLength = 1;
	for (ml_list_node_t *Node = List->Head; Node; Node = Node->Next) {
		ml_stringbuffer_add(Buffer, Seperator, SeperatorLength);
		ml_value_t *Result = ml_inline(AppendMethod, 2, Buffer, Node->Value);
		if (Result->Type == MLErrorT) return Result;
		Seperator = ", ";
		SeperatorLength = 2;
	}
	ml_stringbuffer_add(Buffer, "]", 1);
	return ml_string(ml_stringbuffer_get(Buffer), -1);
}

#define ML_TREE_MAX_DEPTH 32

typedef struct ml_tree_iter_t {
	const ml_type_t *Type;
	ml_tree_node_t *Node;
	ml_tree_node_t *Stack[ML_TREE_MAX_DEPTH];
	int Top;
} ml_tree_iter_t;

static ml_value_t *ml_tree_iter_deref(ml_value_t *Ref) {
	ml_tree_iter_t *Iter = (ml_tree_iter_t *)Ref;
	return Iter->Node->Value;
}

static ml_value_t *ml_tree_iter_assign(ml_value_t *Ref, ml_value_t *Value) {
	ml_tree_iter_t *Iter = (ml_tree_iter_t *)Ref;
	return Iter->Node->Value = Value;
}

static ml_value_t *ml_tree_iter_next(ml_value_t *Ref) {
	ml_tree_iter_t *Iter = (ml_tree_iter_t *)Ref;
	ml_tree_node_t *Node = Iter->Node;
	if (Node->Left) {
		if (Node->Right) Iter->Stack[Iter->Top++] = Node->Right;
		Iter->Node = Node->Left;
		return Ref;
	} else if (Node->Right) {
		Iter->Node = Node->Right;
		return Ref;
	} else if (Iter->Top > 0) {
		Iter->Node = Iter->Stack[--Iter->Top];
		return Ref;
	} else {
		return MLNil;
	}
}

static ml_value_t *ml_tree_iter_key(ml_value_t *Ref) {
	ml_tree_iter_t *Iter = (ml_tree_iter_t *)Ref;
	return Iter->Node->Key;
}

ml_type_t MLTreeIterT[1] = {{
	MLAnyT, "tree-iterator",
	ml_default_hash,
	ml_default_call,
	ml_tree_iter_deref,
	ml_tree_iter_assign,
	ml_tree_iter_next,
	ml_tree_iter_key
}};

static ml_value_t *ml_tree_values(void *Data, int Count, ml_value_t **Args) {
	ml_tree_t *Tree = (ml_tree_t *)Args[0];
	if (Tree->Root) {
		ml_tree_iter_t *Iter = new(ml_tree_iter_t);
		Iter->Type = MLTreeIterT;
		Iter->Node = Tree->Root;
		Iter->Top = 0;
		return (ml_value_t *)Iter;
	} else {
		return MLNil;
	}
}

static int ml_tree_add_insert(ml_value_t *Key, ml_value_t *Value, ml_tree_t *Tree) {
	ml_tree_insert(Tree, Key, Value);
	return 0;
}

static ml_value_t *ml_tree_add(void *Data, int Count, ml_value_t **Args) {
	ml_tree_t *Tree = new(ml_tree_t);
	Tree->Type = MLTreeT;
	ml_tree_foreach(Args[0], Tree, (void *)ml_tree_add_insert);
	ml_tree_foreach(Args[1], Tree, (void *)ml_tree_add_insert);
	return (ml_value_t *)Tree;
}

typedef struct ml_tree_stringer_t {
	const char *Seperator;
	ml_stringbuffer_t Buffer[1];
	int SeperatorLength;
	ml_value_t *Error;
} ml_tree_stringer_t;

static int ml_tree_stringer(ml_value_t *Key, ml_value_t *Value, ml_tree_stringer_t *Stringer) {
	ml_stringbuffer_add(Stringer->Buffer, Stringer->Seperator, Stringer->SeperatorLength);
	Stringer->Error = ml_inline(AppendMethod, 2, Stringer->Buffer, Key);
	if (Stringer->Error->Type == MLErrorT) return 1;
	ml_stringbuffer_add(Stringer->Buffer, " is ", 4);
	Stringer->Error = ml_inline(AppendMethod, 2, Stringer->Buffer, Value);
	if (Stringer->Error->Type == MLErrorT) return 1;
	Stringer->Seperator = ", ";
	Stringer->SeperatorLength = 2;
	return 0;
}

static ml_value_t *ml_tree_to_string(void *Data, int Count, ml_value_t **Args) {
	ml_tree_t *Tree = (ml_tree_t *)Args[0];
	if (!Tree->Size) return ml_string("{}", 2);
	ml_tree_stringer_t Stringer[1] = {{
		"{", {ML_STRINGBUFFER_INIT}, 1
	}};
	if (ml_tree_foreach(Args[0], Stringer, (void *)ml_tree_stringer)) return Stringer->Error;
	ml_stringbuffer_add(Stringer->Buffer, "}", 1);
	return ml_string(ml_stringbuffer_get(Stringer->Buffer), -1);
}

static ml_value_t *ml_hash_any(void *Data, int Count, ml_value_t **Args) {
	ml_value_t *Value = Args[0];
	return ml_integer(Value->Type->hash(Value));
}

static ml_value_t *ml_return_nil(void *Data, int Count, ml_value_t **Args) {
	return MLNil;
}

void ml_init() {
	Methods = anew(ml_method_t *, MaxMethods);
	CompareMethod = ml_method("?");
	ml_method_by_name("#", NULL, ml_hash_any, MLAnyT, NULL);
	ml_method_by_name("?", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name("?", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name("=", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name("=", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name("!=", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name("!=", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name("<", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name("<", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name(">", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name(">", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name("<=", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name("<=", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name(">=", NULL, ml_return_nil, MLNilT, MLAnyT, NULL);
	ml_method_by_name(">=", NULL, ml_return_nil, MLAnyT, MLNilT, NULL);
	ml_method_by_name("-", NULL, ml_neg_integer, MLIntegerT, NULL);
	ml_method_by_name("-", NULL, ml_neg_real, MLRealT, NULL);
	ml_methods_add_number_number(compare, ?);
	ml_methods_add_number_number(add, +);
	ml_methods_add_number_number(sub, -);
	ml_methods_add_number_number(mul, *);
	ml_methods_add_number_number(div, /);
	ml_methods_add_number_number(eq, =);
	ml_methods_add_number_number(neq, !=);
	ml_methods_add_number_number(les, <);
	ml_methods_add_number_number(gre, >);
	ml_methods_add_number_number(leq, <=);
	ml_methods_add_number_number(geq, >=);
	ml_method_by_name("length", NULL, ml_string_length_value, MLStringT, NULL);
	ml_method_by_name("trim", NULL, ml_string_trim, MLStringT, NULL);
	ml_method_by_name("[]", NULL, ml_string_index, MLStringT, MLIntegerT, NULL);
	ml_method_by_name("[]", NULL, ml_string_slice, MLStringT, MLIntegerT, MLIntegerT, NULL);
	ml_method_by_name("%", NULL, ml_mod_integer_integer, MLIntegerT, MLIntegerT, NULL);
	ml_method_by_name("..", NULL, ml_range_integer_integer, MLIntegerT, MLIntegerT, NULL);
	ml_method_by_name("?", NULL, ml_compare_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name("=", NULL, ml_eq_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name("!=", NULL, ml_neq_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name("<", NULL, ml_les_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name(">", NULL, ml_gre_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name("<=", NULL, ml_leq_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name(">=", NULL, ml_geq_string_string, MLStringT, MLStringT, NULL);
	ml_method_by_name("?", NULL, ml_compare_any_any, MLAnyT, MLAnyT, NULL);
	ml_method_by_name("length", NULL, ml_list_length_value, MLListT, NULL);
	ml_method_by_name("[]", NULL, ml_list_index, MLListT, MLIntegerT, NULL);
	ml_method_by_name("[]", NULL, ml_list_slice, MLListT, MLIntegerT, MLIntegerT, NULL);
	ml_method_by_name("values", NULL, ml_list_values, MLListT, NULL);
	ml_method_by_name("push", NULL, ml_list_push, MLListT, NULL);
	ml_method_by_name("put", NULL, ml_list_put, MLListT, NULL);
	ml_method_by_name("pop", NULL, ml_list_pop, MLListT, NULL);
	ml_method_by_name("pull", NULL, ml_list_pull, MLListT, NULL);
	ml_method_by_name("+", NULL, ml_list_add, MLListT, MLListT, NULL);
	ml_method_by_name("size", NULL, ml_tree_size, MLTreeT, NULL);
	ml_method_by_name("[]", NULL, ml_tree_index, MLTreeT, MLAnyT, NULL);
	ml_method_by_name("values", NULL, ml_tree_values, MLTreeT, NULL);
	ml_method_by_name("delete", NULL, ml_tree_delete, MLTreeT, NULL);
	ml_method_by_name("+", NULL, ml_tree_add, MLTreeT, MLTreeT, NULL);
	ml_method_by_name("string", NULL, ml_nil_to_string, MLNilT, NULL);
	ml_method_by_name("string", NULL, ml_some_to_string, MLSomeT, NULL);
	ml_method_by_name("string", NULL, ml_integer_to_string, MLIntegerT, NULL);
	ml_method_by_name("string", NULL, ml_real_to_string, MLRealT, NULL);
	ml_method_by_name("string", NULL, ml_identity, MLStringT, NULL);
	ml_method_by_name("string", 0, ml_list_to_string, MLListT, 0);
	ml_method_by_name("string", 0, ml_tree_to_string, MLTreeT, 0);
	ml_method_by_name("/", NULL, ml_string_string_split, MLStringT, MLStringT, NULL);
	ml_method_by_name("%", NULL, ml_string_match, MLStringT, MLStringT, NULL);
	ml_method_by_name("find", 0, ml_string_find, MLStringT, MLStringT, 0);
	ml_method_by_name("replace", NULL, ml_string_string_replace, MLStringT, MLStringT, MLStringT, NULL);
	ml_method_by_name("type", NULL, ml_error_type_value, MLErrorT, NULL);
	ml_method_by_name("message", NULL, ml_error_message_value, MLErrorT, NULL);

	AppendMethod = ml_method("append");
	ml_method_by_value(AppendMethod, NULL, stringify_nil, MLStringBufferT, MLNilT, NULL);
	ml_method_by_value(AppendMethod, NULL, stringify_integer, MLStringBufferT, MLIntegerT, NULL);
	ml_method_by_value(AppendMethod, NULL, stringify_real, MLStringBufferT, MLRealT, NULL);
	ml_method_by_value(AppendMethod, NULL, stringify_string, MLStringBufferT, MLStringT, NULL);
	ml_method_by_value(AppendMethod, NULL, stringify_method, MLStringBufferT, MLMethodT, NULL);
	ml_method_by_value(AppendMethod, NULL, stringify_list, MLStringBufferT, MLListT, NULL);
	ml_method_by_value(AppendMethod, NULL, stringify_tree, MLStringBufferT, MLTreeT, NULL);

	GC_word StringBufferLayout[] = {1};
	StringBufferDesc = GC_make_descriptor(StringBufferLayout, 1);
}

ml_inst_t *mli_push_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	(++Frame->Top)[-1] = Inst->Params[1].Value;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_pop_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	(--Frame->Top)[0] = 0;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_pop2_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	(--Frame->Top)[0] = 0;
	(--Frame->Top)[0] = 0;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_enter_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	for (int I = Inst->Params[1].Count; --I >= 0;) {
		ml_reference_t *Local = xnew(ml_reference_t, 1, ml_value_t *);
		Local->Type = MLReferenceT;
		Local->Address = Local->Value;
		Local->Value[0] = MLNil;
		(++Frame->Top)[-1] = (ml_value_t *)Local;
	}
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_var_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_reference_t *Local = (ml_reference_t *)Frame->Stack[Inst->Params[1].Index];
	ml_value_t *Value = Frame->Top[-1];
	Value = Value->Type->deref(Value);
	if (Value->Type == MLErrorT) {
		ml_error_trace_add(Value, Inst->Source);
		(Frame->Top++)[0] = Value;
		return Frame->OnError;
	}
	Local->Value[0] = Value;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_def_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	Frame->Top[-1] = Value->Type->deref(Value);
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_exit_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	for (int I = Inst->Params[1].Count; --I >= 0;) (--Frame->Top)[0] = 0;
	Frame->Top[-1] = Value;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_try_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	Frame->OnError = Inst->Params[1].Inst;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_catch_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Error= Frame->Top[-1];
	if (Error->Type != MLErrorT) {
		Frame->Top[-1] = ml_error("InternalError", "expected error value, not %s", Error->Type->Name);
		return Frame->OnError;
	}
	ml_value_t *Value = (ml_value_t *)new(ml_error_t);
	memcpy(Value, Error, sizeof(ml_error_t));
	Value->Type = MLErrorValueT;
	ml_value_t **Top = Frame->Stack + Inst->Params[1].Index;
	while (Frame->Top > Top) (--Frame->Top)[0] = 0;
	Frame->Top[-1] = Value;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_call_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	int Count = Inst->Params[1].Count;
	ml_value_t *Function = Frame->Top[~Count];
	Function = Function->Type->deref(Function);
	if (Function->Type == MLErrorT) {
		ml_error_trace_add(Function, Inst->Source);
		(Frame->Top++)[0] = Function;
		return Frame->OnError;
	}
	ml_value_t **Args = Frame->Top - Count;
	for (int I = 0; I < Count; ++I) {
		Args[I] = Args[I]->Type->deref(Args[I]);
		if (Args[I]->Type == MLErrorT) {
			ml_error_trace_add(Args[I], Inst->Source);
			(Frame->Top++)[0] = Args[I];
			return Frame->OnError;
		}
	}
	ml_value_t *Result = ml_call(Function, Count, Args);
	for (int I = Count; --I >= 0;) (--Frame->Top)[0] = 0;
	Frame->Top[-1] = Result;
	if (Result->Type == MLErrorT) {
		ml_error_trace_add(Result, Inst->Source);
		return Frame->OnError;
	} else {
		return Inst->Params[0].Inst;
	}
}

ml_inst_t *mli_const_call_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	int Count = Inst->Params[1].Count;
	ml_value_t *Function = Inst->Params[2].Value;
	ml_value_t **Args = Frame->Top - Count;
	for (int I = 0; I < Count; ++I) {
		Args[I] = Args[I]->Type->deref(Args[I]);
		if (Args[I]->Type == MLErrorT) {
			ml_error_trace_add(Args[I], Inst->Source);
			(Frame->Top++)[0] = Args[I];
			return Frame->OnError;
		}
	}
	ml_value_t *Result = ml_call(Function, Count, Args);
	if (Count == 0) {
		++Frame->Top;
	} else {
		for (int I = Count - 1; --I >= 0;) (--Frame->Top)[0] = 0;
	}
	Frame->Top[-1] = Result;
	if (Result->Type == MLErrorT) {
		ml_error_trace_add(Result, Inst->Source);
		return Frame->OnError;
	} else {
		return Inst->Params[0].Inst;
	}
}

ml_inst_t *mli_assign_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	(--Frame->Top)[0] = 0;
	Value = Value->Type->deref(Value);
	if (Value->Type == MLErrorT) {
		ml_error_trace_add(Value, Inst->Source);
		(Frame->Top++)[0] = Value;
		return Frame->OnError;
	}
	ml_value_t *Ref = Frame->Top[-1];
	ml_value_t *Result = Frame->Top[-1] = Ref->Type->assign(Ref, Value);
	if (Result->Type == MLErrorT) {
		ml_error_trace_add(Result, Inst->Source);
		return Frame->OnError;
	} else {
		return Inst->Params[0].Inst;
	}
}

ml_inst_t *mli_jump_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_if_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	Value = Value->Type->deref(Value);
	if (Value->Type == MLErrorT) {
		ml_error_trace_add(Value, Inst->Source);
		Frame->Top[-1] = Value;
		return Frame->OnError;
	}
	(--Frame->Top)[0] = 0;
	if (Value == MLNil) {
		return Inst->Params[0].Inst;
	} else {
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_until_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	if (Value == MLNil) {
		return Inst->Params[0].Inst;
	} else {
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_while_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	if (Value != MLNil) {
		return Inst->Params[0].Inst;
	} else {
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_and_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	Value = Value->Type->deref(Value);
	if (Value->Type == MLErrorT) {
		ml_error_trace_add(Value, Inst->Source);
		Frame->Top[-1] = Value;
		return Frame->OnError;
	} else if (Value == MLNil) {
		return Inst->Params[0].Inst;
	} else {
		(--Frame->Top)[0] = 0;
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_or_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	Value = Value->Type->deref(Value);
	if (Value->Type == MLErrorT) {
		ml_error_trace_add(Value, Inst->Source);
		Frame->Top[-1] = Value;
		return Frame->OnError;
	} else if (Value != MLNil) {
		return Inst->Params[0].Inst;
	} else {
		(--Frame->Top)[0] = 0;
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_exists_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	if (Value == MLNil) {
		(--Frame->Top)[0] = 0;
		return Inst->Params[0].Inst;
	} else {
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_next_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Iter = Frame->Top[-1];
	Frame->Top[-1] = Iter = Iter->Type->next(Iter);
	if (Iter->Type == MLErrorT) {
		ml_error_trace_add(Iter, Inst->Source);
		return Frame->OnError;
	} else if (Iter == MLNil) {
		return Inst->Params[0].Inst;
	} else {
		return Inst->Params[1].Inst;
	}
}

ml_inst_t *mli_key_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Iter = Frame->Top[-1];
	ml_value_t *Key = (++Frame->Top)[-1] = Iter->Type->key(Iter);
	if (Key->Type == MLErrorT) {
		ml_error_trace_add(Key, Inst->Source);
		return Frame->OnError;
	} else {
		return Inst->Params[0].Inst;
	}
}

ml_inst_t *mli_local_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	int Index = Inst->Params[1].Index;
	if (Index < 0) {
		(++Frame->Top)[-1] = Frame->UpValues[~Index];
	} else {
		(++Frame->Top)[-1] = Frame->Stack[Index];
	}
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_list_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	(++Frame->Top)[-1] = ml_list();
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_append_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	ml_value_t *Value = Frame->Top[-1];
	Value = Value->Type->deref(Value);
	if (Value->Type == MLErrorT) {
		Frame->Top[-1] = Value;
		return Frame->OnError;
	}
	ml_value_t *List = Frame->Top[-2];
	ml_list_append(List, Value);
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_closure_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	// closure <entry> <frame_size> <num_params> <num_upvalues> <upvalue_1> ...
	ml_closure_info_t *Info = Inst->Params[1].ClosureInfo;
	ml_closure_t *Closure = xnew(ml_closure_t, Info->NumUpValues, ml_value_t *);
	Closure->Type = MLClosureT;
	Closure->Info = Info;
	for (int I = 0; I < Info->NumUpValues; ++I) {
		int Index = Inst->Params[2 + I].Index;
		if (Index < 0) {
			Closure->UpValues[I] = Frame->UpValues[~Index];
		} else {
			Closure->UpValues[I] = Frame->Stack[Index];
		}
	}
	(++Frame->Top)[-1] = (ml_value_t *)Closure;
	return Inst->Params[0].Inst;
}

ml_inst_t *mli_ra_fields_run(ml_inst_t *Inst, ml_frame_t *Frame) {
	// params = <next> <num_fields> <field_1> <field_2> ...
	ml_value_t *Instance = Frame->Top[-1];
	Instance = Instance->Type->deref(Instance);
	(--Frame->Top)[0] = 0;
	if (Instance->Type != RaInstanceT) {
		ml_value_t *Error = ml_error("InternalError", "invalid instance");
		ml_error_trace_add(Error, Inst->Source);
		(++Frame->Top)[-1] = Error;
		return Frame->OnError;
	}
	for (int I = 0; I < Inst->Params[1].Count; ++I) {
		ml_value_t *Value = (++Frame->Top)[-1] = ra_instance_field_by_field((ra_instance_t *)Instance, Inst->Params[2 + I].RaField);
		if (Value->Type == MLErrorT) {
			ml_error_trace_add(Value, Inst->Source);
			return Frame->OnError;
		}
	}
	return Inst->Params[0].Inst;
}

typedef struct mlc_expr_t mlc_expr_t;
typedef struct mlc_scanner_t mlc_scanner_t;
typedef struct mlc_function_t mlc_function_t;
typedef struct mlc_decl_t mlc_decl_t;
typedef struct mlc_loop_t mlc_loop_t;
typedef struct mlc_try_t mlc_try_t;
typedef struct mlc_upvalue_t mlc_upvalue_t;

typedef struct { ml_inst_t *Start, *Exits; } mlc_compiled_t;

struct mlc_function_t {
	ml_getter_t GlobalGet;
	void *Globals;
	mlc_function_t *Up;
	mlc_decl_t *Decls;
	mlc_loop_t *Loop;
	mlc_try_t *Try;
	mlc_upvalue_t *UpValues;
	int Top, Size, Self;
};

struct mlc_decl_t {
	mlc_decl_t *Next;
	const char *Ident;
	int Index;
};

struct mlc_loop_t {
	mlc_loop_t *Up;
	mlc_try_t *Try;
	ml_inst_t *Next, *Exits;
	int NextTop, ExitTop;
};

struct mlc_try_t {
	mlc_try_t *Up;
	ml_inst_t *CatchInst;
	int CatchTop;
};

struct mlc_upvalue_t {
	mlc_upvalue_t *Next;
	mlc_decl_t *Decl;
	int Index;
};

#define MLC_EXPR_FIELDS(TYPE) \
	mlc_compiled_t (*compile)(mlc_function_t *, mlc_ ## TYPE ## _expr_t *, SHA256_CTX *HashContext); \
	mlc_expr_t *Next; \
	ml_source_t Source

struct mlc_expr_t {
	mlc_compiled_t (*compile)(mlc_function_t *, mlc_expr_t *, SHA256_CTX *HashContext);
	mlc_expr_t *Next;
	ml_source_t Source;
};

static inline ml_inst_t *ml_inst_new(int N, ml_source_t Source, ml_inst_t *(*run)(ml_inst_t *Inst, ml_frame_t *Frame)) {
	ml_inst_t *Inst = xnew(ml_inst_t, N, ml_param_t);
	Inst->Source = Source;
	Inst->run = run;
	return Inst;
}

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define ML_COMPILE_HASH sha256_update(HashContext, (BYTE *)__FILE__ TOSTRING(__LINE__), strlen(__FILE__ TOSTRING(__LINE__)));

static inline mlc_compiled_t ml_compile(mlc_function_t *Function, mlc_expr_t *Expr, SHA256_CTX *HashContext) {
	//static int Indent = NULL;
	if (Expr) {
		//for (int I = Indent; --I >= 0;) printf("\t");
		//printf("before compiling %s:%d Function->Top = %d\n", Expr->Source.Name, Expr->Source.Line, Function->Top);
		//++Indent;
		mlc_compiled_t Compiled = Expr->compile(Function, Expr, HashContext);
		//--Indent;
		//for (int I = Indent; --I >= 0;) printf("\t");
		//printf("after compiling %s:%d Function->Top = %d\n", Expr->Source.Name, Expr->Source.Line, Function->Top);
		//printf("after compiling %s:%d state =", Expr->Source.Name, Expr->Source.Line);
		//for (int I = 0; I < 8; ++I) printf("%08x", HashContext->state[I]);
		//printf("\n");
		return Compiled;
	} else {
		ML_COMPILE_HASH
		ml_inst_t *NilInst = ml_inst_new(2, (ml_source_t){"<internal>", 0}, mli_push_run);
		NilInst->Params[1].Value = MLNil;
		++Function->Top;
		return (mlc_compiled_t){NilInst, NilInst};
	}
}

static inline void mlc_connect(ml_inst_t *Exits, ml_inst_t *Start) {
	for (ml_inst_t *Exit = Exits; Exit;) {
		ml_inst_t *NextExit = Exit->Params[0].Inst;
		Exit->Params[0].Inst = Start;
		Exit = NextExit;
	}
}

typedef struct mlc_if_expr_t mlc_if_expr_t;
typedef struct mlc_if_case_t mlc_if_case_t;
typedef struct mlc_parent_expr_t mlc_parent_expr_t;
typedef struct mlc_fun_expr_t mlc_fun_expr_t;
typedef struct mlc_decl_expr_t mlc_decl_expr_t;
typedef struct mlc_dot_expr_t mlc_dot_expr_t;
typedef struct mlc_value_expr_t mlc_value_expr_t;
typedef struct mlc_ident_expr_t mlc_ident_expr_t;
typedef struct mlc_const_call_expr_t mlc_const_call_expr_t;
typedef struct mlc_block_expr_t mlc_block_expr_t;

struct mlc_if_case_t {
	mlc_if_case_t *Next;
	ml_source_t Source;
	mlc_expr_t *Condition;
	mlc_expr_t *Body;
};

struct mlc_if_expr_t {
	MLC_EXPR_FIELDS(if);
	mlc_if_case_t *Cases;
	mlc_expr_t *Else;
};

static mlc_compiled_t ml_if_expr_compile(mlc_function_t *Function, mlc_if_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top;
	mlc_if_case_t *Case = Expr->Cases;
	mlc_compiled_t Compiled = ml_compile(Function, Case->Condition, HashContext);
	--Function->Top;
	mlc_compiled_t BodyCompiled = ml_compile(Function, Case->Body, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *IfInst = ml_inst_new(2, Expr->Source, mli_and_run);
	IfInst->Params[0].Inst = BodyCompiled.Exits;
	IfInst->Params[1].Inst = BodyCompiled.Start;
	mlc_connect(Compiled.Exits, IfInst);
	Compiled.Exits = IfInst;
	while ((Case = Case->Next)) {
		Function->Top = OldTop;
		Compiled.Exits = IfInst->Params[0].Inst;
		mlc_compiled_t ConditionCompiled = ml_compile(Function, Case->Condition, HashContext);
		IfInst->run = mli_if_run;
		IfInst->Params[0].Inst = ConditionCompiled.Start;
		--Function->Top;
		BodyCompiled = ml_compile(Function, Case->Body, HashContext);
		ml_inst_t **Slot = &Compiled.Exits;
		while (Slot[0]) Slot = &Slot[0]->Params[0].Inst;
		Slot[0] = BodyCompiled.Exits;
		ML_COMPILE_HASH
		IfInst = ml_inst_new(2, Case->Source, mli_and_run);
		IfInst->Params[0].Inst = Compiled.Exits;
		IfInst->Params[1].Inst = BodyCompiled.Start;
		mlc_connect(ConditionCompiled.Exits, IfInst);
		Compiled.Exits = IfInst;
	}
	Function->Top = OldTop;
	if (Expr->Else) {
		Compiled.Exits = IfInst->Params[0].Inst;
		mlc_compiled_t BodyCompiled = ml_compile(Function, Expr->Else, HashContext);
		IfInst->run = mli_if_run;
		IfInst->Params[0].Inst = BodyCompiled.Start;
		ml_inst_t **Slot = &Compiled.Exits;
		while (Slot[0]) Slot = &Slot[0]->Params[0].Inst;
		Slot[0] = BodyCompiled.Exits;
	} else {
		++Function->Top;
	}
	return Compiled;
}

struct mlc_parent_expr_t {
	MLC_EXPR_FIELDS(parent);
	mlc_expr_t *Child;
};

static mlc_compiled_t ml_or_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top;
	mlc_expr_t *Child = Expr->Child;
	mlc_compiled_t Compiled = ml_compile(Function, Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *OrInst = ml_inst_new(2, Expr->Source, mli_or_run);
	mlc_connect(Compiled.Exits, OrInst);
	Compiled.Exits = OrInst;
	for (Child = Child->Next; Child->Next; Child = Child->Next) {
		Function->Top = OldTop;
		mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
		OrInst->Params[1].Inst = ChildCompiled.Start;
		ML_COMPILE_HASH
		OrInst = ml_inst_new(2, Expr->Source, mli_or_run);
		mlc_connect(ChildCompiled.Exits, OrInst);
		OrInst->Params[0].Inst = Compiled.Exits;
		Compiled.Exits = OrInst;
	}
	Function->Top = OldTop;
	mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
	OrInst->Params[1].Inst = ChildCompiled.Start;
	ml_inst_t **Slot = &Compiled.Exits;
	while (Slot[0]) Slot = &Slot[0]->Params[0].Inst;
	Slot[0] = ChildCompiled.Exits;
	return Compiled;
}

static mlc_compiled_t ml_and_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top;
	ML_COMPILE_HASH
	mlc_expr_t *Child = Expr->Child;
	mlc_compiled_t Compiled = ml_compile(Function, Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *IfInst = ml_inst_new(2, Expr->Source, mli_and_run);
	mlc_connect(Compiled.Exits, IfInst);
	Compiled.Exits = IfInst;
	for (Child = Child->Next; Child->Next; Child = Child->Next) {
		Function->Top = OldTop;
		mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
		IfInst->Params[1].Inst = ChildCompiled.Start;
		ML_COMPILE_HASH
		IfInst = ml_inst_new(2, Expr->Source, mli_and_run);
		mlc_connect(ChildCompiled.Exits, IfInst);
		IfInst->Params[0].Inst = Compiled.Exits;
		Compiled.Exits = IfInst;
	}
	Function->Top = OldTop;
	mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
	IfInst->Params[1].Inst = ChildCompiled.Start;
	ml_inst_t **Slot = &Compiled.Exits;
	while (Slot[0]) Slot = &Slot[0]->Params[0].Inst;
	Slot[0] = ChildCompiled.Exits;
	return Compiled;
}

static mlc_compiled_t ml_loop_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top;
	ML_COMPILE_HASH
	ml_inst_t *LoopInst = ml_inst_new(1, Expr->Source, mli_pop_run);
	mlc_loop_t Loop = {
		Function->Loop, Function->Try,
		LoopInst, NULL,
		Function->Top + 1, Function->Top + 1
	};
	Function->Loop = &Loop;
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	LoopInst->Params[0].Inst = Compiled.Start;
	mlc_connect(Compiled.Exits, LoopInst);
	Function->Loop = Loop.Up;
	Function->Top = OldTop + 1;
	Compiled.Exits = Loop.Exits;
	return Compiled;
}

static mlc_compiled_t ml_next_expr_compile(mlc_function_t *Function, mlc_expr_t *Expr, SHA256_CTX *HashContext) {
	ml_inst_t *NilInst = ml_inst_new(2, (ml_source_t){"<internal>", 0}, mli_push_run);
	NilInst->Params[1].Value = MLNil;
	ml_inst_t *NextInst = Function->Loop->Next;
	Function->Top++;
	if (Function->Try != Function->Loop->Try) {
		ML_COMPILE_HASH
		ml_inst_t *TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		TryInst->Params[1].Inst = Function->Try ? Function->Try->CatchInst : NULL;
		TryInst->Params[0].Inst = Function->Loop->Next;
		NextInst = TryInst;
	}
	if (Function->Top > Function->Loop->NextTop) {
		ML_COMPILE_HASH
		ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
		ExitInst->Params[0].Inst = NextInst;
		ExitInst->Params[1].Count = Function->Top - Function->Loop->NextTop;
		NilInst->Params[0].Inst = ExitInst;

	} else {
		ML_COMPILE_HASH
		NilInst->Params[0].Inst = NextInst;
	}
	Function->Top--;
	return (mlc_compiled_t){NilInst, NULL};
}

static mlc_compiled_t ml_exit_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_loop_t *Loop = Function->Loop;
	mlc_try_t *Try = Function->Try;
	Function->Loop = Loop->Up;
	Function->Try = Loop->Try;
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	if (Function->Try != Try) {
		ML_COMPILE_HASH
		ml_inst_t *TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		TryInst->Params[1].Inst = Function->Try ? Function->Try->CatchInst : NULL;
		TryInst->Params[0].Inst = Compiled.Start;
		Compiled.Start = TryInst;
	}
	Function->Loop = Loop;
	Function->Try = Try;
	if (Function->Top > Function->Loop->ExitTop) {
		ML_COMPILE_HASH
		ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
		ExitInst->Params[1].Count = Function->Top - Function->Loop->ExitTop;
		mlc_connect(Compiled.Exits, ExitInst);
		Compiled.Exits = ExitInst;
	}
	if (Compiled.Exits) {
		ml_inst_t **Slot = &Function->Loop->Exits;
		while (Slot[0]) Slot = &Slot[0]->Params[0].Inst;
		Slot[0] = Compiled.Exits;
	}
	return (mlc_compiled_t){Compiled.Start, NULL};
}

static mlc_compiled_t ml_not_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *NotInst = ml_inst_new(2, Expr->Source, mli_if_run);
	mlc_connect(Compiled.Exits, NotInst);
	ml_inst_t *NilInst = ml_inst_new(2, Expr->Source, mli_push_run);
	NilInst->Params[1].Value = MLNil;
	ml_inst_t *SomeInst = ml_inst_new(2, Expr->Source, mli_push_run);
	SomeInst->Params[1].Value = MLSome;
	NotInst->Params[0].Inst = SomeInst;
	NotInst->Params[1].Inst = NilInst;
	NilInst->Params[0].Inst = SomeInst;
	Compiled.Exits = NilInst;
	return Compiled;
}

static mlc_compiled_t ml_while_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
	ExitInst->Params[1].Count = Function->Top - Function->Loop->ExitTop;
	mlc_loop_t *Loop = Function->Loop;
	if (Function->Try != Loop->Try) {
		ML_COMPILE_HASH
		ml_inst_t *TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		TryInst->Params[1].Inst = Loop->Try ? Loop->Try->CatchInst : NULL;
		TryInst->Params[0].Inst = ExitInst;
		ExitInst = TryInst;
	}
	ML_COMPILE_HASH
	ml_inst_t *WhileInst = ml_inst_new(2, Expr->Source, mli_while_run);
	mlc_connect(Compiled.Exits, WhileInst);
	Compiled.Exits = WhileInst;
	WhileInst->Params[1].Inst = ExitInst;
	ExitInst->Params[0].Inst = Function->Loop->Exits;
	Function->Loop->Exits = ExitInst;
	return Compiled;
}

static mlc_compiled_t ml_until_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
	ExitInst->Params[1].Count = Function->Top - Function->Loop->ExitTop;
	mlc_loop_t *Loop = Function->Loop;
	if (Function->Try != Loop->Try) {
		ML_COMPILE_HASH
		ml_inst_t *TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		TryInst->Params[1].Inst = Loop->Try ? Loop->Try->CatchInst : NULL;
		TryInst->Params[0].Inst = ExitInst;
		ExitInst = TryInst;
	}
	ML_COMPILE_HASH
	ml_inst_t *UntilInst = ml_inst_new(2, Expr->Source, mli_until_run);
	mlc_connect(Compiled.Exits, UntilInst);
	Compiled.Exits = UntilInst;
	UntilInst->Params[1].Inst = ExitInst;
	ExitInst->Params[0].Inst = Function->Loop->Exits;
	Function->Loop->Exits = ExitInst;
	return Compiled;
}

static mlc_compiled_t ml_return_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	mlc_connect(Compiled.Exits, NULL);
	Compiled.Exits = NULL;
	return Compiled;
}

struct mlc_decl_expr_t {
	MLC_EXPR_FIELDS(decl);
	mlc_decl_t *Decl;
	mlc_expr_t *Child;
};

static mlc_compiled_t ml_var_expr_compile(mlc_function_t *Function, mlc_decl_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *VarInst = ml_inst_new(2, Expr->Source, mli_var_run);
	VarInst->Params[1].Index = Expr->Decl->Index;
	mlc_connect(Compiled.Exits, VarInst);
	Compiled.Exits = VarInst;
	return Compiled;
}

static mlc_compiled_t ml_def_expr_compile(mlc_function_t *Function, mlc_decl_expr_t *Expr, SHA256_CTX *HashContext) {
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *DefInst = ml_inst_new(1, Expr->Source, mli_def_run);
	mlc_decl_t *Decl = Expr->Decl;
	Decl->Index = Function->Top - 1;
	Decl->Next = Function->Decls;
	Function->Decls = Decl;
	mlc_connect(Compiled.Exits, DefInst);
	Compiled.Exits = DefInst;
	return Compiled;
}

static mlc_compiled_t ml_with_expr_compile(mlc_function_t *Function, mlc_decl_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top + 1;
	mlc_decl_t *OldScope = Function->Decls;
	mlc_expr_t *Child = Expr->Child;
	mlc_compiled_t Compiled = ml_compile(Function, Child, HashContext);
	mlc_decl_t *Decl = Expr->Decl;
	Decl->Index = Function->Top - 1;
	Child = Child->Next;
	mlc_decl_t *NextDecl = Decl->Next;
	Decl->Next = Function->Decls;
	Function->Decls = Decl;
	while (NextDecl) {
		Decl = NextDecl;
		NextDecl = Decl->Next;
		mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
		mlc_connect(Compiled.Exits, ChildCompiled.Start);
		Compiled.Exits = ChildCompiled.Exits;
		Decl->Index = Function->Top - 1;
		Decl->Next = Function->Decls;
		Function->Decls = Decl;
		Child = Child->Next;
	}
	mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
	mlc_connect(Compiled.Exits, ChildCompiled.Start);
	ML_COMPILE_HASH
	ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
	ExitInst->Params[1].Count = Function->Top - OldTop;
	mlc_connect(ChildCompiled.Exits, ExitInst);
	Compiled.Exits = ExitInst;
	Function->Decls = OldScope;
	Function->Top = OldTop;
	return Compiled;
}

static mlc_compiled_t ml_for_expr_compile(mlc_function_t *Function, mlc_decl_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top;
	mlc_decl_t *OldScope = Function->Decls;
	mlc_expr_t *Child = Expr->Child;
	mlc_compiled_t Compiled = ml_compile(Function, Child, HashContext);
	ml_inst_t *StartInst = ml_inst_new(2, Expr->Source, mli_until_run);
	mlc_connect(Compiled.Exits, StartInst);
	mlc_decl_t *Decl = Expr->Decl;
	Decl->Index = Function->Top - 1;
	mlc_decl_t *KeyDecl = Decl->Next;
	if (KeyDecl) {
		KeyDecl->Index = (Decl->Index = Function->Top) - 1;
		if (++Function->Top >= Function->Size) Function->Size = Function->Top + 1;
		KeyDecl->Next = Function->Decls;
	} else {
		Decl->Next = Function->Decls;
	}
	Function->Decls = Decl;
	ML_COMPILE_HASH
	ml_inst_t *NextInst = ml_inst_new(2, Expr->Source, mli_next_run);
	ML_COMPILE_HASH
	ml_inst_t *PopInst = ml_inst_new(1, Expr->Source, mli_pop_run);
	PopInst->Params[0].Inst = NextInst;
	mlc_loop_t Loop = {
		Function->Loop, Function->Try,
		PopInst, NULL,
		Function->Top + 1, OldTop + 1
	};
	Function->Loop = &Loop;
	mlc_compiled_t BodyCompiled = ml_compile(Function, Child->Next, HashContext);
	mlc_connect(BodyCompiled.Exits, PopInst);
	if (KeyDecl) {
		ML_COMPILE_HASH
		ml_inst_t *KeyInst = ml_inst_new(1, Expr->Source, mli_key_run);
		KeyInst->Params[0].Inst = BodyCompiled.Start;
		NextInst->Params[1].Inst = KeyInst;
		StartInst->Params[1].Inst = KeyInst;
		PopInst->run = mli_pop2_run;
	} else {
		NextInst->Params[1].Inst = BodyCompiled.Start;
		StartInst->Params[1].Inst = BodyCompiled.Start;
	}
	Compiled.Exits = Loop.Exits;
	Function->Loop = Loop.Up;
	Function->Top = OldTop;
	if (Child->Next->Next) {
		mlc_compiled_t ElseCompiled = ml_compile(Function, Child->Next->Next, HashContext);
		ML_COMPILE_HASH
		ml_inst_t *PopInst = ml_inst_new(1, Expr->Source, mli_pop_run);
		PopInst->Params[0].Inst = ElseCompiled.Start;
		StartInst->Params[0].Inst = PopInst;
		NextInst->Params[0].Inst = PopInst;
		ml_inst_t **Slot = &Compiled.Exits;
		while (Slot[0]) Slot = &Slot[0]->Params[0].Inst;
		Slot[0] = ElseCompiled.Exits;
	} else {
		++Function->Top;
		NextInst->Params[0].Inst = Compiled.Exits;
		StartInst->Params[0].Inst = NextInst;
		Compiled.Exits = StartInst;
	}
	Function->Decls = OldScope;
	return Compiled;
}

static mlc_compiled_t ml_all_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	ML_COMPILE_HASH
	ml_inst_t *ListInst = ml_inst_new(1, Expr->Source, mli_list_run);
	++Function->Top;
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	ListInst->Params[0].Inst = Compiled.Start;
	ml_inst_t *UntilInst = ml_inst_new(2, Expr->Source, mli_until_run);
	mlc_connect(Compiled.Exits, UntilInst);
	ml_inst_t *AppendInst = ml_inst_new(1, Expr->Source, mli_append_run);
	UntilInst->Params[1].Inst = AppendInst;
	ml_inst_t *NextInst = ml_inst_new(2, Expr->Source, mli_next_run);
	ml_inst_t *PopInst = ml_inst_new(1, Expr->Source, mli_pop_run);
	AppendInst->Params[0].Inst = NextInst;
	UntilInst->Params[0].Inst = PopInst;
	NextInst->Params[0].Inst = PopInst;
	NextInst->Params[1].Inst = AppendInst;
	return (mlc_compiled_t){ListInst, PopInst};
}

struct mlc_block_expr_t {
	MLC_EXPR_FIELDS(block);
	mlc_decl_t *Decl;
	mlc_expr_t *Child;
	mlc_decl_t *CatchDecl;
	mlc_expr_t *Catch;
};

static mlc_compiled_t ml_block_expr_compile(mlc_function_t *Function, mlc_block_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top + 1, NumVars = 0, NumDefs = 0;
	mlc_decl_t *OldScope = Function->Decls;
	mlc_try_t Try;
	ml_inst_t *CatchExitInst;
	if (Expr->Catch) {
		ML_COMPILE_HASH
		Expr->CatchDecl->Index = Function->Top++;
		Expr->CatchDecl->Next = Function->Decls;
		Function->Decls = Expr->CatchDecl;
		mlc_compiled_t TryCompiled = ml_compile(Function, Expr->Catch, HashContext);
		ml_inst_t *TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		ml_inst_t *CatchInst = ml_inst_new(2, Expr->Source, mli_catch_run);
		TryInst->Params[0].Inst = CatchInst;
		TryInst->Params[1].Inst = Function->Try ? Function->Try->CatchInst : NULL;
		CatchInst->Params[0].Inst = TryCompiled.Start;
		CatchInst->Params[1].Index = OldTop;
		Function->Decls = OldScope;
		Function->Top = OldTop - 1;
		Try.Up = Function->Try;
		Try.CatchInst = TryInst;
		Try.CatchTop = OldTop;
		CatchExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
		CatchExitInst->Params[1].Count = 1;
		mlc_connect(TryCompiled.Exits, CatchExitInst);
		Function->Try = &Try;
	}
	for (mlc_decl_t *Decl = Expr->Decl; Decl;) {
		Decl->Index = Function->Top++;
		mlc_decl_t *NextDecl = Decl->Next;
		Decl->Next = Function->Decls;
		Function->Decls = Decl;
		Decl = NextDecl;
		++NumVars;
	}
	if (Function->Top >= Function->Size) Function->Size = Function->Top + 1;
	mlc_expr_t *Child = Expr->Child;
	mlc_compiled_t Compiled = ml_compile(Function, Child, HashContext);
	if (Child) while ((Child = Child->Next)) {
		ML_COMPILE_HASH
		ml_inst_t *PopInst = ml_inst_new(1, Expr->Source, mli_pop_run);
		mlc_connect(Compiled.Exits, PopInst);
		--Function->Top;
		mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
		PopInst->Params[0].Inst = ChildCompiled.Start;
		Compiled.Exits = ChildCompiled.Exits;
	}
	if (NumVars > 0) {
		ML_COMPILE_HASH
		ml_inst_t *EnterInst = ml_inst_new(2, Expr->Source, mli_enter_run);
		EnterInst->Params[0].Inst = Compiled.Start;
		EnterInst->Params[1].Count = NumVars;
		Compiled.Start = EnterInst;
	}
	if (NumVars + NumDefs > 0) {
		ML_COMPILE_HASH
		ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
		ExitInst->Params[1].Count = NumVars + NumDefs;
		mlc_connect(Compiled.Exits, ExitInst);
		Compiled.Exits = ExitInst;
	}
	if (Expr->Catch) {
		ml_inst_t *TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		TryInst->Params[0].Inst = Compiled.Start;
		TryInst->Params[1].Inst = Try.CatchInst;
		Compiled.Start = TryInst;
		Function->Try = Try.Up;
		TryInst = ml_inst_new(2, Expr->Source, mli_try_run);
		TryInst->Params[1].Inst = Function->Try ? Function->Try->CatchInst : NULL;
		TryInst->Params[0].Inst = CatchExitInst;
		mlc_connect(Compiled.Exits, TryInst);
		Compiled.Exits = TryInst;
	}
	Function->Decls = OldScope;
	Function->Top = OldTop;
	return Compiled;
}

static mlc_compiled_t ml_call_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top + 1;
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	int NumArgs = 0;
	for (mlc_expr_t *Child = Expr->Child->Next; Child; Child = Child->Next) {
		++NumArgs;
		mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
		mlc_connect(Compiled.Exits, ChildCompiled.Start);
		Compiled.Exits = ChildCompiled.Exits;
	}
	ML_COMPILE_HASH
	ml_inst_t *CallInst = ml_inst_new(2, Expr->Source, mli_call_run);
	CallInst->Params[1].Count = NumArgs;
	mlc_connect(Compiled.Exits, CallInst);
	Compiled.Exits = CallInst;
	Function->Top = OldTop;
	return Compiled;
}

static mlc_compiled_t ml_assign_expr_compile(mlc_function_t *Function, mlc_parent_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldSelf = Function->Self;
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
	Function->Self = Function->Top - 1;
	mlc_compiled_t ValueCompiled = ml_compile(Function, Expr->Child->Next, HashContext);
	mlc_connect(Compiled.Exits, ValueCompiled.Start);
	ML_COMPILE_HASH
	ml_inst_t *AssignInst = ml_inst_new(1, Expr->Source, mli_assign_run);
	mlc_connect(ValueCompiled.Exits, AssignInst);
	Compiled.Exits = AssignInst;
	Function->Top -= 1;
	Function->Self = OldSelf;
	return Compiled;
}

static mlc_compiled_t ml_old_expr_compile(mlc_function_t *Function, mlc_expr_t *Expr, SHA256_CTX *HashContext) {
	ML_COMPILE_HASH
	ml_inst_t *OldInst = ml_inst_new(2, Expr->Source, mli_local_run);
	OldInst->Params[1].Index = Function->Self;
	if (++Function->Top >= Function->Size) Function->Size = Function->Top + 1;
	return (mlc_compiled_t){OldInst, OldInst};
}

struct mlc_const_call_expr_t {
	MLC_EXPR_FIELDS(const_call);
	mlc_expr_t *Child;
	ml_value_t *Value;
};

static mlc_compiled_t ml_const_call_expr_compile(mlc_function_t *Function, mlc_const_call_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top + 1;
	if (OldTop >= Function->Size) Function->Size = Function->Top + 1;
	long ValueHash = ml_hash(Expr->Value);
	sha256_update(HashContext, (void *)&ValueHash, sizeof(ValueHash));
	ML_COMPILE_HASH
	ml_inst_t *CallInst = ml_inst_new(3, Expr->Source, mli_const_call_run);
	CallInst->Params[2].Value = Expr->Value;
	if (Expr->Child) {
		int NumArgs = 1;
		mlc_compiled_t Compiled = ml_compile(Function, Expr->Child, HashContext);
		for (mlc_expr_t *Child = Expr->Child->Next; Child; Child = Child->Next) {
			++NumArgs;
			mlc_compiled_t ChildCompiled = ml_compile(Function, Child, HashContext);
			mlc_connect(Compiled.Exits, ChildCompiled.Start);
			Compiled.Exits = ChildCompiled.Exits;
		}
		CallInst->Params[1].Count = NumArgs;
		mlc_connect(Compiled.Exits, CallInst);
		Compiled.Exits = CallInst;
		Function->Top = OldTop;
		return Compiled;
	} else {
		CallInst->Params[1].Count = 0;
		Function->Top = OldTop;
		return (mlc_compiled_t){CallInst, CallInst};
	}
}

struct mlc_fun_expr_t {
	MLC_EXPR_FIELDS(fun);
	mlc_decl_t *Params;
	mlc_expr_t *Body;
};

static mlc_compiled_t ml_fun_expr_compile(mlc_function_t *Function, mlc_fun_expr_t *Expr, SHA256_CTX *HashContext) {
	// closure <entry> <frame_size> <num_params> <num_upvalues> <upvalue_1> ...
	mlc_function_t SubFunction[1] = {{Function->GlobalGet, Function->Globals, NULL,}};
	SubFunction->Up = Function;
	int NumParams = 0;
	mlc_decl_t **ParamSlot = &SubFunction->Decls;
	for (mlc_decl_t *Param = Expr->Params; Param;) {
		mlc_decl_t *NextParam = Param->Next;
		++NumParams;
		if (Param->Index) NumParams = ~NumParams;
		Param->Index = SubFunction->Top++;
		ParamSlot[0] = Param;
		ParamSlot = &Param->Next;
		Param = NextParam;
	}
	SubFunction->Size = SubFunction->Top + 1;
	SHA256_CTX SubHashContext[1];
	sha256_init(SubHashContext);
	mlc_compiled_t Compiled = ml_compile(SubFunction, Expr->Body, SubHashContext);
	mlc_connect(Compiled.Exits, NULL);
	int NumUpValues = 0;
	for (mlc_upvalue_t *UpValue = SubFunction->UpValues; UpValue; UpValue = UpValue->Next) ++NumUpValues;
	ML_COMPILE_HASH
	ml_inst_t *ClosureInst = ml_inst_new(2 + NumUpValues, Expr->Source, mli_closure_run);
	ml_param_t *Params = ClosureInst->Params;
	ml_closure_info_t *Info = new(ml_closure_info_t);
	Info->Entry = Compiled.Start;
	Info->FrameSize = SubFunction->Size;
	Info->NumParams = NumParams;
	Info->NumUpValues = NumUpValues;
	sha256_final(SubHashContext, Info->Hash);
	Params[1].ClosureInfo = Info;
	int Index = 2;
	for (mlc_upvalue_t *UpValue = SubFunction->UpValues; UpValue; UpValue = UpValue->Next) Params[Index++].Index = UpValue->Index;
	if (++Function->Top >= Function->Size) Function->Size = Function->Top + 1;
	return (mlc_compiled_t){ClosureInst, ClosureInst};
}

struct mlc_ident_expr_t {
	MLC_EXPR_FIELDS(ident);
	const char *Ident;
};

static int ml_upvalue_find(mlc_function_t *Function, mlc_decl_t *Decl, mlc_function_t *Origin) {
	if (Function == Origin) return Decl->Index;
	mlc_upvalue_t **UpValueSlot = &Function->UpValues;
	int Index = 0;
	while (UpValueSlot[0]) {
		if (UpValueSlot[0]->Decl == Decl) return ~Index;
		UpValueSlot = &UpValueSlot[0]->Next;
		++Index;
	}
	mlc_upvalue_t *UpValue = new(mlc_upvalue_t);
	UpValue->Decl = Decl;
	UpValue->Index = ml_upvalue_find(Function->Up, Decl, Origin);
	UpValueSlot[0] = UpValue;
	return ~Index;
}

static mlc_compiled_t ml_ident_expr_compile(mlc_function_t *Function, mlc_ident_expr_t *Expr, SHA256_CTX *HashContext) {
	for (mlc_function_t *UpFunction = Function; UpFunction; UpFunction = UpFunction->Up) {
		for (mlc_decl_t *Decl = UpFunction->Decls; Decl; Decl = Decl->Next) {
			if (!strcmp(Decl->Ident, Expr->Ident)) {
				int Index = ml_upvalue_find(Function, Decl, UpFunction);
				sha256_update(HashContext, (void *)&Index, sizeof(Index));
				ML_COMPILE_HASH
				ml_inst_t *LocalInst = ml_inst_new(2, Expr->Source, mli_local_run);
				LocalInst->Params[1].Index = Index;
				if (++Function->Top >= Function->Size) Function->Size = Function->Top + 1;
				return (mlc_compiled_t){LocalInst, LocalInst};
			}
		}
	}
	sha256_update(HashContext, (BYTE *)Expr->Ident, strlen(Expr->Ident));
	ML_COMPILE_HASH
	ml_inst_t *ValueInst = ml_inst_new(2, Expr->Source, mli_push_run);
	ValueInst->Params[1].Value = (Function->GlobalGet)(Function->Globals, Expr->Ident);
	if (++Function->Top >= Function->Size) Function->Size = Function->Top + 1;
	return (mlc_compiled_t){ValueInst, ValueInst};
}

struct mlc_value_expr_t {
	MLC_EXPR_FIELDS(value);
	ml_value_t *Value;
};

static mlc_compiled_t ml_value_expr_compile(mlc_function_t *Function, mlc_value_expr_t *Expr, SHA256_CTX *HashContext) {
	long ValueHash = ml_hash(Expr->Value);
	sha256_update(HashContext, (void *)&ValueHash, sizeof(ValueHash));
	ML_COMPILE_HASH
	ml_inst_t *ValueInst = ml_inst_new(2, Expr->Source, mli_push_run);
	ValueInst->Params[1].Value = Expr->Value;
	if (++Function->Top >= Function->Size) Function->Size = Function->Top + 1;
	return (mlc_compiled_t){ValueInst, ValueInst};
}

typedef struct mlc_ra_exists_expr_t mlc_ra_exists_expr_t;

struct mlc_ra_exists_expr_t {
	MLC_EXPR_FIELDS(ra_exists);
	mlc_decl_t *Decl;
	mlc_expr_t *Exists, *Then, *Else;
	ra_schema_field_t **Fields;
	int NumFields;
};

static mlc_compiled_t ml_ra_exists_expr_compile(mlc_function_t *Function, mlc_ra_exists_expr_t *Expr, SHA256_CTX *HashContext) {
	int OldTop = Function->Top;
	mlc_compiled_t Compiled = ml_compile(Function, Expr->Exists, HashContext);
	ML_COMPILE_HASH
	ml_inst_t *FieldsInst = ml_inst_new(2 + Expr->NumFields, Expr->Source, mli_ra_fields_run);
	--Function->Top;
	mlc_decl_t *OldScope = Function->Decls;
	mlc_decl_t *Decl = Expr->Decl;
	while (Decl) {
		++Function->Top;
		Decl->Index = Function->Top - 1;
		mlc_decl_t *NextDecl = Decl->Next;
		Decl->Next = Function->Decls;
		Function->Decls = Decl;
		Decl = NextDecl;
	}
	FieldsInst->Params[1].Count = Expr->NumFields;
	for (int I = 0; I < Expr->NumFields; ++I) FieldsInst->Params[2 + I].RaField = Expr->Fields[I];
	mlc_compiled_t BodyCompiled = ml_compile(Function, Expr->Then, HashContext);
	FieldsInst->Params[0].Inst = BodyCompiled.Start;
	ML_COMPILE_HASH
	ml_inst_t *ExitInst = ml_inst_new(2, Expr->Source, mli_exit_run);
	ExitInst->Params[1].Count = Expr->NumFields;
	mlc_connect(BodyCompiled.Exits, ExitInst);
	ML_COMPILE_HASH
	ml_inst_t *IfInst = ml_inst_new(2, Expr->Source, mli_until_run);
	IfInst->Params[0].Inst = ExitInst;
	IfInst->Params[1].Inst = FieldsInst;
	mlc_connect(Compiled.Exits, IfInst);
	Compiled.Exits = IfInst;
	Function->Top = OldTop;
	Function->Decls = OldScope;
	if (Expr->Else) {
		Compiled.Exits = ExitInst;
		mlc_compiled_t BodyCompiled = ml_compile(Function, Expr->Else, HashContext);
		IfInst->run = mli_exists_run;
		IfInst->Params[0].Inst = BodyCompiled.Start;
		ExitInst->Params[0].Inst = BodyCompiled.Exits;
	} else {
		++Function->Top;
	}
	return Compiled;
}

typedef enum ml_token_t {
	MLT_NONE,
	MLT_EOL,
	MLT_EOI,
	MLT_IF,
	MLT_THEN,
	MLT_ELSEIF,
	MLT_ELSE,
	MLT_END,
	MLT_LOOP,
	MLT_WHILE,
	MLT_UNTIL,
	MLT_EXIT,
	MLT_NEXT,
	MLT_FOR,
	MLT_ALL,
	MLT_IN,
	MLT_IS,
	MLT_FUN,
	MLT_RETURN,
	MLT_WITH,
	MLT_DO,
	MLT_ON,
	MLT_NIL,
	MLT_AND,
	MLT_OR,
	MLT_NOT,
	MLT_OLD,
	MLT_DEF,
	MLT_WHEN,
	MLT_SCHEMA,
	MLT_INDEX,
	MLT_EXISTS,
	MLT_INSERT,
	MLT_SIGNAL,
	MLT_UPDATE,
	MLT_DELETE,
	MLT_VAR,
	MLT_IDENT,
	MLT_LEFT_PAREN,
	MLT_RIGHT_PAREN,
	MLT_LEFT_SQUARE,
	MLT_RIGHT_SQUARE,
	MLT_LEFT_BRACE,
	MLT_RIGHT_BRACE,
	MLT_SEMICOLON,
	MLT_COLON,
	MLT_COMMA,
	MLT_ASSIGN,
	MLT_SYMBOL,
	MLT_VALUE,
	MLT_EXPR,
	MLT_OPERATOR,
	MLT_METHOD
} ml_token_t;

const char *MLTokens[] = {
	"", // MLT_NONE,
	"<end of line>", // MLT_EOL,
	"<end of input>", // MLT_EOI,
	"if", // MLT_IF,
	"then", // MLT_THEN,
	"elseif", // MLT_ELSEIF,
	"else", // MLT_ELSE,
	"end", // MLT_END,
	"loop", // MLT_LOOP,
	"while", // MLT_WHILE,
	"until", // MLT_UNTIL,
	"exit", // MLT_EXIT,
	"next", // MLT_NEXT,
	"for", // MLT_FOR,
	"all", // MLT_ALL,
	"in", // MLT_IN,
	"is", // MLT_IS,
	"fun", // MLT_FUN,
	"return", // MLT_RETURN,
	"with", // MLT_WITH,
	"do", // MLT_DO,
	"on", // MLT_ON,
	"nil", // MLT_NIL,
	"and", // MLT_AND,
	"or", // MLT_OR,
	"not", // MLT_NOT,
	"old", // MLT_OLD,
	"def", // MLT_DEF,
	"when", // MLT_WHEN,
	"schema", // MLT_SCHEMA,
	"index", // MLT_INDEX,
	"exists", // MLT_EXISTS,
	"insert", // MLT_INSERT,
	"signal", // MLT_SIGNAL,
	"update", // MLT_UPDATE,
	"delete", // MLT_DELETE,
	"var", // MLT_VAR,
	"<identifier>", // MLT_IDENT,
	"(", // MLT_LEFT_PAREN,
	")", // MLT_RIGHT_PAREN,
	"[", // MLT_LEFT_SQUARE,
	"]", // MLT_RIGHT_SQUARE,
	"{", // MLT_LEFT_BRACE,
	"}", // MLT_RIGHT_BRACE,
	";", // MLT_SEMICOLON,
	":", // MLT_COLON,
	",", // MLT_COMMA,
	":=", // MLT_ASSIGN,
	"::", // MLT_SYMBOL,
	"<value>", // MLT_VALUE,
	"<expr>", // MLT_EXPR,
	"<operator>", // MLT_OPERATOR
	"<method>" // MLT_METHOD
};

struct mlc_scanner_t {
	const char *Next;
	ml_source_t Source;
	ml_token_t Token;
	ml_value_t *Value;
	mlc_expr_t *Expr;
	const char *Ident;
	void *Data;
	const char *(*read)(void *);
	jmp_buf OnError;
	ml_value_t *Error;
} *ml_scanner(const char *SourceName, void *Data, const char *(*read)(void *)) {
	mlc_scanner_t *Scanner = new(mlc_scanner_t);
	Scanner->Token = MLT_NONE;
	Scanner->Next = "";
	Scanner->Source.Name = SourceName;
	Scanner->Source.Line = 0;
	Scanner->Data = Data;
	Scanner->read = read;
	return Scanner;
}

typedef enum {EXPR_SIMPLE, EXPR_AND, EXPR_OR, EXPR_DEFAULT} ml_expr_level_t;

static mlc_expr_t *ml_accept_term(mlc_scanner_t *Scanner);
static mlc_expr_t *ml_parse_expression(mlc_scanner_t *Scanner, ml_expr_level_t Level);
static mlc_expr_t *ml_accept_expression(mlc_scanner_t *Scanner, ml_expr_level_t Level);
static mlc_expr_t *ml_accept_block(mlc_scanner_t *Scanner);
static void ml_accept(mlc_scanner_t *Scanner, ml_token_t Token);

static mlc_expr_t *ml_accept_string(mlc_scanner_t *Scanner) {
	char Char = Scanner->Next[0];
	if (Char == '\'') {
		++Scanner->Next;
		return NULL;
	}
	int Length = 0;
	const char *End = Scanner->Next;
	while (End[0] && End[0] != '\'' && End[0] != '{') {
		if (End[0] == '\\') {
			if (!*++End) {
				Scanner->Error = ml_error("ParseError", "end of line while parsing string");
				ml_error_trace_add(Scanner->Error, Scanner->Source);
				longjmp(Scanner->OnError, 1);
			}
		}
		++Length;
		++End;
	}
	mlc_expr_t *Expr = NULL;
	if (Length > 0) {
		char *String = snew(Length + 1), *D = String;
		for (const char *S = Scanner->Next; S < End; ++S) {
			if (*S == '\\') {
				++S;
				switch (*S) {
				case 'r': *D++ = '\r'; break;
				case 'n': *D++ = '\n'; break;
				case 't': *D++ = '\t'; break;
				case 'e': *D++ = '\e'; break;
				case '\'': *D++ = '\''; break;
				case '\"': *D++ = '\"'; break;
				case '\\': *D++ = '\\'; break;
				case '{': *D++ = '{'; break;
				}
			} else {
				*D++ = *S;
			}
		}
		*D = 0;
		mlc_value_expr_t *ValueExpr = new(mlc_value_expr_t);
		ValueExpr->compile = ml_value_expr_compile;
		ValueExpr->Source = Scanner->Source;
		ValueExpr->Source = Scanner->Source;
		ValueExpr->Value = ml_string(String, Length);
		Expr = (mlc_expr_t *)ValueExpr;
	}
	Scanner->Next = End + 1;
	if (!End[0]) {
		Scanner->Next = (Scanner->read)(Scanner->Data);
		++Scanner->Source.Line;
		if (!Scanner->Next) {
			Scanner->Error = ml_error("ParseError", "end of input while parsing string");
			ml_error_trace_add(Scanner->Error, Scanner->Source);
			longjmp(Scanner->OnError, 1);
		}
		mlc_expr_t *Next = ml_accept_string(Scanner);
		if (Expr) {
			Expr->Next = Next;
		} else {
			Expr = Next;
		}
	} else if (End[0] == '{') {
		mlc_expr_t *Embedded = ml_accept_expression(Scanner, EXPR_DEFAULT);
		ml_accept(Scanner, MLT_RIGHT_BRACE);
		Embedded->Next = ml_accept_string(Scanner);
		if (Expr) {
			Expr->Next = Embedded;
		} else {
			Expr = Embedded;
		}
	}
	return Expr;
}

static int ml_parse(mlc_scanner_t *Scanner, ml_token_t Token) {
	static int OperatorChars[] = {
		['!'] = 1,
		['@'] = 1,
		['#'] = 1,
		['$'] = 1,
		['%'] = 1,
		['^'] = 1,
		['&'] = 1,
		['*'] = 1,
		['-'] = 1,
		['+'] = 1,
		['='] = 1,
		['|'] = 1,
		['\\'] = 1,
		['~'] = 1,
		['`'] = 1,
		['/'] = 1,
		['?'] = 1,
		['<'] = 1,
		['>'] = 1,
		['.'] = 1
	};
	if (Scanner->Token == MLT_NONE) for (;;) {
		char Char = Scanner->Next[0];
		if (!Char) {
			Scanner->Next = (Scanner->read)(Scanner->Data);
			++Scanner->Source.Line;
			if (Scanner->Next) continue;
			Scanner->Token = MLT_EOI;
			goto done;
		}
		if (Char == '\n') {
			++Scanner->Next;
			Scanner->Token = MLT_EOL;
			goto done;
		}
		if (Char <= ' ') {
			++Scanner->Next;
			continue;
		}
		if (isalpha(Char) || Char == '_') {
			const char *End = Scanner->Next + 1;
			for (Char = End[0]; isalnum(Char) || Char == '_'; Char = *++End);
			int Length = End - Scanner->Next;
			for (ml_token_t T = MLT_IF; T <= MLT_VAR; ++T) {
				const char *P = Scanner->Next;
				const char *C = MLTokens[T];
				while (*C && *C == *P) {++C; ++P;}
				if (!*C && P == End) {
					Scanner->Token = T;
					Scanner->Next = End;
					goto done;
				}
			}
			char *Ident = snew(Length + 1);
			memcpy(Ident, Scanner->Next, Length);
			Ident[Length] = 0;
			Scanner->Ident = Ident;
			Scanner->Token = MLT_IDENT;
			Scanner->Next = End;
			goto done;
		}
		if (isdigit(Char) || (Char == '-' && isdigit(Scanner->Next[1]))) {
			char *End;
			double Double = strtod(Scanner->Next, &End);
			for (const char *P = Scanner->Next; P < End; ++P) {
				if (P[0] == '.' || P[0] == 'e' || P[0] == 'E') {
					Scanner->Value = ml_real(Double);
					Scanner->Token = MLT_VALUE;
					Scanner->Next = End;
					goto done;
				}
			}
			long Integer = strtol(Scanner->Next, &End, 10);
			Scanner->Value = ml_integer(Integer);
			Scanner->Token = MLT_VALUE;
			Scanner->Next = End;
			goto done;
		}
		if (Char == '\'') {
			++Scanner->Next;
			mlc_expr_t *Child = ml_accept_string(Scanner);
			if (!Child) {
				Scanner->Token = MLT_VALUE;
				Scanner->Value = ml_string("", 0);
			} else if (!Child->Next && Child->compile == (void *)ml_value_expr_compile && ((mlc_value_expr_t *)Child)->Value->Type == MLStringT) {
				Scanner->Token = MLT_VALUE;
				Scanner->Value = ((mlc_value_expr_t *)Child)->Value;
			} else {
				mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
				CallExpr->compile = ml_const_call_expr_compile;
				CallExpr->Source = Scanner->Source;
				CallExpr->Value = (ml_value_t *)StringNew;
				CallExpr->Child = Child;
				Scanner->Token = MLT_EXPR;
				Scanner->Expr = (mlc_expr_t *)CallExpr;
			}
			goto done;
		}
		if (Char == '\"') {
			++Scanner->Next;
			int Length = 0;
			const char *End = Scanner->Next;
			while (End[0] != '\"') {
				if (!End[0]) {
					Scanner->Error = ml_error("ParseError", "end of input while parsing string");
					ml_error_trace_add(Scanner->Error, Scanner->Source);
					longjmp(Scanner->OnError, 1);
				}
				if (End[0] == '\\') ++End;
				++Length;
				++End;
			}
			char *String = snew(Length + 1), *D = String;
			for (const char *S = Scanner->Next; S < End; ++S) {
				if (*S == '\\') {
					++S;
					switch (*S) {
					case 'r': *D++ = '\r'; break;
					case 'n': *D++ = '\n'; break;
					case 't': *D++ = '\t'; break;
					case 'e': *D++ = '\e'; break;
					case '\'': *D++ = '\''; break;
					case '\"': *D++ = '\"'; break;
					case '\\': *D++ = '\\'; break;
					}
				} else {
					*D++ = *S;
				}
			}
			*D = 0;
			Scanner->Value = ml_string(String, Length);
			Scanner->Token = MLT_VALUE;
			Scanner->Next = End + 1;
			goto done;
		}
		if (Char == ':') {
			if (Scanner->Next[1] == '=') {
				Scanner->Token = MLT_ASSIGN;
				Scanner->Next += 2;
				goto done;
			} else if (isalpha(Scanner->Next[1]) || Scanner->Next[1] == '_') {
				const char *End = Scanner->Next + 1;
				for (Char = End[0]; isalnum(Char) || Char == '_'; Char = *++End);
				int Length = End - Scanner->Next - 1;
				char *Ident = snew(Length + 1);
				memcpy(Ident, Scanner->Next + 1, Length);
				Ident[Length] = 0;
				Scanner->Ident = Ident;
				Scanner->Token = MLT_METHOD;
				Scanner->Next = End;
				goto done;
			} else if (Scanner->Next[1] == ':') {
				const char *End = Scanner->Next + 2;
				for (Char = End[0]; OperatorChars[(int)Char]; Char = *++End);
				int Length = End - Scanner->Next - 1;
				char *Operator = snew(Length + 1);
				strncpy(Operator, Scanner->Next + 2, Length);
				Operator[Length] = 0;
				Scanner->Ident = Operator;
				Scanner->Token = MLT_METHOD;
				Scanner->Next = End;
				goto done;
			}
		}
		if (Char == '-' && Scanner->Next[1] == '-') {
			Scanner->Next = "";
			continue;
		}
		for (ml_token_t T = MLT_LEFT_PAREN; T <= MLT_COMMA; ++T) {
			if (Char == MLTokens[T][0]) {
				Scanner->Token = T;
				++Scanner->Next;
				goto done;
			}
		}
		if (OperatorChars[(int)Char]) {
			const char *End = Scanner->Next;
			for (Char = End[0]; OperatorChars[(int)Char]; Char = *++End);
			int Length = End - Scanner->Next;
			char *Operator = snew(Length + 1);
			strncpy(Operator, Scanner->Next, Length);
			Operator[Length] = 0;
			Scanner->Ident = Operator;
			Scanner->Token = MLT_OPERATOR;
			Scanner->Next = End;
			goto done;
		}
		Scanner->Error = ml_error("ParseError", "unexpected character <%c>", Char);
		ml_error_trace_add(Scanner->Error, Scanner->Source);
		longjmp(Scanner->OnError, 1);
	}
	done:
	if (Scanner->Token == Token) {
		Scanner->Token = MLT_NONE;
		return 1;
	} else {
		return 0;
	}
}

static void ml_accept(mlc_scanner_t *Scanner, ml_token_t Token) {
	while (ml_parse(Scanner, MLT_EOL));
	if (ml_parse(Scanner, Token)) return;
	Scanner->Error = ml_error("ParseError", "expected %s not %s", MLTokens[Token], MLTokens[Scanner->Token]);
	ml_error_trace_add(Scanner->Error, Scanner->Source);
	longjmp(Scanner->OnError, 1);
}

static const char **ml_ra_accept_schema_filter(mlc_scanner_t *Scanner, int Index, mlc_expr_t **ExprSlot) {
	mlc_expr_t *Expr = ml_accept_expression(Scanner, EXPR_DEFAULT);
	mlc_expr_t *NameExpr, *ValueExpr;
	if (Expr->compile == (void *)ml_assign_expr_compile) {
		NameExpr = ((mlc_parent_expr_t *)Expr)->Child;
		ValueExpr = NameExpr->Next;
	} else {
		NameExpr = ValueExpr = Expr;
	}
	if (NameExpr->compile != (void *)ml_ident_expr_compile) {
		Scanner->Error = ml_error("ParseError", "expected valid alias");
		ml_error_trace_add(Scanner->Error, Scanner->Source);
		longjmp(Scanner->OnError, 1);
	}
	const char *Name = ((mlc_ident_expr_t *)NameExpr)->Ident;
	ExprSlot[0] = ValueExpr;
	ExprSlot = &ValueExpr->Next;
	if (ml_parse(Scanner, MLT_COMMA)) {
		const char **FieldNames = ml_ra_accept_schema_filter(Scanner, Index + 1, ExprSlot);
		FieldNames[Index] = Name;
		return FieldNames;
	} else {
		const char **FieldNames = anew(const char *, Index + 2);
		FieldNames[Index] = Name;
		return FieldNames;
	}
}

static ra_schema_field_t **ml_ra_accept_schema_updates(mlc_scanner_t *Scanner, ra_schema_t *Schema, int Index, mlc_expr_t **ExprSlot) {
	mlc_expr_t *Expr = ml_accept_expression(Scanner, EXPR_DEFAULT);
	mlc_expr_t *NameExpr, *ValueExpr;
	if (Expr->compile == (void *)ml_assign_expr_compile) {
		NameExpr = ((mlc_parent_expr_t *)Expr)->Child;
		ValueExpr = NameExpr->Next;
	} else {
		NameExpr = ValueExpr = Expr;
	}
	if (NameExpr->compile != (void *)ml_ident_expr_compile) {
		Scanner->Error = ml_error("ParseError", "expected valid alias");
		ml_error_trace_add(Scanner->Error, Scanner->Source);
		longjmp(Scanner->OnError, 1);
	}
	const char *FieldName = ((mlc_ident_expr_t *)NameExpr)->Ident;
	ra_schema_field_t *Field = ra_schema_field_by_name(Schema, FieldName) ?: ra_schema_value_field_create(Schema, FieldName);
	ExprSlot[0] = ValueExpr;
	ExprSlot = &ValueExpr->Next;
	if (ml_parse(Scanner, MLT_COMMA)) {
		ra_schema_field_t **Fields = ml_ra_accept_schema_updates(Scanner, Schema, Index + 1, ExprSlot);
		Fields[Index] = Field;
		return Fields;
	} else {
		ra_schema_field_t **Fields = anew(ra_schema_field_t *, Index + 2);
		Fields[Index] = Field;
		return Fields;
	}
}

static ra_schema_field_t **ml_ra_accept_schema_fields(mlc_scanner_t *Scanner, ra_schema_t *Schema, int Index, mlc_decl_t **DeclSlot, int *NumFields) {
	mlc_expr_t *Expr = ml_accept_expression(Scanner, EXPR_DEFAULT);
	mlc_expr_t *AliasExpr, *FieldExpr;
	if (Expr->compile == (void *)ml_assign_expr_compile) {
		AliasExpr = ((mlc_parent_expr_t *)Expr)->Child;
		FieldExpr = AliasExpr->Next;
	} else {
		AliasExpr = FieldExpr = Expr;
	}
	if (AliasExpr->compile != (void *)ml_ident_expr_compile) {
		Scanner->Error = ml_error("ParseError", "expected valid alias");
		ml_error_trace_add(Scanner->Error, Scanner->Source);
		longjmp(Scanner->OnError, 1);
	}
	mlc_decl_t *Decl = DeclSlot[0] = new(mlc_decl_t);
	ra_schema_field_t *Field;
	Decl->Ident = ((mlc_ident_expr_t *)AliasExpr)->Ident;
	if (FieldExpr->compile == (void *)ml_old_expr_compile) {
		Field = InstanceField;
	} else if (FieldExpr->compile == (void *)ml_ident_expr_compile) {
		const char *FieldName = ((mlc_ident_expr_t *)FieldExpr)->Ident;
		Field = ra_schema_field_by_name(Schema, FieldName) ?: ra_schema_value_field_create(Schema, FieldName);
	} else {
		Scanner->Error = ml_error("ParseError", "expected valid field");
		ml_error_trace_add(Scanner->Error, Scanner->Source);
		longjmp(Scanner->OnError, 1);
	}
	if (ml_parse(Scanner, MLT_COMMA)) {
		ra_schema_field_t **Fields = ml_ra_accept_schema_fields(Scanner, Schema, Index + 1, &Decl->Next, NumFields);
		Fields[Index] = Field;
		return Fields;
	} else {
		ra_schema_field_t **Fields = anew(ra_schema_field_t *, Index + 1);
		NumFields[0] = Index + 1;
		Fields[Index] = Field;
		return Fields;
	}
}

static ra_listener_template_t *ml_ra_accept_listener_template(mlc_scanner_t *Scanner, int Index, mlc_decl_t **ParamsSlot, mlc_expr_t **ExprSlot) {
	int Negated = ml_parse(Scanner, MLT_NOT);
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ml_accept(Scanner, MLT_LEFT_SQUARE);
	mlc_fun_expr_t *IndexFunctionExpr = new(mlc_fun_expr_t);
	IndexFunctionExpr->Source = Scanner->Source;
	IndexFunctionExpr->compile = ml_fun_expr_compile;
	mlc_parent_expr_t *ReturnExpr = new(mlc_parent_expr_t);
	ReturnExpr->compile = ml_return_expr_compile;
	ReturnExpr->Source = Scanner->Source;
	IndexFunctionExpr->Body = (mlc_expr_t *)ReturnExpr;
	IndexFunctionExpr->Params = ParamsSlot[0];
	mlc_const_call_expr_t *ListExpr = new(mlc_const_call_expr_t);
	ListExpr->compile = ml_const_call_expr_compile;
	ListExpr->Source = Scanner->Source;
	ListExpr->Value = (ml_value_t *)ListNew;
	const char **FieldNames = ml_ra_accept_schema_filter(Scanner, 0, &ListExpr->Child);
	ReturnExpr->Child = (mlc_expr_t *)ListExpr;
	ml_accept(Scanner, MLT_RIGHT_SQUARE);
	ExprSlot[0] = (mlc_expr_t *)IndexFunctionExpr;
	ExprSlot = &IndexFunctionExpr->Next;
	ra_schema_index_t *SchemaIndex = ra_schema_index_by_names(Schema, FieldNames) ?: ra_schema_index_create(Schema, FieldNames);
	ra_schema_field_t **Fields = 0;
	int NumFields = 0;
	mlc_decl_t *NewParams = 0, **NewParamSlot = &NewParams;
	for (mlc_decl_t *Param = ParamsSlot[0]; Param; Param = Param->Next) {
		mlc_decl_t *NewParam = NewParamSlot[0] = new(mlc_decl_t);
		NewParam->Ident = Param->Ident;
		NewParamSlot = &NewParam->Next;
	}
	ParamsSlot[0] = NewParams;
	if (!Negated && ml_parse(Scanner, MLT_LEFT_PAREN)) {
		NewParamSlot = ParamsSlot;
		while (NewParamSlot[0]) NewParamSlot = &NewParamSlot[0]->Next;
		Fields = ml_ra_accept_schema_fields(Scanner, Schema, 0, NewParamSlot, &NumFields);
		ml_accept(Scanner, MLT_RIGHT_PAREN);
	}
	ra_listener_template_t *Template;
	if (ml_parse(Scanner, MLT_COMMA)) {
		Template = ml_ra_accept_listener_template(Scanner, Index + 1, ParamsSlot, ExprSlot);
	} else {
		Template = xnew(ra_listener_template_t, Index + 1, ra_schema_listener_template_t);
		Template->NumSchemas = Index + 1;
	}
	Template->Schemas[Index].Schema = Schema;
	Template->Schemas[Index].Index = SchemaIndex;
	Template->Schemas[Index].SelectedFields = Fields;
	Template->Schemas[Index].NumSelectedFields = NumFields;
	Template->Schemas[Index].Negated = Negated;
	return Template;
}

static mlc_expr_t *ml_ra_accept_when_expr(mlc_scanner_t *Scanner) {
	int Negated = ml_parse(Scanner, MLT_DELETE);
	int Created = ml_parse(Scanner, MLT_INSERT);
	mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
	CallExpr->compile = ml_const_call_expr_compile;
	CallExpr->Source = Scanner->Source;
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ra_schema_index_t *SchemaIndex = 0;
	if (ml_parse(Scanner, MLT_LEFT_SQUARE)) {
		const char **FieldNames = ml_ra_accept_schema_filter(Scanner, 0, &CallExpr->Child);
		SchemaIndex = ra_schema_index_by_names(Schema, FieldNames) ?: ra_schema_index_create(Schema, FieldNames);
		ml_accept(Scanner, MLT_RIGHT_SQUARE);
	}
	mlc_decl_t *Params = 0;
	ra_schema_field_t **Fields = 0;
	int NumFields = 0;
	if (ml_parse(Scanner, MLT_LEFT_PAREN)) {
		Fields = ml_ra_accept_schema_fields(Scanner, Schema, 0, &Params, &NumFields);
		ml_accept(Scanner, MLT_RIGHT_PAREN);
	}
	ra_listener_template_t *Template;
	if (ml_parse(Scanner, MLT_COMMA)) {
		mlc_expr_t **ExprSlot = &CallExpr->Child;
		while (ExprSlot[0]) ExprSlot = &ExprSlot[0]->Next;
		Template = ml_ra_accept_listener_template(Scanner, 1, &Params, ExprSlot);
	} else {
		Template = xnew(ra_listener_template_t, 1, ra_schema_listener_template_t);
		Template->NumSchemas = 1;
	}
	Template->Schemas[0].Schema = Schema;
	Template->Schemas[0].Index = SchemaIndex;
	Template->Schemas[0].SelectedFields = Fields;
	Template->Schemas[0].NumSelectedFields = NumFields;
	Template->Schemas[0].Negated = Negated;
	Template->Schemas[0].Created = Created;
	ml_accept(Scanner, MLT_DO);
	mlc_fun_expr_t *FunExpr = new(mlc_fun_expr_t);
	FunExpr->compile = ml_fun_expr_compile;
	FunExpr->Source = Scanner->Source;
	FunExpr->Params = Params;
	FunExpr->Body = ml_accept_block(Scanner);
	ml_accept(Scanner, MLT_END);
	mlc_expr_t **ExprSlot = &CallExpr->Child;
	while (ExprSlot[0]) ExprSlot = &ExprSlot[0]->Next;
	ExprSlot[0] = (mlc_expr_t *)FunExpr;
	CallExpr->Value = ml_function(Template, (void *)ra_listener_create_callback);
	return (mlc_expr_t *)CallExpr;
}

static mlc_expr_t *ml_ra_accept_exists_expr(mlc_scanner_t *Scanner) {
	int Negated = ml_parse(Scanner, MLT_NOT);
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ml_accept(Scanner, MLT_LEFT_SQUARE);
	mlc_const_call_expr_t *ExistsCallExpr = new(mlc_const_call_expr_t);
	ExistsCallExpr->compile = ml_const_call_expr_compile;
	ExistsCallExpr->Source = Scanner->Source;
	const char **FieldNames = ml_ra_accept_schema_filter(Scanner, 0, &ExistsCallExpr->Child);
	ml_accept(Scanner, MLT_RIGHT_SQUARE);
	ra_schema_index_t *SchemaIndex = ra_schema_index_by_names(Schema, FieldNames) ?: ra_schema_index_create(Schema, FieldNames);
	ExistsCallExpr->Value = ml_function(SchemaIndex, (void *)ra_index_instance_exists_callback);
	mlc_ra_exists_expr_t *ExistsExpr = new(mlc_ra_exists_expr_t);
	ExistsExpr->compile = ml_ra_exists_expr_compile;
	ExistsExpr->Exists = (mlc_expr_t *)ExistsCallExpr;
	if (!Negated && ml_parse(Scanner, MLT_LEFT_PAREN)) {
		ExistsExpr->Fields = ml_ra_accept_schema_fields(Scanner, Schema, 0, &ExistsExpr->Decl, &ExistsExpr->NumFields);
		ml_accept(Scanner, MLT_RIGHT_PAREN);
	}
	mlc_expr_t **ThenSlot = Negated ? &ExistsExpr->Else : &ExistsExpr->Then;
	mlc_expr_t **ElseSlot = Negated ? &ExistsExpr->Then : &ExistsExpr->Else;
	if (ml_parse(Scanner, MLT_COMMA)) {
		ThenSlot[0] = ml_ra_accept_exists_expr(Scanner);
	} else {
		ml_accept(Scanner, MLT_THEN);
		ThenSlot[0] = ml_accept_block(Scanner);
		if (ml_parse(Scanner, MLT_ELSE)) ElseSlot[0] = ml_accept_block(Scanner);
		ml_accept(Scanner, MLT_END);
	}
	return (mlc_expr_t *)ExistsExpr;
}

static mlc_expr_t *ml_ra_accept_insert_expr(mlc_scanner_t *Scanner) {
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ml_accept(Scanner, MLT_LEFT_PAREN);
	mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
	CallExpr->compile = ml_const_call_expr_compile;
	CallExpr->Source = Scanner->Source;
	ra_instance_template_t *Template = new(ra_instance_template_t);
	Template->Schema = Schema;
	Template->Fields = ml_ra_accept_schema_updates(Scanner, Schema, 0, &CallExpr->Child);
	while (Template->Fields[Template->NumFields]) ++Template->NumFields;
	ml_accept(Scanner, MLT_RIGHT_PAREN);
	CallExpr->Value = ml_function(Template, (void *)ra_instance_create_callback);
	return (mlc_expr_t *)CallExpr;
}

static mlc_expr_t *ml_ra_accept_signal_expr(mlc_scanner_t *Scanner) {
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ml_accept(Scanner, MLT_LEFT_PAREN);
	mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
	CallExpr->compile = ml_const_call_expr_compile;
	CallExpr->Source = Scanner->Source;
	ra_instance_template_t *Template = new(ra_instance_template_t);
	Template->Schema = Schema;
	Template->Fields = ml_ra_accept_schema_updates(Scanner, Schema, 0, &CallExpr->Child);
	while (Template->Fields[Template->NumFields]) ++Template->NumFields;
	ml_accept(Scanner, MLT_RIGHT_PAREN);
	CallExpr->Value = ml_function(Template, (void *)ra_instance_signal_callback);
	return (mlc_expr_t *)CallExpr;
}

static mlc_expr_t *ml_ra_accept_update_expr(mlc_scanner_t *Scanner) {
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ml_accept(Scanner, MLT_LEFT_SQUARE);
	mlc_const_call_expr_t *ExistsCallExpr = new(mlc_const_call_expr_t);
	ExistsCallExpr->compile = ml_const_call_expr_compile;
	ExistsCallExpr->Source = Scanner->Source;
	const char **FieldNames = ml_ra_accept_schema_filter(Scanner, 0, &ExistsCallExpr->Child);
	ml_accept(Scanner, MLT_RIGHT_SQUARE);
	ra_schema_index_t *SchemaIndex = ra_schema_index_by_names(Schema, FieldNames) ?: ra_schema_index_create(Schema, FieldNames);
	ExistsCallExpr->Value = ml_function(SchemaIndex, (void *)ra_index_instance_exists_callback);
	ml_accept(Scanner, MLT_LEFT_PAREN);
	ra_schema_field_t **Fields = ml_ra_accept_schema_updates(Scanner, Schema, 0, &ExistsCallExpr->Next);
	ml_accept(Scanner, MLT_RIGHT_PAREN);
	mlc_const_call_expr_t *UpdateCallExpr = new(mlc_const_call_expr_t);
	UpdateCallExpr->compile = ml_const_call_expr_compile;
	UpdateCallExpr->Source = Scanner->Source;
	UpdateCallExpr->Child = (mlc_expr_t *)ExistsCallExpr;
	UpdateCallExpr->Value = ml_function(Fields, (void *)ra_index_instance_update_callback);
	return (mlc_expr_t *)UpdateCallExpr;
}

static mlc_expr_t *ml_ra_accept_delete_expr(mlc_scanner_t *Scanner) {
	ml_accept(Scanner, MLT_IDENT);
	ra_schema_t *Schema = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
	ml_accept(Scanner, MLT_LEFT_SQUARE);
	mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
	CallExpr->compile = ml_const_call_expr_compile;
	CallExpr->Source = Scanner->Source;
	const char **FieldNames = ml_ra_accept_schema_filter(Scanner, 0, &CallExpr->Child);
	ml_accept(Scanner, MLT_RIGHT_SQUARE);
	ra_schema_index_t *SchemaIndex = ra_schema_index_by_names(Schema, FieldNames) ?: ra_schema_index_create(Schema, FieldNames);
	CallExpr->Value = ml_function(SchemaIndex, (void *)ra_index_instance_delete_callback);
	return (mlc_expr_t *)CallExpr;
}

static mlc_expr_t *ml_parse_term(mlc_scanner_t *Scanner) {
	if (ml_parse(Scanner, MLT_DO)) {
		mlc_expr_t *Expr = ml_accept_block(Scanner);
		ml_accept(Scanner, MLT_END);
		return Expr;
	} else if (ml_parse(Scanner, MLT_IF)) {
		mlc_if_expr_t *IfExpr = new(mlc_if_expr_t);
		IfExpr->compile = ml_if_expr_compile;
		IfExpr->Source = Scanner->Source;
		mlc_if_case_t **CaseSlot = &IfExpr->Cases;
		do {
			mlc_if_case_t *Case = CaseSlot[0] = new(mlc_if_case_t);
			CaseSlot = &Case->Next;
			Case->Source = Scanner->Source;
			Case->Condition = ml_accept_expression(Scanner, EXPR_DEFAULT);
			ml_accept(Scanner, MLT_THEN);
			Case->Body = ml_accept_block(Scanner);
		} while (ml_parse(Scanner, MLT_ELSEIF));
		if (ml_parse(Scanner, MLT_ELSE)) IfExpr->Else = ml_accept_block(Scanner);
		ml_accept(Scanner, MLT_END);
		return (mlc_expr_t *)IfExpr;
	} else if (ml_parse(Scanner, MLT_LOOP)) {
		mlc_parent_expr_t *LoopExpr = new(mlc_parent_expr_t);
		LoopExpr->compile = ml_loop_expr_compile;
		LoopExpr->Source = Scanner->Source;
		LoopExpr->Child = ml_accept_block(Scanner);
		ml_accept(Scanner, MLT_END);
		return (mlc_expr_t *)LoopExpr;
	} else if (ml_parse(Scanner, MLT_FOR)) {
		mlc_decl_expr_t *ForExpr = new(mlc_decl_expr_t);
		ForExpr->compile = ml_for_expr_compile;
		ForExpr->Source = Scanner->Source;
		int Deref = ml_parse(Scanner, MLT_VAR);
		mlc_decl_t *Decl = new(mlc_decl_t);
		ml_accept(Scanner, MLT_IDENT);
		Decl->Ident = Scanner->Ident;
		int HasKey = 0;
		if (ml_parse(Scanner, MLT_COMMA)) {
			ml_accept(Scanner, MLT_IDENT);
			mlc_decl_t *KeyDecl = new(mlc_decl_t);
			KeyDecl->Ident = Scanner->Ident;
			Decl->Next = KeyDecl;
			HasKey = 1;
		}
		ForExpr->Decl = Decl;
		if (ml_parse(Scanner, MLT_ASSIGN)) {
			ForExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
		} else {
			ml_accept(Scanner, MLT_IN);
			mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
			CallExpr->compile = ml_const_call_expr_compile;
			CallExpr->Source = Scanner->Source;
			CallExpr->Value = ml_method("values");
			CallExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
			ForExpr->Child = (mlc_expr_t *)CallExpr;
		}
		ml_accept(Scanner, MLT_DO);
		ForExpr->Child->Next = ml_accept_block(Scanner);
		if (Deref) {
			mlc_block_expr_t *Block = (mlc_block_expr_t *)ForExpr->Child->Next;
			char *ValueIdent = snew(strlen(Decl->Ident) + 2);
			ValueIdent[0] = '#';
			strcpy(ValueIdent + 1, Decl->Ident);
			mlc_decl_t *ValueDecl = new(mlc_decl_t);
			mlc_ident_expr_t *ValueIdentExpr = new(mlc_ident_expr_t);
			ValueIdentExpr->compile = ml_ident_expr_compile;
			ValueIdentExpr->Source = Scanner->Source;
			ValueIdentExpr->Ident = ValueDecl->Ident = Decl->Ident;
			mlc_ident_expr_t *OldValueIdentExpr = new(mlc_ident_expr_t);
			OldValueIdentExpr->compile = ml_ident_expr_compile;
			OldValueIdentExpr->Source = Scanner->Source;
			OldValueIdentExpr->Ident = ValueIdent;
			mlc_parent_expr_t *ValueAssignExpr = new(mlc_parent_expr_t);
			ValueAssignExpr->compile = ml_assign_expr_compile;
			ValueAssignExpr->Source = Scanner->Source;
			ValueAssignExpr->Child = (mlc_expr_t *)ValueIdentExpr;
			ValueIdentExpr->Next = (mlc_expr_t *)OldValueIdentExpr;
			Decl->Ident = ValueIdent;
			if (HasKey) {
				char *KeyIdent = snew(strlen(Decl->Next->Ident) + 2);
				KeyIdent[0] = '#';
				strcpy(KeyIdent + 1, Decl->Next->Ident);
				mlc_decl_t *KeyDecl = new(mlc_decl_t);
				mlc_ident_expr_t *KeyIdentExpr = new(mlc_ident_expr_t);
				KeyIdentExpr->compile = ml_ident_expr_compile;
				KeyIdentExpr->Source = Scanner->Source;
				KeyIdentExpr->Ident = KeyDecl->Ident = Decl->Next->Ident;
				mlc_ident_expr_t *OldKeyIdentExpr = new(mlc_ident_expr_t);
				OldKeyIdentExpr->compile = ml_ident_expr_compile;
				OldKeyIdentExpr->Source = Scanner->Source;
				OldKeyIdentExpr->Ident = KeyIdent;
				mlc_parent_expr_t *KeyAssignExpr = new(mlc_parent_expr_t);
				KeyAssignExpr->compile = ml_assign_expr_compile;
				KeyAssignExpr->Source = Scanner->Source;
				KeyAssignExpr->Child = (mlc_expr_t *)KeyIdentExpr;
				KeyIdentExpr->Next = (mlc_expr_t *)OldKeyIdentExpr;
				Decl->Next->Ident = KeyIdent;
				ValueDecl->Next = KeyDecl;
				KeyDecl->Next = Block->Decl;
				ValueAssignExpr->Next = (mlc_expr_t *)KeyAssignExpr;
				KeyAssignExpr->Next = Block->Child;
			} else {
				ValueDecl->Next = Block->Decl;
				ValueAssignExpr->Next = Block->Child;
			}
			Block->Decl = ValueDecl;
			Block->Child = (mlc_expr_t *)ValueAssignExpr;
		}
		if (ml_parse(Scanner, MLT_ELSE)) {
			ForExpr->Child->Next->Next = ml_accept_block(Scanner);
		}
		ml_accept(Scanner, MLT_END);
		return (mlc_expr_t *)ForExpr;
	} else if (ml_parse(Scanner, MLT_ALL)) {
		mlc_parent_expr_t *AllExpr = new(mlc_parent_expr_t);
		AllExpr->compile = ml_all_expr_compile;
		AllExpr->Source = Scanner->Source;
		AllExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
		return (mlc_expr_t *)AllExpr;
	} else if (ml_parse(Scanner, MLT_NOT)) {
		mlc_parent_expr_t *NotExpr = new(mlc_parent_expr_t);
		NotExpr->compile = ml_not_expr_compile;
		NotExpr->Source = Scanner->Source;
		NotExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
		return (mlc_expr_t *)NotExpr;
	} else if (ml_parse(Scanner, MLT_WHILE)) {
		mlc_parent_expr_t *WhileExpr = new(mlc_parent_expr_t);
		WhileExpr->compile = ml_while_expr_compile;
		WhileExpr->Source = Scanner->Source;
		WhileExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
		return (mlc_expr_t *)WhileExpr;
	} else if (ml_parse(Scanner, MLT_UNTIL)) {
		mlc_parent_expr_t *UntilExpr = new(mlc_parent_expr_t);
		UntilExpr->compile = ml_until_expr_compile;
		UntilExpr->Source = Scanner->Source;
		UntilExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
		return (mlc_expr_t *)UntilExpr;
	} else if (ml_parse(Scanner, MLT_EXIT)) {
		mlc_parent_expr_t *ExitExpr = new(mlc_parent_expr_t);
		ExitExpr->compile = ml_exit_expr_compile;
		ExitExpr->Source = Scanner->Source;
		ExitExpr->Child = ml_parse_expression(Scanner, EXPR_DEFAULT);
		return (mlc_expr_t *)ExitExpr;
	} else if (ml_parse(Scanner, MLT_NEXT)) {
		mlc_expr_t *NextExpr = new(mlc_expr_t);
		NextExpr->compile = ml_next_expr_compile;
		NextExpr->Source = Scanner->Source;
		return NextExpr;
	} else if (ml_parse(Scanner, MLT_FUN)) {
		mlc_fun_expr_t *FunExpr = new(mlc_fun_expr_t);
		FunExpr->compile = ml_fun_expr_compile;
		FunExpr->Source = Scanner->Source;
		ml_accept(Scanner, MLT_LEFT_PAREN);
		if (!ml_parse(Scanner, MLT_RIGHT_PAREN)) {
			mlc_decl_t **ParamSlot = &FunExpr->Params;
			do {
				ml_accept(Scanner, MLT_IDENT);
				mlc_decl_t *Param = ParamSlot[0] = new(mlc_decl_t);
				ParamSlot = &Param->Next;
				Param->Ident = Scanner->Ident;
				if (ml_parse(Scanner, MLT_LEFT_SQUARE)) {
					ml_accept(Scanner, MLT_RIGHT_SQUARE);
					Param->Index = 1;
					break;
				}
			} while (ml_parse(Scanner, MLT_COMMA));
			ml_accept(Scanner, MLT_RIGHT_PAREN);
		}
		/*if (ml_parse(Scanner, MLT_DO)) {
			FunExpr->Body = ml_accept_block(Scanner);
			ml_accept(Scanner, MLT_END);
		} else {*/
			FunExpr->Body = ml_accept_expression(Scanner, EXPR_DEFAULT);
		//}
		return (mlc_expr_t *)FunExpr;
	} else if (ml_parse(Scanner, MLT_RETURN)) {
		mlc_parent_expr_t *ReturnExpr = new(mlc_parent_expr_t);
		ReturnExpr->compile = ml_return_expr_compile;
		ReturnExpr->Source = Scanner->Source;
		ReturnExpr->Child = ml_parse_expression(Scanner, EXPR_DEFAULT);
		return (mlc_expr_t *)ReturnExpr;
	} else if (ml_parse(Scanner, MLT_WITH)) {
		mlc_decl_expr_t *WithExpr = new(mlc_decl_expr_t);
		WithExpr->compile = ml_with_expr_compile;
		WithExpr->Source = Scanner->Source;
		mlc_decl_t **DeclSlot = &WithExpr->Decl;
		mlc_expr_t **ExprSlot = &WithExpr->Child;
		do {
			ml_accept(Scanner, MLT_IDENT);
			mlc_decl_t *Decl = DeclSlot[0] = new(mlc_decl_t);
			DeclSlot = &Decl->Next;
			Decl->Ident = Scanner->Ident;
			ml_accept(Scanner, MLT_ASSIGN);
			mlc_expr_t *Expr = ExprSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
			ExprSlot = &Expr->Next;
		} while (ml_parse(Scanner, MLT_COMMA));
		ml_accept(Scanner, MLT_DO);
		ExprSlot[0] = ml_accept_block(Scanner);
		ml_accept(Scanner, MLT_END);
		return (mlc_expr_t *)WithExpr;
	} else if (ml_parse(Scanner, MLT_IDENT)) {
		mlc_ident_expr_t *IdentExpr = new(mlc_ident_expr_t);
		IdentExpr->compile = ml_ident_expr_compile;
		IdentExpr->Source = Scanner->Source;
		IdentExpr->Ident = Scanner->Ident;
		return (mlc_expr_t *)IdentExpr;
	} else if (ml_parse(Scanner, MLT_VALUE)) {
		mlc_value_expr_t *ValueExpr = new(mlc_value_expr_t);
		ValueExpr->compile = ml_value_expr_compile;
		ValueExpr->Source = Scanner->Source;
		ValueExpr->Value = Scanner->Value;
		return (mlc_expr_t *)ValueExpr;
	} else if (ml_parse(Scanner, MLT_EXPR)) {
		return Scanner->Expr;
	} else if (ml_parse(Scanner, MLT_NIL)) {
		mlc_value_expr_t *ValueExpr = new(mlc_value_expr_t);
		ValueExpr->compile = ml_value_expr_compile;
		ValueExpr->Source = Scanner->Source;
		ValueExpr->Value = MLNil;
		return (mlc_expr_t *)ValueExpr;
	} else if (ml_parse(Scanner, MLT_LEFT_PAREN)) {
		mlc_expr_t *Expr = ml_accept_expression(Scanner, EXPR_DEFAULT);
		ml_accept(Scanner, MLT_RIGHT_PAREN);
		return Expr;
	} else if (ml_parse(Scanner, MLT_LEFT_SQUARE)) {
		mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
		CallExpr->compile = ml_const_call_expr_compile;
		CallExpr->Source = Scanner->Source;
		CallExpr->Value = (ml_value_t *)ListNew;
		mlc_expr_t **ArgsSlot = &CallExpr->Child;
		if (!ml_parse(Scanner, MLT_RIGHT_SQUARE)) {
			do {
				mlc_expr_t *Arg = ArgsSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
				ArgsSlot = &Arg->Next;
			} while (ml_parse(Scanner, MLT_COMMA));
			ml_accept(Scanner, MLT_RIGHT_SQUARE);
		}
		return (mlc_expr_t *)CallExpr;
	} else if (ml_parse(Scanner, MLT_LEFT_BRACE)) {
		mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
		CallExpr->compile = ml_const_call_expr_compile;
		CallExpr->Source = Scanner->Source;
		CallExpr->Value = (ml_value_t *)TreeNew;
		mlc_expr_t **ArgsSlot = &CallExpr->Child;
		if (!ml_parse(Scanner, MLT_RIGHT_BRACE)) {
			do {
				mlc_expr_t *Arg = ArgsSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
				ArgsSlot = &Arg->Next;
				if (ml_parse(Scanner, MLT_IS)) {
					mlc_expr_t *ArgExpr = ArgsSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
					ArgsSlot = &ArgExpr->Next;
				} else {
					mlc_value_expr_t *ArgExpr = new(mlc_value_expr_t);
					ArgExpr->compile = ml_value_expr_compile;
					ArgExpr->Source = Scanner->Source;
					ArgExpr->Value = MLNil;
					ArgsSlot[0] = (mlc_expr_t *)ArgExpr;
					ArgsSlot = &ArgExpr->Next;
				}
			} while (ml_parse(Scanner, MLT_COMMA));
			ml_accept(Scanner, MLT_RIGHT_BRACE);
		}
		return (mlc_expr_t *)CallExpr;
	} else if (ml_parse(Scanner, MLT_OLD)) {
		mlc_expr_t *OldExpr = new(mlc_expr_t);
		OldExpr->compile = ml_old_expr_compile;
		OldExpr->Source = Scanner->Source;
		return OldExpr;
	} else if (ml_parse(Scanner, MLT_OPERATOR)) {
		mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
		CallExpr->compile = ml_const_call_expr_compile;
		CallExpr->Source = Scanner->Source;
		CallExpr->Value = (ml_value_t *)ml_method(Scanner->Ident);
		CallExpr->Child = ml_accept_term(Scanner);
		return (mlc_expr_t *)CallExpr;
	} else if (ml_parse(Scanner, MLT_METHOD)) {
		mlc_value_expr_t *ValueExpr = new(mlc_value_expr_t);
		ValueExpr->compile = ml_value_expr_compile;
		ValueExpr->Source = Scanner->Source;
		ValueExpr->Value = (ml_value_t *)ml_method(Scanner->Ident);
		return (mlc_expr_t *)ValueExpr;
	} else if (ml_parse(Scanner, MLT_WHEN)) {
		return ml_ra_accept_when_expr(Scanner);
	} else if (ml_parse(Scanner, MLT_EXISTS)) {
		return ml_ra_accept_exists_expr(Scanner);
	} else if (ml_parse(Scanner, MLT_INSERT)) {
		return ml_ra_accept_insert_expr(Scanner);
	} else if (ml_parse(Scanner, MLT_SIGNAL)) {
			return ml_ra_accept_signal_expr(Scanner);
	} else if (ml_parse(Scanner, MLT_UPDATE)) {
		return ml_ra_accept_update_expr(Scanner);
	} else if (ml_parse(Scanner, MLT_DELETE)) {
		return ml_ra_accept_delete_expr(Scanner);
	}
	return NULL;
}

static mlc_expr_t *ml_accept_term(mlc_scanner_t *Scanner) {
	while (ml_parse(Scanner, MLT_EOL));
	mlc_expr_t *Expr = ml_parse_term(Scanner);
	if (Expr) return Expr;
	Scanner->Error = ml_error("ParseError", "expected <term> not %s", MLTokens[Scanner->Token]);
	ml_error_trace_add(Scanner->Error, Scanner->Source);
	longjmp(Scanner->OnError, 1);
}

static mlc_expr_t *ml_parse_factor(mlc_scanner_t *Scanner) {
	mlc_expr_t *Expr = ml_parse_term(Scanner);
	if (!Expr) return NULL;
	for (;;) {
		if (ml_parse(Scanner, MLT_LEFT_PAREN)) {
			mlc_parent_expr_t *CallExpr = new(mlc_parent_expr_t);
			CallExpr->compile = ml_call_expr_compile;
			CallExpr->Source = Scanner->Source;
			CallExpr->Child = Expr;
			mlc_expr_t **ArgsSlot = &Expr->Next;
			if (!ml_parse(Scanner, MLT_RIGHT_PAREN)) {
				do {
					mlc_expr_t *Arg = ArgsSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
					ArgsSlot = &Arg->Next;
				} while (ml_parse(Scanner, MLT_COMMA));
				ml_accept(Scanner, MLT_RIGHT_PAREN);
			}
			if (ml_parse(Scanner, MLT_DO)) {
				mlc_fun_expr_t *FunExpr = new(mlc_fun_expr_t);
				FunExpr->compile = ml_fun_expr_compile;
				FunExpr->Source = Scanner->Source;
				FunExpr->Body = ml_accept_block(Scanner);
				ml_accept(Scanner, MLT_END);
				ArgsSlot[0] = (mlc_expr_t *)FunExpr;
			}
			Expr = (mlc_expr_t *)CallExpr;
		} else if (ml_parse(Scanner, MLT_LEFT_SQUARE)) {
			mlc_const_call_expr_t *IndexExpr = new(mlc_const_call_expr_t);
			IndexExpr->compile = ml_const_call_expr_compile;
			IndexExpr->Value = ml_method("[]");
			IndexExpr->Source = Scanner->Source;
			IndexExpr->Child = Expr;
			mlc_expr_t **ArgsSlot = &Expr->Next;
			if (!ml_parse(Scanner, MLT_RIGHT_SQUARE)) {
				do {
					mlc_expr_t *Arg = ArgsSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
					ArgsSlot = &Arg->Next;
				} while (ml_parse(Scanner, MLT_COMMA));
				ml_accept(Scanner, MLT_RIGHT_SQUARE);
			}
			Expr = (mlc_expr_t *)IndexExpr;
		} else if (ml_parse(Scanner, MLT_METHOD)) {
			mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
			CallExpr->compile = ml_const_call_expr_compile;
			CallExpr->Source = Scanner->Source;
			CallExpr->Value = (ml_value_t *)ml_method(Scanner->Ident);
			CallExpr->Child = Expr;
			if (ml_parse(Scanner, MLT_LEFT_PAREN) && !ml_parse(Scanner, MLT_RIGHT_PAREN)) {
				mlc_expr_t **ArgsSlot = &Expr->Next;
				if (!ml_parse(Scanner, MLT_RIGHT_PAREN)) {
					do {
						mlc_expr_t *Arg = ArgsSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
						ArgsSlot = &Arg->Next;
					} while (ml_parse(Scanner, MLT_COMMA));
					ml_accept(Scanner, MLT_RIGHT_PAREN);
				}
			}
			Expr = (mlc_expr_t *)CallExpr;
		} else {
			return Expr;
		}
	}
	return NULL; // Unreachable
}

static mlc_expr_t *ml_accept_factor(mlc_scanner_t *Scanner) {
	while (ml_parse(Scanner, MLT_EOL));
	mlc_expr_t *Expr = ml_parse_factor(Scanner);
	if (Expr) return Expr;
	Scanner->Error = ml_error("ParseError", "expected <factor> not %s", MLTokens[Scanner->Token]);
	ml_error_trace_add(Scanner->Error, Scanner->Source);
	longjmp(Scanner->OnError, 1);
}

static mlc_expr_t *ml_parse_expression(mlc_scanner_t *Scanner, ml_expr_level_t Level) {
	mlc_expr_t *Expr = ml_parse_factor(Scanner);
	if (!Expr) return NULL;
	for (;;) if (ml_parse(Scanner, MLT_OPERATOR)) {
		mlc_const_call_expr_t *CallExpr = new(mlc_const_call_expr_t);
		CallExpr->compile = ml_const_call_expr_compile;
		CallExpr->Source = Scanner->Source;
		CallExpr->Value = (ml_value_t *)ml_method(Scanner->Ident);
		CallExpr->Child = Expr;
		Expr->Next = ml_accept_factor(Scanner);
		Expr = (mlc_expr_t *)CallExpr;
	} else if (ml_parse(Scanner, MLT_ASSIGN)) {
		mlc_parent_expr_t *AssignExpr = new(mlc_parent_expr_t);
		AssignExpr->compile = ml_assign_expr_compile;
		AssignExpr->Source = Scanner->Source;
		AssignExpr->Child = Expr;
		Expr->Next = ml_accept_expression(Scanner, EXPR_DEFAULT);
		Expr = (mlc_expr_t *)AssignExpr;
	} else {
		break;
	}
	if (Level >= EXPR_AND && ml_parse(Scanner, MLT_AND)) {
		mlc_parent_expr_t *AndExpr = new(mlc_parent_expr_t);
		AndExpr->compile = ml_and_expr_compile;
		AndExpr->Source = Scanner->Source;
		mlc_expr_t *LastChild = AndExpr->Child = Expr;
		do {
			LastChild = LastChild->Next = ml_accept_expression(Scanner, EXPR_SIMPLE);
		} while (ml_parse(Scanner, MLT_AND));
		Expr = (mlc_expr_t *)AndExpr;
	}
	if (Level >= EXPR_OR && ml_parse(Scanner, MLT_OR)) {
		mlc_parent_expr_t *OrExpr = new(mlc_parent_expr_t);
		OrExpr->compile = ml_or_expr_compile;
		OrExpr->Source = Scanner->Source;
		mlc_expr_t *LastChild = OrExpr->Child = Expr;
		do {
			LastChild = LastChild->Next = ml_accept_expression(Scanner, EXPR_AND);
		} while (ml_parse(Scanner, MLT_OR));
		Expr = (mlc_expr_t *)OrExpr;
	}
	return Expr;
}

static mlc_expr_t *ml_accept_expression(mlc_scanner_t *Scanner, ml_expr_level_t Level) {
	while (ml_parse(Scanner, MLT_EOL));
	mlc_expr_t *Expr = ml_parse_expression(Scanner, Level);
	if (Expr) return Expr;
	Scanner->Error = ml_error("ParseError", "expected <expression> not %s", MLTokens[Scanner->Token]);
	ml_error_trace_add(Scanner->Error, Scanner->Source);
	longjmp(Scanner->OnError, 1);
}

static const char **ml_accept_ident_list(mlc_scanner_t *Scanner, int Index) {
	const char *Ident = Scanner->Ident;
	const char **Idents;
	if (ml_parse(Scanner, MLT_COMMA)) {
		ml_accept(Scanner, MLT_IDENT);
		Idents = ml_accept_ident_list(Scanner, Index + 1);
	} else {
		Idents = anew(const char *, Index + 2);
	}
	Idents[Index] = Ident;
	return Idents;
}

static mlc_expr_t *ml_accept_block(mlc_scanner_t *Scanner) {
	mlc_block_expr_t *BlockExpr = new(mlc_block_expr_t);
	BlockExpr->compile = ml_block_expr_compile;
	BlockExpr->Source = Scanner->Source;
	mlc_expr_t **ExprSlot = &BlockExpr->Child;
	mlc_decl_t **DeclSlot = &BlockExpr->Decl;
	for (;;) {
		while (ml_parse(Scanner, MLT_EOL));
		if (ml_parse(Scanner, MLT_VAR)) {
			do {
				ml_accept(Scanner, MLT_IDENT);
				mlc_decl_t *Decl = DeclSlot[0] = new(mlc_decl_t);
				Decl->Ident = Scanner->Ident;
				DeclSlot = &Decl->Next;
				if (ml_parse(Scanner, MLT_ASSIGN)) {
					mlc_decl_expr_t *DeclExpr = new(mlc_decl_expr_t);
					DeclExpr->compile = ml_var_expr_compile;
					DeclExpr->Source = Scanner->Source;
					DeclExpr->Decl = Decl;
					DeclExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
					ExprSlot[0] = (mlc_expr_t *)DeclExpr;
					ExprSlot = &DeclExpr->Next;
				}
			} while (ml_parse(Scanner, MLT_COMMA));
		} else if (ml_parse(Scanner, MLT_DEF)) {
			ml_accept(Scanner, MLT_IDENT);
			mlc_decl_t *Decl = new(mlc_decl_t);
			Decl->Ident = Scanner->Ident;
			ml_accept(Scanner, MLT_ASSIGN);
			mlc_decl_expr_t *DeclExpr = new(mlc_decl_expr_t);
			DeclExpr->compile = ml_def_expr_compile;
			DeclExpr->Source = Scanner->Source;
			DeclExpr->Decl = Decl;
			DeclExpr->Child = ml_accept_expression(Scanner, EXPR_DEFAULT);
			ExprSlot[0] = (mlc_expr_t *)DeclExpr;
			ExprSlot = &DeclExpr->Next;
		} else if (ml_parse(Scanner, MLT_ON)) {
			if (BlockExpr->CatchDecl) {
				Scanner->Error = ml_error("ParseError", "no more than one error handler allowed in a block");
				ml_error_trace_add(Scanner->Error, Scanner->Source);
				longjmp(Scanner->OnError, 1);
			}
			ml_accept(Scanner, MLT_IDENT);
			mlc_decl_t *Decl = new(mlc_decl_t);
			Decl->Ident = Scanner->Ident;
			BlockExpr->CatchDecl = Decl;
			ml_accept(Scanner, MLT_DO);
			BlockExpr->Catch = ml_accept_block(Scanner);
			//ml_accept(Scanner, MLT_END);
			return (mlc_expr_t *)BlockExpr;
		} else if (ml_parse(Scanner, MLT_SCHEMA)) {
			ml_accept(Scanner, MLT_IDENT);
			const char *SchemaName = Scanner->Ident;
			ra_schema_t *Parent = 0;
			if (ml_parse(Scanner, MLT_LEFT_PAREN)) {
				ml_accept(Scanner, MLT_IDENT);
				Parent = ra_schema_by_name(Scanner->Ident) ?: ra_schema_create(Scanner->Ident, 0);
				ml_accept(Scanner, MLT_RIGHT_PAREN);
			}
			ra_schema_t *Schema = ra_schema_by_name(SchemaName) ?: ra_schema_create(SchemaName, Parent);
			ml_accept(Scanner, MLT_IS);
			for (;;) {
				while (ml_parse(Scanner, MLT_EOL));
				if (ml_parse(Scanner, MLT_VAR)) {
					ml_accept(Scanner, MLT_IDENT);
					if (!ra_schema_field_by_name(Schema, Scanner->Ident)) ra_schema_value_field_create(Schema, Scanner->Ident);
					while (ml_parse(Scanner, MLT_COMMA)) {
						ml_accept(Scanner, MLT_IDENT);
						if (!ra_schema_field_by_name(Schema, Scanner->Ident)) ra_schema_value_field_create(Schema, Scanner->Ident);
					}
				} else if (ml_parse(Scanner, MLT_FUN)) {
					ml_accept(Scanner, MLT_IDENT);
					const char *FieldName = Scanner->Ident;
					ml_accept(Scanner, MLT_LEFT_PAREN);
					const char **FieldNames = ml_parse(Scanner, MLT_IDENT) ? ml_accept_ident_list(Scanner, 0) : anew(const char *, 1);
					ml_accept(Scanner, MLT_RIGHT_PAREN);
					mlc_fun_expr_t *FunExpr = new(mlc_fun_expr_t);
					FunExpr->compile = ml_fun_expr_compile;
					FunExpr->Source = Scanner->Source;
					mlc_decl_t **ParamSlot = &FunExpr->Params;
					for (const char **ParamName = FieldNames; ParamName[0]; ++ParamName) {
						mlc_decl_t *Param = ParamSlot[0] = new(mlc_decl_t);
						ParamSlot = &Param->Next;
						Param->Ident = ParamName[0];
					}
					if (ml_parse(Scanner, MLT_DO)) {
						FunExpr->Body = ml_accept_block(Scanner);
						ml_accept(Scanner, MLT_END);
					} else {
						FunExpr->Body = ml_accept_expression(Scanner, EXPR_DEFAULT);
					}
					mlc_function_t TempFunction[1] = {{0,}};
					SHA256_CTX HashContext[1];
					sha256_init(HashContext);
					mlc_compiled_t Compiled = ml_compile(TempFunction, (mlc_expr_t *)FunExpr, HashContext);
					mlc_connect(Compiled.Exits, 0);
					ml_closure_t *Closure = new(ml_closure_t);
					ml_closure_info_t *Info = Closure->Info = new(ml_closure_info_t);
					Closure->Type = MLClosureT;
					Info->Entry = Compiled.Start;
					Info->FrameSize = TempFunction->Size;
					ra_schema_computed_field_create(Schema, FieldName, (ml_value_t *)Closure, FieldNames);
				} else if (ml_parse(Scanner, MLT_INDEX)) {
					ml_accept(Scanner, MLT_IDENT);
					const char **FieldNames = ml_accept_ident_list(Scanner, 0);
					ra_schema_index_create(Schema, FieldNames);
				} else {
					ml_accept(Scanner, MLT_END);
					break;
				}
			}
		} else {
			mlc_expr_t *Expr = ml_parse_expression(Scanner, EXPR_DEFAULT);
			if (!Expr) return (mlc_expr_t *)BlockExpr;
			ExprSlot[0] = Expr;
			ExprSlot = &Expr->Next;
		}
		ml_parse(Scanner, MLT_SEMICOLON);
	}
	return NULL; // Unreachable
}

static mlc_expr_t *ml_accept_command(mlc_scanner_t *Scanner, stringmap_t *Vars) {
	while (ml_parse(Scanner, MLT_EOL));
	mlc_block_expr_t *BlockExpr = new(mlc_block_expr_t);
	BlockExpr->compile = ml_block_expr_compile;
	BlockExpr->Source = Scanner->Source;
	mlc_expr_t **ExprSlot = &BlockExpr->Child;
	if (ml_parse(Scanner, MLT_EOI)) {
		return (mlc_expr_t *)-1;
	} else if (ml_parse(Scanner, MLT_VAR)) {
		do {
			ml_accept(Scanner, MLT_IDENT);
			const char *Ident = Scanner->Ident;
			ml_value_t *Ref = ml_reference(NULL);
			stringmap_insert(Vars, Ident, Ref);
			if (ml_parse(Scanner, MLT_ASSIGN)) {
				mlc_value_expr_t *RefExpr = new(mlc_value_expr_t);
				RefExpr->compile = ml_value_expr_compile;
				RefExpr->Source = Scanner->Source;
				RefExpr->Value = Ref;
				mlc_parent_expr_t *AssignExpr = new(mlc_parent_expr_t);
				AssignExpr->compile = ml_assign_expr_compile;
				AssignExpr->Source = Scanner->Source;
				AssignExpr->Child = (mlc_expr_t *)RefExpr;
				RefExpr->Next = ml_accept_expression(Scanner, EXPR_DEFAULT);
				ExprSlot[0] = (mlc_expr_t *)AssignExpr;
				ExprSlot = &AssignExpr->Next;
			}
		} while (ml_parse(Scanner, MLT_COMMA));
	} else {
		mlc_expr_t *Expr = ExprSlot[0] = ml_accept_expression(Scanner, EXPR_DEFAULT);
		ExprSlot = &Expr->Next;
	}
	ml_parse(Scanner, MLT_SEMICOLON);
	return (mlc_expr_t *)BlockExpr;
}

ml_type_t *ml_class(ml_type_t *Parent, const char *Name) {
	ml_type_t *Type = new(ml_type_t);
	Type->Parent = Parent;
	Type->Name = Name;
	Type->hash = Parent->hash;
	Type->call = Parent->call;
	Type->deref = Parent->deref;
	Type->assign = Parent->assign;
	Type->next = Parent->next;
	Type->key = Parent->key;
	return Type;
}

static const char *ml_file_read(void *Data) {
	FILE *File = (FILE *)Data;
	char *Line = NULL;
	size_t Length = 0;
	if (getline(&Line, &Length, File) < 0) return NULL;
	return Line;
}

ml_value_t *ml_load(ml_getter_t GlobalGet, void *Globals, const char *FileName) {
	FILE *File = fopen(FileName, "r");
	if (!File) return ml_error("LoadError", "error opening %s", FileName);
	mlc_scanner_t *Scanner = ml_scanner(FileName, File, ml_file_read);
	if (setjmp(Scanner->OnError)) return Scanner->Error;
	mlc_expr_t *Expr = ml_accept_block(Scanner);
	ml_accept(Scanner, MLT_EOI);
	fclose(File);
	mlc_function_t Function[1] = {{GlobalGet, Globals, NULL,}};
	SHA256_CTX HashContext[1];
	sha256_init(HashContext);
	mlc_compiled_t Compiled = ml_compile(Function, Expr, HashContext);
	mlc_connect(Compiled.Exits, NULL);
	ml_closure_t *Closure = new(ml_closure_t);
	ml_closure_info_t *Info = Closure->Info = new(ml_closure_info_t);
	Closure->Type = MLClosureT;
	Info->Entry = Compiled.Start;
	Info->FrameSize = Function->Size;
	sha256_final(HashContext, Info->Hash);
	return (ml_value_t *)Closure;
}

typedef struct ml_console_t {
	ml_getter_t ParentGetter;
	void *ParentGlobals;
	const char *Prompt;
	stringmap_t Globals[1];
} ml_console_t;

static ml_value_t *ml_console_global_get(ml_console_t *Console, const char *Name) {
	return stringmap_search(Console->Globals, Name) ?: (Console->ParentGetter)(Console->ParentGlobals, Name);
}

static const char *ml_console_line_read(ml_console_t *Console) {
	const char *Line = linenoise(Console->Prompt);
	if (!Line) return NULL;
	linenoiseHistoryAdd(Line);
	int Length = strlen(Line);
	char *Buffer = snew(Length + 2);
	memcpy(Buffer, Line, Length);
	Buffer[Length] = '\n';
	Buffer[Length + 1] = 0;
	return Buffer;
}

void ml_console(ml_getter_t GlobalGet, void *Globals) {
	ml_console_t Console[1] = {{
		GlobalGet, Globals, "--> ",
		{STRINGMAP_INIT}
	}};
	mlc_scanner_t *Scanner = ml_scanner("console", Console, (void *)ml_console_line_read);
	mlc_function_t Function[1] = {{(void *)ml_console_global_get, Console, NULL,}};
	SHA256_CTX HashContext[1];
	sha256_init(HashContext);
	ml_value_t *StringMethod = ml_method("string");
	if (setjmp(Scanner->OnError)) {
		printf("Error: %s\n", ml_error_message(Scanner->Error));
		const char *Source;
		int Line;
		for (int I = 0; ml_error_trace(Scanner->Error, I, &Source, &Line); ++I) printf("\t%s:%d\n", Source, Line);
		Scanner->Token = MLT_NONE;
		Scanner->Next = "";
	}
	for (;;) {
		mlc_expr_t *Expr = ml_accept_command(Scanner, Console->Globals);
		if (Expr == (mlc_expr_t *)-1) return;
		mlc_compiled_t Compiled = ml_compile(Function, Expr, HashContext);
		mlc_connect(Compiled.Exits, NULL);
		ml_closure_t *Closure = new(ml_closure_t);
		ml_closure_info_t *Info = Closure->Info = new(ml_closure_info_t);
		Closure->Type = MLClosureT;
		Info->Entry = Compiled.Start;
		Info->FrameSize = Function->Size;
		ml_value_t *Result = ml_closure_call((ml_value_t *)Closure, 0, NULL);
		if (Result->Type == MLErrorT) {
			printf("Error: %s\n", ml_error_message(Result));
			const char *Source;
			int Line;
			for (int I = 0; ml_error_trace(Result, I, &Source, &Line); ++I) printf("\t%s:%d\n", Source, Line);
		} else {
			ml_value_t *String = ml_call(StringMethod, 1, &Result);
			if (String->Type == MLStringT) {
				printf("%s\n", ml_string_value(String));
			} else {
				printf("<%s>\n", Result->Type->Name);
			}
		}
	}
}

int ml_is(ml_value_t *Value, ml_type_t *Expected) {
	const ml_type_t *Type = Value->Type;
	while (Type) {
		if (Type == Expected) return 1;
		Type = Type->Parent;
	}
	return 0;
}
