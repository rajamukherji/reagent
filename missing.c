#include <stdio.h>
#include "minilang.h"

char *stpcpy(char *Dest, const char *Source) {
	while (*Source) *Dest++ = *Source++;
	return Dest;
}

int getline(const char **LinePtr, size_t *LengthPtr, FILE *File) {
	ml_stringbuffer_t Buffer[1] = {ML_STRINGBUFFER_INIT};
	size_t Length = 0;
	for (;;) {
		int Char = fgetc(File);
		if (Char == EOF) return -1;
		if (Char == '\n') break;
		ml_stringbuffer_add(Buffer, (char *)&(Char), 1);
	}
	LinePtr[0] = ml_stringbuffer_get(Buffer);
	if (LengthPtr) LengthPtr[0] = Length;
	return Length;
}
