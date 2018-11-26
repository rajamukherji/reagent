#ifndef RA_SCHEMA_H
#define RA_SCHEMA_H

#include "minilang.h"
#include "stringmap.h"

typedef struct ra_schema_t ra_schema_t;
typedef struct ra_schema_field_t ra_schema_field_t;
typedef struct ra_schema_index_t ra_schema_index_t;
typedef struct ra_schema_index_node_t ra_schema_index_node_t;
typedef struct ra_schema_listener_t ra_schema_listener_t;

typedef struct ra_instance_t ra_instance_t;
typedef struct ra_listener_t ra_listener_t;

typedef struct ra_instance_template_t ra_instance_template_t;
typedef struct ra_listener_template_t ra_listener_template_t;
typedef struct ra_schema_listener_template_t ra_schema_listener_template_t;

struct ra_schema_listener_template_t {
	ra_schema_t *Schema;
	ra_schema_index_t *Index;
	ra_schema_field_t **SelectedFields;
	int NumSelectedFields, Negated, Created;
};

struct ra_listener_template_t {
	int NumSchemas;
	ra_schema_listener_template_t Schemas[];
};

struct ra_instance_template_t {
	ra_schema_t *Schema;
	ra_schema_field_t **Fields;
	int NumFields;
};

ra_schema_t *ra_schema_create(const char *Name, ra_schema_t *Parent);
ra_schema_t *ra_schema_by_name(const char *Name);
ra_schema_field_t *ra_schema_value_field_create(ra_schema_t *Schema, const char *Name);
ra_schema_field_t *ra_schema_computed_field_create(ra_schema_t *Schema, const char *Name, ml_value_t *Function, const char **FieldNames);
ra_schema_field_t *ra_schema_constant_field_create(ra_schema_t *Schema, const char *Name, ml_value_t *Constant);
ra_schema_field_t *ra_schema_field_by_name(ra_schema_t *Schema, const char *Name);
ra_schema_index_t *ra_schema_index_create(ra_schema_t *Schema, const char **FieldNames);
ra_schema_index_t *ra_schema_index_by_names(ra_schema_t *Schema, const char **FieldNames);
ra_instance_t *ra_schema_index_search(ra_schema_index_t *Index, ml_value_t **Values);

int ra_schema_foreach(ra_schema_t *Schema, void *Data, int (*callback)(ra_instance_t *Instance, void *Data));

ra_instance_t *ra_instance_create(ra_schema_t *Schema, int NumFields, ra_schema_field_t **Fields, ml_value_t **Values, int Signal);
ra_instance_t *ra_instance_update(ra_instance_t *Instance, int NumFields, ra_schema_field_t **Fields, ml_value_t **Values);
void ra_instance_delete(ra_instance_t *Instance);

ml_value_t *ra_listener_create_callback(ra_listener_template_t *Template, int Count, ml_value_t **Args);
ml_value_t *ra_instance_create_callback(ra_instance_template_t *Schema, int Count, ml_value_t **Args);
ml_value_t *ra_instance_signal_callback(ra_instance_template_t *Schema, int Count, ml_value_t **Args);
ml_value_t *ra_index_instance_exists_callback(ra_schema_index_t *Index, int Count, ml_value_t **Args);
ml_value_t *ra_index_instance_update_callback(ra_schema_field_t **Fields, int Count, ml_value_t **Args);
ml_value_t *ra_index_instance_delete_callback(ra_schema_index_t *Index, int Count, ml_value_t **Args);

ml_value_t *ra_instance_field_by_field(ra_instance_t *Instance, ra_schema_field_t *Field);

void ra_schema_init();

extern ml_type_t RaSchemaT[1];
extern ml_type_t RaSchemaIndexT[1];
extern ml_type_t RaInstanceT[1];

extern ra_schema_field_t *InstanceField;

#endif
