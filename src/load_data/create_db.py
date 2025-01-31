#----------------------------------------------------------------------------------
#
# Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------------------

#
# This script creates the database tables that follow the structure
# in the gdb file.
#
 
import appconfig

roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table']

query = f"""
    --drop schema if exists {appconfig.dataSchema} cascade;
    --drop schema if exists {appconfig.dataSchema}_wcrp cascade;

    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;
    
    create schema {appconfig.dataSchema};
    create schema {appconfig.dataSchema}_wcrp;
    
    create table {appconfig.dataSchema}.{appconfig.streamTable} (
        id uuid NOT NULL,
        aoi_id uuid,
        ef_type integer,
        ef_subtype integer,
        rank integer,
        length double precision,
        rivernameid1 uuid,
        rivername1 varchar,
        rivernameid2 uuid,
        rivername2 varchar,
        strahler_order integer,
        graph_id integer,
        mainstem_id uuid,
        max_uplength double precision,
        hack_order integer,
        mainstem_seq integer,
        shreve_order integer,
        geometry geometry(linestring, {appconfig.dataSrid}) NOT NULL,
        primary key(id)
    );

    create index {appconfig.streamTable}_geom2d_idx on {appconfig.dataSchema}.{appconfig.streamTable} using gist(geometry);
    
    create table {appconfig.dataSchema}.{roadTable} ( 
        id uuid not null,
        fid integer,
        roadsegid integer,
        feat_code varchar(254),
        feat_desc varchar(254),
        ids integer,
        segid integer,
        street varchar(254),
        mun_id integer,
        trafficdir integer,
        traff_desc varchar(254),
        date_act integer,
        date_rev integer,
        structid varchar(254),
        roadclass varchar(254),
        roadc_desc varchar(254),
        rte_no integer,
        anum varchar(254),
        owner varchar(254),
        owner_desc varchar(254),
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {roadTable}_geom_idx on {appconfig.dataSchema}.{roadTable} using gist(geometry);
    
    create table {appconfig.dataSchema}.{trailTable} ( 
        id uuid not null,
        fid integer,
        roadsegid integer,
        feat_code varchar(254),
        feat_desc varchar(254),
        ids integer,
        segid integer,
        street varchar(254),
        mun_id integer,
        trafficdir integer,
        traff_desc varchar(254),
        date_act integer,
        date_rev integer,
        structid varchar(254),
        roadclass varchar(254),
        roadc_desc varchar(254),
        rte_no integer,
        anum varchar(254),
        owner varchar(254),
        owner_desc varchar(254),
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {trailTable}_geom_idx on {appconfig.dataSchema}.{trailTable} using gist(geometry);

    create table {appconfig.dataSchema}.{railTable} ( 
        id uuid not null,
        fid integer,
        roadsegid integer,
        feat_code varchar(254),
        feat_desc varchar(254),
        ids integer,
        segid integer,
        street varchar(254),
        mun_id integer,
        trafficdir integer,
        traff_desc varchar(254),
        date_act integer,
        date_rev integer,
        structid varchar(254),
        roadclass varchar(254),
        roadc_desc varchar(254),
        rte_no integer,
        anum varchar(254),
        owner varchar(254),
        owner_desc varchar(254),
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {railTable}_geom_idx on {appconfig.dataSchema}.{railTable} using gist(geometry);

    ALTER SCHEMA {appconfig.dataSchema} OWNER TO cwf_analyst;
    ALTER TABLE {appconfig.dataSchema}.{appconfig.streamTable} OWNER TO cwf_analyst;
    ALTER TABLE {appconfig.dataSchema}.{roadTable} OWNER TO cwf_analyst;
    ALTER TABLE {appconfig.dataSchema}.{trailTable} OWNER TO cwf_analyst;
    ALTER TABLE {appconfig.dataSchema}.{railTable} OWNER TO cwf_analyst;
    
    ALTER SCHEMA {appconfig.dataSchema}_wcrp OWNER TO cwf_analyst;

    
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

# set up foreign tables to access CHyF data
query = f"""
CREATE FOREIGN TABLE IF NOT EXISTS public.chyf_aoi(
    id uuid NULL,
    short_name character varying NULL COLLATE pg_catalog."default",
    full_name character varying NULL COLLATE pg_catalog."default",
    geometry geometry(Polygon,4617) NULL
)
    SERVER chyf_server
    OPTIONS (schema_name 'chyf2', table_name 'aoi');

GRANT SELECT ON TABLE public.chyf_aoi TO PUBLIC;

CREATE FOREIGN TABLE IF NOT EXISTS public.chyf_flowpath(
    id uuid NULL,
    aoi_id uuid NULL,
    ef_type integer NULL,
    ef_subtype integer NULL,
    rank integer NULL,
    length double precision NULL,
    rivernameid1 uuid NULL,
    rivernameid2 uuid NULL,
    geometry geometry(LineString,4617) NULL
)
    SERVER chyf_server
    OPTIONS (schema_name 'chyf2', table_name 'eflowpath');

GRANT SELECT ON TABLE public.chyf_flowpath TO PUBLIC;

CREATE FOREIGN TABLE IF NOT EXISTS public.chyf_flowpath_properties(
    id uuid NULL,
    aoi_id uuid NULL,
    strahler_order integer NULL,
    graph_id integer NULL,
    mainstem_id uuid NULL,
    max_uplength double precision NULL,
    hack_order integer NULL,
    mainstem_seq integer NULL,
    shreve_order integer NULL
)
    SERVER chyf_server
    OPTIONS (schema_name 'chyf2', table_name 'eflowpath_properties_vw');

GRANT SELECT ON TABLE public.chyf_flowpath_properties TO PUBLIC;

CREATE FOREIGN TABLE IF NOT EXISTS public.chyf_names(
    name_id uuid NULL,
    name_en character varying NULL COLLATE pg_catalog."default",
    name_fr character varying NULL COLLATE pg_catalog."default",
    geodbname character varying NULL COLLATE pg_catalog."default",
    geodb_id character varying NULL COLLATE pg_catalog."default"
)
    SERVER chyf_server
    OPTIONS (schema_name 'chyf2', table_name 'names');

GRANT SELECT ON TABLE public.chyf_names TO PUBLIC;

CREATE FOREIGN TABLE IF NOT EXISTS public.chyf_shoreline(
    id uuid NULL,
    aoi_id uuid NULL,
    geometry geometry NULL
)
    SERVER chyf_server
    OPTIONS (schema_name 'chyf2', table_name 'shoreline');

GRANT SELECT ON TABLE public.chyf_shoreline TO PUBLIC;
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

## Set up ENUM types for tracking table dropdowns
# In QGIS, fields with an ENUM type can be edited via
# a user-friendly dropdown.
# This allows for columns where the value must be one of
# a predefined set of values and the options will
# conventiently show up when a user edits the field in QGIS
query = f"""
drop type if exists
	tt_structure_type,
	tt_structure_list_status_type,
	tt_passability_asmt_type,
	tt_assessment_step_type,
	tt_excl_reason_type,
	tt_excl_method_type,
	tt_partial_passability_type,
	tt_partial_passability_notes_type,
	tt_upstr_hab_quality_type,
	tt_constructability_type,
	tt_priority_type,
	tt_rehab_type;
	
CREATE TYPE tt_structure_type AS ENUM
    ('Dam', 'Stream crossing - OBS', 'Stream crossing - CBS', 'Stream crossing - Ford', 'Aboiteaux', 'Other', 'None', '');
	
CREATE TYPE tt_structure_list_status_type AS ENUM
    ('Assessed structure that remains data deficient', 'Confirmed barrier', 'Rehabilitated barrier', 'Excluded structure', '');
	
create type tt_passability_asmt_type as enum
	('Informal assessment', 'Rapid assessment', 'Full assessment', '');
	
CREATE TYPE tt_assessment_step_type AS ENUM
    ('Informal assessment', 
	 'Barrier assessment', 
	 'Habitat confirmation', 
	 'Detailed habitat investigation', 
	 'Engineering design', 
	 'Rehabilitated', 
	 'Post-rehabilitation monitoring', 
	 'Other',
	 '');
	 
CREATE TYPE tt_excl_reason_type AS ENUM
    ('Passable', 'No structure', 'No key upstream habitat', 'No structure and key upstream habitat', '');
	
CREATE TYPE tt_excl_method_type AS ENUM
    ('Imagery review', 'Informal assessment', 'Field assessment', 'Local knowledge', '');
	
CREATE TYPE tt_partial_passability_type AS ENUM
    ('Yes', 'No', 'Unknown');
	
CREATE TYPE tt_partial_passability_notes_type AS ENUM
    ('Proportion of individuals', 'Proportion of time', '');
	
CREATE TYPE tt_upstr_hab_quality_type AS ENUM
    ('High', 'Medium', 'Low', 'N/A or unassessed');
	
create type tt_constructability_type as enum (
	'Difficult',
	'Moderate',
	'Easy',
	''
);

CREATE TYPE tt_priority_type AS ENUM
    ('High', 'Medium', 'Low', 'Non-actionable', '');
	
	
CREATE TYPE tt_rehab_type AS ENUM
    ('Removal/decommissioned', 'Replacement - OBS', 'Replacement - CBS', 'Retrofit', '');
	
CREATE TYPE tt_next_steps_type AS ENUM
    ('Barrier assessment (data deficient structures only)', 
	 'Habitat confirmation (data deficient structures only)',
	 'In-depth habitat investigation (data deficient structures only)',
	 'In-depth passage assessment (data deficient structures only)', 
	 'Engage with barrier owner', 
	 'Bring barrier to regulator',
	 'Commission engineering designs', 
	 'Leave until end of lifecycle', 
	 'Identify barrier owner', 
	 'Engage in public consultation',
	 'Fundraise', 
	 'Rehabilitation',
	 'Post-rehabilitation monitoring',
	 'N/A - project complete (rehabilitated structures only)', 
	 'Engage with partners',
	 'Barrier reassessment',
	 'Non-actionable',
	 ''
	);
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

## Add function to change blank values to null in tracking tables
# ENUM types support NULL values but when a layer with an ENUM is 
# brought into QGIS, th field can only be updated to values defined in the ENUM
# Therefore, all ENUM types define a blank ('') option and the
# following trigger changes such values to NULL in the database.
query = f"""
-- Trigger to set blank values to null
create or replace function blank2null()
	returns trigger
	language plpgsql as
$func$
begin
	if NEW.structure_list_status = '' then
		NEW.structure_list_status = NULL;
	end if;
	
	if NEW.structure_type = '' then
		NEW.structure_type = NULL;
	end if;
	
	if NEW.assessment_step_completed = '' then
		NEW.assessment_step_completed = NULL;
	end if;
	
	if NEW.reason_for_exclusion = '' then
		NEW.reason_for_exclusion = NULL;
	end if;
	
	if NEW.method_of_exclusion = '' then
		NEW.method_of_exclusion = NULL;
	end if;
	
	if NEW.partial_passability_notes = '' then
		NEW.partial_passability_notes = NULL;
	end if;
	
	if NEW.constructability = '' then
		NEW.constructability = NULL;
	end if;
	
	if NEW.priority = '' then
		NEW.priority = NULL;
	end if;
	
	if NEW.type_of_rehabilitation = '' then
		NEW.type_of_rehabilitation = NULL;
	end if;
	
	if NEW.next_steps = '' then
		NEW.next_steps = NULL;
	end if;
	
	return NEW;
end
$func$;
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

## Create function to create tracking tables
# New tracking tables should be created manually using this function
# Tracking tables are combined under one table in the database called <wcrp>.combined_tracking_table_<watershed> (eg. cmm.combined_tracking_table_st_croix). 
# The table is only editable by the biologist for the WCRP. 
query = f"""
create or replace function create_tracking_table(p_wcrp text, p_species text[])
	returns void
as
$$
declare 
	struct_list_status_cols text = '';
	partial_pass_cols text = '';
	species_name text;
	v_schema_name text;
	v_table_name text;
	i integer;
	table_exists boolean;
begin 

	v_schema_name := p_wcrp;
	
	if p_wcrp = 'cmm' then
		v_table_name = 'combined_tracking_table_st_croix';
	else
		v_table_name = 'combined_tracking_table_' || p_wcrp;
	end if;
	
	for i in 1..array_length(p_species, 1) loop
		species_name := p_species[i];
		struct_list_status_cols := struct_list_status_cols || format('structure_list_status_%s tt_structure_list_status_type, ', species_name);
		partial_pass_cols := partial_pass_cols || format('partial_passability_%s tt_partial_passability_type, ', species_name);
		partial_pass_cols := partial_pass_cols || format('partial_passability_notes_%s tt_partial_passability_notes_type, ', species_name);
	end loop;
		
	execute format(
		'CREATE TABLE IF NOT EXISTS %I_wcrp.%I
		(
			internal_name varchar,
			barrier_id varchar PRIMARY KEY,
			watercourse_name varchar,
			road_name varchar,
			structure_type tt_structure_type,
			structure_owner varchar,
			private_owner_details varchar,
			%s
			passability_assessment_type tt_passability_asmt_type,
			assessment_step_completed tt_assessment_step_type,
			reason_for_exclusion tt_excl_reason_type,
			method_of_exclusion tt_excl_method_type,
			%s
			upstream_habitat_quality tt_upstr_hab_quality_type,
			constructability tt_constructability_type,
			estimated_cost_$ numeric,
			priority tt_priority_type,
			type_of_rehabilitation tt_rehab_type,
			rehabilitated_by varchar,
			rehabilitated_date text,
			estimated_rehabilitation_cost_$ numeric,
			actual_project_cost_$ numeric,
			next_steps tt_next_steps_type,
			timeline_for_next_steps text,
			lead_for_next_steps varchar,
			others_involved_in_next_steps varchar,
			reason varchar,
			notes varchar,
			supporting_links varchar
		);',
		v_schema_name,
		v_table_name,
		struct_list_status_cols,
		partial_pass_cols
	);
	
	
	
	SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = v_table_name
        AND table_schema = v_schema_name
    ) INTO table_exists;
	
	IF table_exists THEN 
		execute format('ALTER TABLE IF EXISTS %I_wcrp.%I OWNER to cwf_analyst;', p_wcrp, v_table_name);
    	execute format('REVOKE ALL ON TABLE %I_wcrp.%I FROM cwf_user;', p_wcrp, v_table_name);
		execute format('GRANT SELECT ON TABLE %I_wcrp.%I TO cwf_user;', p_wcrp, v_table_name);
		execute format('GRANT ALL ON TABLE %I_wcrp.%I TO cwf_analyst;', p_wcrp, v_table_name);
		execute format('GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE %I_wcrp.%I TO fieldingm;', p_wcrp, v_table_name);
	END IF;  
	
end
$$
language plpgsql;
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)


## Create function to join the tracking table to the crossings view
query = f"""
create or replace function join_tracking_table_crossings_vw(p_wcrp text, p_species text[])
	returns void
as
$$
declare
	bp_species_cols text = '';
	tt_struct_list_status_cols text = '';
	tt_partial_pass_cols text = '';
    upstr_hab_condition text = '';
    sec_wshed_condition text = '';
	species_name text;
	v_table_name text;
	join_table text;
	i integer;
	table_exists boolean;
begin 

	
	if p_wcrp = 'cmm' then
		v_table_name = 'combined_tracking_table_crossings_st_croix_vw';
		join_table = 'combined_tracking_table_st_croix';
        sec_wshed_condition = 'bp.secondary_wshed_name = ''St. Croix R.'' AND';
	else
		v_table_name = 'combined_tracking_table_crossings_' || p_wcrp || '_vw';
		join_table = 'combined_tracking_table_' || p_wcrp;
	end if;
	
	for i in 1..array_length(p_species, 1) loop
		species_name := p_species[i];
		bp_species_cols := bp_species_cols || format('bp.func_upstr_hab_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.total_upstr_hab_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.w_func_upstr_hab_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.w_total_upstr_hab_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.group_id_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.num_barriers_group_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.downstr_group_ids_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.total_hab_gain_group_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.w_total_hab_gain_group_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.avg_gain_per_barrier_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.w_avg_gain_per_barrier_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.rank_w_avg_gain_tiered_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.rank_w_total_upstr_hab_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.rank_combined_%s,', species_name);
		bp_species_cols := bp_species_cols || format('bp.passability_status_%s,', species_name);
		
		tt_struct_list_status_cols := tt_struct_list_status_cols || format('structure_list_status_%s,', species_name);
		
		tt_partial_pass_cols := tt_partial_pass_cols || format('partial_passability_%s,', species_name);
		tt_partial_pass_cols := tt_partial_pass_cols || format('partial_passability_notes_%s,', species_name);
        
        if i = 1 then
			upstr_hab_condition := format('bp.total_upstr_hab_%s > 0', species_name);
		else
			upstr_hab_condition := upstr_hab_condition || format(' or bp.total_upstr_hab_%s > 0', species_name);
		end if;
	end loop;
	
	
	execute format(
		'create or replace view %I_wcrp.%I as
		select 
		tt.barrier_id,
		bp.original_point,
		bp.snapped_point,
		bp.name,
		bp.type,
		bp.owner,
		bp.dam_use,
		bp.fall_height_m,
		bp.stream_name,
		bp.strahler_order,
		bp.wshed_name,
		bp.secondary_wshed_name,
		bp.transport_feature_name,
		bp.critical_habitat,
		bp.crossing_status,
		bp.crossing_feature_type,
		bp.crossing_type,
		bp.crossing_subtype,
		bp.culvert_number,
		bp.culvert_condition,
		bp.action_items,
		bp.passability_status_notes,
		
		%s
		
		tt.internal_name,
		tt.watercourse_name,
		tt.road_name,
		tt.structure_type,
		tt.structure_owner,
		tt.private_owner_details,
		%s
		tt.passability_assessment_type,
		tt.assessment_step_completed,
		tt.reason_for_exclusion,
		tt.method_of_exclusion,
		%s
		tt.upstream_habitat_quality,
		tt.constructability,
		tt.estimated_cost_$,
		tt.priority,
		tt.type_of_rehabilitation,
		tt.rehabilitated_by,
		tt.rehabilitated_date,
		tt.estimated_rehabilitation_cost_$,
		tt.actual_project_cost_$,
		tt.next_steps,
		tt.timeline_for_next_steps,
		tt.lead_for_next_steps,
		tt.others_involved_in_next_steps,
		tt.reason,
		tt.notes,
		tt.supporting_links
		from %I_wcrp.barrier_passability_view bp
		full outer join %I.%I tt on 
			cast(bp.barrier_id as varchar) = tt.barrier_id
        where %s (%s) or tt.barrier_id is not null;', p_wcrp, v_table_name, bp_species_cols, tt_struct_list_status_cols, tt_partial_pass_cols, p_wcrp, p_wcrp, join_table, upstr_hab_condition);
			 
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_name = v_table_name
			AND table_schema = p_wcrp
		) INTO table_exists;
			
		IF table_exists THEN 
			execute format('ALTER TABLE IF EXISTS %I_wcrp.%I OWNER to cwf_analyst;', p_wcrp, v_table_name);
			execute format('REVOKE ALL ON TABLE %I_wcrp.%I FROM cwf_user;', p_wcrp, v_table_name);
			execute format('GRANT ALL ON TABLE %I_wcrp.%I TO cwf_analyst;', p_wcrp, v_table_name);
			execute format('GRANT SELECT ON TABLE %I_wcrp.%I TO cwf_user;', p_wcrp, v_table_name);
			execute format('GRANT SELECT ON TABLE %I_wcrp.%I TO fieldingm;', p_wcrp, v_table_name);
		END IF;
end
$$
language plpgsql;
"""

print (f"""Database schema {appconfig.dataSchema} created and ready for data """)
