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
    drop schema if exists {appconfig.dataSchema} cascade;

    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;
    
    create schema {appconfig.dataSchema};
    
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

print (f"""Database schema {appconfig.dataSchema} created and ready for data """)