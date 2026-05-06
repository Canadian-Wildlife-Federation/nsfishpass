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
# This script loads updates to habitat and accessibility information into the database
# Necessary step if habitat table does not exist yet
# and if habitat data is coming from an external geopackage
#
import subprocess
import appconfig


# MUCH EASIER TO SET THESE IN COMPUTER HOWEVER REQUIRE ADMIN PRIVILEGES FOR THIS
# ONLY SET THESE IF MODEL OUTPUTS ERROR: CANNOT FIND PROJ.DB
#-----------------------------------------------------------------------------------
# import os
# os.environ['PROJ_LIB'] = r'C:\Program Files\QGIS 3.22.1\share\proj'
# os.environ['PROJ_DATA'] = r'C:\Program Files\QGIS 3.22.1\share\proj'
#-----------------------------------------------------------------------------------

iniSection = appconfig.args.args[0]
streamTable = appconfig.config['DATABASE']['stream_table']
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
file = appconfig.config[iniSection]['habitat_access_updates']

datatable = "habitat_access_updates"

snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

def main():


    with appconfig.connectdb() as conn:

        # query = f"""DROP TABLE IF EXISTS {dbTargetSchema}.{datatable};"""
        # with conn.cursor() as cursor:
        #     cursor.execute(query)
        # conn.commit()

        query = f"""
            CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{datatable}
            (
                update_source character varying COLLATE pg_catalog."default",
                update_date date,
                update_type character varying COLLATE pg_catalog."default",
                notes character varying COLLATE pg_catalog."default",
                species character varying COLLATE pg_catalog."default",
                pair_id integer,
                upstream boolean,
                downstream boolean,
                habitat_type character varying COLLATE pg_catalog."default",
                latitude double precision,
                longitude double precision,
                geometry geometry(Point,2961),
                id uuid primary key,
                snapped_point geometry(Point,2961),
                stream_measure numeric,
                stream_id_up uuid,
                stream_id_down uuid
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS {dbTargetSchema}.{datatable}
                OWNER to cwf_analyst;

            REVOKE ALL ON TABLE {dbTargetSchema}.{datatable} FROM PUBLIC;

            GRANT SELECT ON TABLE {dbTargetSchema}.{datatable} TO PUBLIC;

            GRANT ALL ON TABLE {dbTargetSchema}.{datatable} TO andrewp;

            GRANT ALL ON TABLE {dbTargetSchema}.{datatable} TO cwf_tech;

            GRANT ALL ON TABLE {dbTargetSchema}.{datatable} TO cwf_analyst;
            -- Index: habitat_access_updates_geometry_geom_idx

            -- DROP INDEX IF EXISTS {dbTargetSchema}.{datatable}_geometry_geom_idx;

            CREATE INDEX IF NOT EXISTS habitat_access_updates_geometry_geom_idx
                ON {dbTargetSchema}.{datatable} USING gist
                (geometry)
                TABLESPACE pg_default;
            -- Index: habitat_access_updates_snapped_point_idx

            -- DROP INDEX IF EXISTS {dbTargetSchema}.{datatable}_snapped_point_idx;

            CREATE INDEX IF NOT EXISTS habitat_access_updates_snapped_point_idx
                ON {dbTargetSchema}.{datatable} USING gist
                (snapped_point)
                TABLESPACE pg_default;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()           

        # As with barrier updates, we can initially load habitat updates from an existing geopackage or from the existing data layer
        print("Loading habitat and accessibility updates")
        layer = "habitat_access_updates"
        orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
        pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + dbTargetSchema + '.' + datatable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
        print(pycmd)
        subprocess.run(pycmd)
        
        query = f"""
        -- original geopkg with observations did not include id column so generate here
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS id;
        ALTER TABLE {dbTargetSchema}.{datatable} add column id uuid;
        UPDATE {dbTargetSchema}.{datatable} set id = gen_random_uuid();
        
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS snapped_point;
        ALTER TABLE {dbTargetSchema}.{datatable} add column snapped_point geometry(POINT, {appconfig.dataSrid});
        
        --SELECT public.snap_to_network('{dbTargetSchema}', '{datatable}', 'geometry', 'snapped_point', '{snapDistance}');

        -- fish observations are placed in QGIS
        -- snap to nearest point on stream here
        SELECT public.snap_to_network('{dbTargetSchema}', '{datatable}', 'geometry', 'snapped_point', '125');

        CREATE INDEX {datatable}_snapped_point_idx ON {dbTargetSchema}.{datatable} USING gist (snapped_point);
        
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS stream_measure;
        ALTER TABLE {dbTargetSchema}.{datatable} add column stream_id uuid;
        ALTER TABLE {dbTargetSchema}.{datatable} add column stream_measure numeric;
        
        -- add stream id and stream measure of nearest stream segment to habitat point
        with match as (
        SELECT a.id as stream_id, b.id as pntid, st_linelocatepoint(a.geometry, b.snapped_point) as streammeasure
        FROM {dbTargetSchema}.{dbTargetStreamTable} a, {dbTargetSchema}.{datatable} b
        WHERE st_intersects(a.geometry, st_buffer(b.snapped_point, 0.0001))
        )
        UPDATE {dbTargetSchema}.{datatable}
        SET stream_id = a.stream_id, stream_measure = a.streammeasure
        FROM match a WHERE a.pntid = {dbTargetSchema}.{datatable}.id;

        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print("Loading habitat and accessibility updates complete")

if __name__ == "__main__":
    main()