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
# Loads dam barriers from the CABD API into local database
#
import appconfig
import json, urllib.request
from appconfig import dataSchema
import ast
import sys

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbWatershedId = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
dbRawDataSchema = appconfig.config['DATABASE']['data_schema']
workingWatershedId = appconfig.config[iniSection]['watershed_id']
nhnWatershedId = ast.literal_eval(appconfig.config[iniSection]['nhn_watershed_id'])
nhnWatershedId = ','.join(nhnWatershedId)

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']
fishSpeciesTable = appconfig.config['DATABASE']['fish_species_table']
secondaryWatershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['secondary_watershed_table']
species = appconfig.config[iniSection]['species']

def main():
    
    with appconfig.connectdb() as conn:

        global species

        specCodes = [substring.strip() for substring in species.split(',')]

        query = f""" DROP VIEW IF EXISTS {dbTargetSchema}.barrier_passability_view; """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # create fish species table
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.fish_species;
            
            CREATE TABLE IF NOT EXISTS {dbTargetSchema}.fish_species (
                id uuid not null default gen_random_uuid()
                ,code varchar(32)
                ,common_name varchar(128)
                ,mi_kmaw_name varchar(128)
                
                ,primary key (id)
            );

            INSERT INTO {dbTargetSchema}.fish_species (code, common_name, mi_kmaw_name)
            SELECT code, name, mi_kmaw_name FROM {appconfig.dataSchema}.{fishSpeciesTable};

            ALTER TABLE {dbTargetSchema}.fish_species OWNER TO cwf_analyst;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)

        # creates barriers table with attributes from CABD and crossings table
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbBarrierTable};

            create table if not exists {dbTargetSchema}.{dbBarrierTable} (
                id uuid not null default gen_random_uuid(),
                cabd_id uuid,
                modelled_id uuid,
                update_id varchar,
                original_point geometry(POINT, {appconfig.dataSrid}),
                snapped_point geometry(POINT, {appconfig.dataSrid}),
                name varchar(256),
                type varchar(32),
                owner varchar,
                passability_status varchar,
                passability_status_notes varchar,

                dam_use varchar,

                fall_height_m real,

                stream_name varchar,
                strahler_order integer,
                stream_id uuid,
                wshed_name varchar,
                secondary_wshed_name varchar,
                transport_feature_name varchar,

                critical_habitat varchar[],
                
                crossing_status varchar CHECK (crossing_status in ('MODELLED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,

                ais_upstr varchar[],
                ais_downstr varchar[],
                
                culvert_number varchar,
                structure_id varchar,
                date_examined date,
                road varchar,
                culvert_type varchar,
                culvert_condition varchar,
                action_items varchar,
                cmm_pt_exists boolean,

                primary key (id)
            );

            --CREATE INDEX {dbTargetSchema}_{dbBarrierTable}_original_point on {dbTargetSchema}.{dbBarrierTable} using gist(original_point);
            --CREATE INDEX {dbTargetSchema}_{dbBarrierTable}_snapped_point on {dbTargetSchema}.{dbBarrierTable} using gist(snapped_point);

            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} OWNER TO cwf_analyst;
            
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # create passability table
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbPassabilityTable};
            
            CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{dbPassabilityTable} (
                barrier_id uuid
                ,species_id uuid
                ,species_code varchar
                ,passability_status varchar
                ,passability_status_notes varchar
            );

            ALTER TABLE {dbTargetSchema}.{dbPassabilityTable} OWNER TO cwf_analyst;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # retrieve barrier data from CABD API
        url = f"https://cabd-web.azurewebsites.net/cabd-api/features/dams?&filter=nhn_watershed_id:in:{nhnWatershedId}&filter=use_analysis:eq:true"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())

        feature_data = data["features"]
        output_data = []

        for feature in feature_data:
            output_feature = []
            output_feature.append(feature["properties"]["cabd_id"])
            output_feature.append(feature["geometry"]["coordinates"][0])
            output_feature.append(feature["geometry"]["coordinates"][1])
            output_feature.append(feature["properties"]["dam_name_en"])
            output_feature.append(feature["properties"]["owner"])
            output_feature.append(feature["properties"]["dam_use"])
            output_feature.append(feature["properties"]["passability_status"])
            output_data.append(output_feature)


        insertquery = f"""
            INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
                cabd_id, 
                original_point,
                name,
                owner,
                dam_use,
                passability_status,
                type)
            VALUES (%s, ST_Transform(ST_GeomFromText('POINT(%s %s)',4617),{appconfig.dataSrid}), %s, %s, %s, UPPER(%s), 'dam');
        """
        with conn.cursor() as cursor:
            for feature in output_data:
                cursor.execute(insertquery, feature)
        conn.commit()

        # retrieve waterfall data from CABD API
        url = f"https://cabd-web.azurewebsites.net/cabd-api/features/waterfalls?&filter=nhn_watershed_id:in:{nhnWatershedId}"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())

        feature_data = data["features"]
        output_data = []

        for feature in feature_data:
            output_feature = []
            output_feature.append(feature["properties"]["cabd_id"])
            output_feature.append(feature["geometry"]["coordinates"][0])
            output_feature.append(feature["geometry"]["coordinates"][1])
            output_feature.append(feature["properties"]["fall_name_en"])
            output_feature.append(feature["properties"]["fall_height_m"])
            output_feature.append(feature["properties"]["passability_status"])
            output_data.append(output_feature)


        insertquery = f"""
            INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
                cabd_id, 
                original_point,
                name,
                fall_height_m,
                passability_status,
                type)
            VALUES (%s, ST_Transform(ST_GeomFromText('POINT(%s %s)',4617),{appconfig.dataSrid}), %s,%s, UPPER(%s), 'waterfall');
        """
        with conn.cursor() as cursor:
            for feature in output_data:
                cursor.execute(insertquery, feature)
        conn.commit()

        # snaps barrier features to network
        query = f"""
            CREATE OR REPLACE FUNCTION public.snap_to_network(src_schema varchar, src_table varchar, raw_geom varchar, snapped_geom varchar, max_distance_m double precision) RETURNS VOID AS $$
            DECLARE    
              pnt_rec RECORD;
              fp_rec RECORD;
            BEGIN
                FOR pnt_rec IN EXECUTE format('SELECT id, %I as rawg FROM %I.%I WHERE %I is not null', raw_geom, src_schema, src_table,raw_geom) 
                LOOP 
                    FOR fp_rec IN EXECUTE format ('SELECT fp.geometry as geometry, st_distance(%L::geometry, fp.geometry) AS distance FROM {dbTargetSchema}.{dbTargetStreamTable} fp WHERE st_expand(%L::geometry, %L) && fp.geometry and st_distance(%L::geometry, fp.geometry) < %L ORDER BY distance ', pnt_rec.rawg, pnt_rec.rawg, max_distance_m, pnt_rec.rawg, max_distance_m)
                    LOOP
                        EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry, %L::geometry) ) WHERE id = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, pnt_rec.id);
                        EXIT;
                    END LOOP;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
         
            SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');

            --remove any dam features not snapped to streams
            --because using nhn_watershed_id can cover multiple watersheds
            DELETE FROM {dbTargetSchema}.{dbBarrierTable}
            WHERE snapped_point IS NULL
            AND (type = 'dam' OR type = 'waterfall');
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        if secondaryWatershedTable != 'None':
            # get secondary watershed names
            query = f"""
            --UPDATE {dbTargetSchema}.{dbBarrierTable} b SET secondary_wshed_name = a.sec_name FROM {appconfig.dataSchema}.{secondaryWatershedTable} a WHERE ST_INTERSECTS(b.original_point, a.geometry);
            UPDATE {dbTargetSchema}.{dbBarrierTable} b SET secondary_wshed_name = a.sec_name FROM {appconfig.dataSchema}.{secondaryWatershedTable} a WHERE ST_INTERSECTS(b.snapped_point, a.geometry);
            """
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
        else:
            # default call secondary watershed by wcrp name
            query = f"""
            UPDATE {dbTargetSchema}.{dbBarrierTable} b SET secondary_wshed_name = '{iniSection}';
            """
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()

        # Load barrier passability to intermediate table
        query = f"""
            SELECT id, code
            FROM {dbTargetSchema}.fish_species;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            species = cursor.fetchall()
        conn.commit()

        # get waterfall height thresholds
        query = f"""
            SELECT code, fall_height_threshold
            FROM {dbRawDataSchema}.fish_species;
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            fall_heights = cursor.fetchall()
        conn.commit()

        query = f"""
            SELECT id, passability_status, type, fall_height_m
            FROM {dbTargetSchema}.{dbBarrierTable};
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            feature_data = cursor.fetchall()
        conn.commit()
        
        passability_data = []

        # Get data for passability table
        for feature in feature_data:
            for s in species:
                # acquire fall height threshold for this species
                for h in fall_heights:
                    if s[1] == h[0]:
                        fall_height_threshold = h[1]
                passability_feature = []
                passability_feature.append(feature[0])
                passability_feature.append(s[0])
                # assign passability
                if feature[2] == 'waterfall' and feature[3]:
                    if float(feature[3]) >= fall_height_threshold:
                        passability_feature.append('BARRIER')
                    else:
                        passability_feature.append('PASSABLE')
                elif feature[2] == 'waterfall':     # Waterfalls with unknown height are passable
                    passability_feature.append('PASSABLE')
                else:
                    passability_feature.append(feature[1])
                passability_data.append(passability_feature)
                        
        insertquery = f"""
            INSERT INTO {dbTargetSchema}.barrier_passability (
                barrier_id
                ,species_id
                ,passability_status
                )
            VALUES (%s, %s, UPPER(%s));

            
        """
        with conn.cursor() as cursor:
            for feature in passability_data:
                cursor.execute(insertquery, feature)
        conn.commit()

        updatequery = f"""
            UPDATE {dbTargetSchema}.barrier_passability
                SET passability_status = 
                    CASE
                    WHEN passability_status = 'BARRIER' THEN 0
                    WHEN passability_status = 'UNKNOWN' THEN 0
                    WHEN passability_status = 'PARTIAL BARRIER' THEN 0.5
                    WHEN passability_status = 'PASSABLE' THEN 1
                    ELSE NULL END;
        """

        with conn.cursor() as cursor:
            cursor.execute(updatequery)
        conn.commit()

        updatequery = f"""
            ALTER TABLE {dbTargetSchema}.barrier_passability
            ADD COLUMN IF NOT EXISTS species_code varchar(32);

            UPDATE {dbTargetSchema}.barrier_passability b
            SET species_code = f.code
            FROM {dbTargetSchema}.fish_species f 
            WHERE f.id = b.species_id;
        """

        with conn.cursor() as cursor:
            cursor.execute(updatequery)
        conn.commit()
         
        print("Loading barriers from CABD dataset complete")


        # add species-specific passability fields
        # for species in specCodes:
        #     code = species[0]

        #     colname = "passability_status_" + code
            
        #     query = f"""
        #         alter table {dbTargetSchema}.{dbBarrierTable} 
        #         add column if not exists {colname} numeric;
    
        #         update {dbTargetSchema}.{dbBarrierTable}
        #         set {colname} = 
        #             CASE
        #             WHEN passability_status = 'BARRIER' THEN 0
        #             WHEN passability_status = 'UNKNOWN' THEN 0
        #             WHEN passability_status = 'PARTIAL BARRIER' THEN 0.5
        #             WHEN passability_status = 'PASSABLE' THEN 1
        #             ELSE NULL END;
        #     """

        #     with conn.cursor() as cursor:
        #         cursor.execute(query)
        
        query = f"""
            alter table {dbTargetSchema}.{dbBarrierTable} 
            drop column if exists passability_status cascade;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print("Loading barrier data complete")


if __name__ == "__main__":
    main()