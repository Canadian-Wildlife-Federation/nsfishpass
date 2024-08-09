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
# This script loads a barrier updates file into the database, and
# joins these updates to their respective tables. It can add, delete,
# and modify features of any barrier type.
#
# The script assumes the barrier updates file only contains data
# for a single watershed.
#

import subprocess
import appconfig
import sys

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbWatershedId = appconfig.config[iniSection]['watershed_id']
rawData = appconfig.config[iniSection]['barrier_updates']
dataSchema = appconfig.config['DATABASE']['data_schema']

dbTempTable = 'barrier_updates_' + dbWatershedId
dbTargetTable = appconfig.config['BARRIER_PROCESSING']['barrier_updates_table']

dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']
secondaryWatershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['secondary_watershed_table']
joinDistance = appconfig.config['CROSSINGS']['join_distance']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']
specCodes = appconfig.config[iniSection]['species']


def loadBarrierUpdates(connection):

    # create barrier update table if it doesn't exist
    global specCodes

    passability_cols  = ''

    for species in specCodes:
        species = species[0]

        passability_cols = f"""
            {passability_cols}
            passability_status_{species} varchar,
        """

    query = f"""
        CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{dbTargetTable} (
            update_id uuid NOT NULL DEFAULT gen_random_uuid(),
            barrier_id uuid,
            update_source varchar,
            update_date date,
            update_type varchar,
            site_id varchar,
            date_examined date,
            barrier_type varchar,
            {passability_cols}
            latitude double precision,
            longitude double precision,
            stream_name varchar,
            road_name varchar,
            ownership varchar,
            crossing_subtype varchar,
            notes varchar,
            cmm_pt_exists boolean,
            update_status varchar,
            geometry geometry(Point,2961),
            snapped_point geometry(Point,2961),
            

            PRIMARY KEY (update_id)
        );

        ALTER TABLE {dbTargetSchema}.{dbTargetTable} OWNER TO cwf_analyst;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()
        
    # load updates into a table
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dbTargetSchema + '.' + dbTargetTable + '" -lco GEOMETRY_NAME=geometry "' + rawData + '" -oo EMPTY_STRING_AS_NULL=YES'
    subprocess.run(pycmd)

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetTable}_archive;
        CREATE TABLE {dbTargetSchema}.{dbTargetTable}_archive
        AS SELECT * FROM {dbTargetSchema}.{dbTargetTable};
        ALTER TABLE  {dbTargetSchema}.{dbTargetTable}_archive OWNER TO cwf_analyst;

        ALTER TABLE {dbTargetSchema}.{dbTargetTable} DROP COLUMN IF EXISTS update_id;
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD COLUMN update_id uuid default gen_random_uuid();
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} DROP CONSTRAINT IF EXISTS {dbTargetTable}_pkey_v1;
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD CONSTRAINT {dbTargetTable}_pkey_v1 PRIMARY KEY (update_id);

    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def joinBarrierUpdates(connection):

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD COLUMN IF NOT EXISTS barrier_id uuid;

        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET snapped_point = original_point WHERE snapped_point IS NULL;
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)

    query = f"""
        SELECT DISTINCT barrier_type
        FROM {dbTargetSchema}.{dbTargetTable};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        barrierTypes = cursor.fetchall()

    for bType in barrierTypes:
        barrier = bType[0]
        query = f"""
        with match AS (
            SELECT
            foo.update_id,
            closest_point.id,
            closest_point.cabd_id,
            closest_point.modelled_id,
            closest_point.dist
            FROM {dbTargetSchema}.{dbTargetTable} AS foo
            CROSS JOIN LATERAL 
            (SELECT
                id,
                cabd_id,
                modelled_id,
                ST_Distance(bar.snapped_point, ST_Transform(foo.geometry, 2961)) as dist
                FROM {dbTargetSchema}.{dbBarrierTable} AS bar
                WHERE ST_DWithin(bar.snapped_point, ST_Transform(foo.geometry, 2961), {joinDistance})
                AND bar.type = '{barrier}'
                ORDER BY ST_Distance(bar.snapped_point, ST_Transform(foo.geometry, 2961))
                LIMIT 1
            ) AS closest_point
            WHERE foo.barrier_type = '{barrier}'
            )
        UPDATE {dbTargetSchema}.{dbTargetTable}
        SET barrier_id = a.id
        FROM match AS a WHERE a.update_id = {dbTargetSchema}.{dbTargetTable}.update_id 
        AND {dbTargetSchema}.{dbTargetTable}.update_type IN ('modify feature', 'delete feature');
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()
    
    

def processUpdates(connection):

    def processMultiple(connection):

        # where multiple updates exist for a feature, only update one at a time
        waitCount = 0
        waitQuery = f"""SELECT COUNT(*) FROM {dbTargetSchema}.{dbTargetTable} WHERE update_status = 'wait'"""

        while(True):
            with connection.cursor() as cursor:
                cursor.execute(initializeQuery)
                cursor.execute(waitQuery)
                waitCount = int(cursor.fetchone()[0])
                print("   ", waitCount, "updates are waiting to be made...")

                # update most fields
                cursor.execute(mappingQuery)

                query = f"""
                    UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'done' WHERE update_status = 'ready';
                    UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'ready' WHERE update_status = 'wait';
                """
                cursor.execute(query)

            
                connection.commit()

            if waitCount == 0:
                break

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD COLUMN IF NOT EXISTS update_status varchar;
        UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'ready';
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    
    initializeQuery = f"""
        WITH cte AS (
        SELECT update_id, barrier_id,
            row_number() OVER(PARTITION BY barrier_id ORDER BY update_date ASC) AS rn
        FROM {dbTargetSchema}.{dbTargetTable} WHERE update_status = 'ready'
        AND update_type = 'modify feature'
        )
        UPDATE {dbTargetSchema}.{dbTargetTable}
        SET update_status = 'wait'
            WHERE update_id IN (SELECT update_id FROM cte WHERE rn > 1);
    """
    with connection.cursor() as cursor:
        cursor.execute(initializeQuery)

    newDeleteQuery = f"""
        -- new points
        INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
            update_id, original_point, type, owner, 
            passability_status_notes,
            stream_name, date_examined,
            transport_feature_name,
            cmm_pt_exists
            )
        SELECT 
            update_id, ST_Transform(geometry, 2961), barrier_type, ownership, 
            notes,
            stream_name, date_examined,
            road_name,
            cmm_pt_exists
        FROM {dbTargetSchema}.{dbTargetTable}
        WHERE update_type = 'new feature'
        AND update_status = 'ready';

        -- barrier ids
        UPDATE {dbTargetSchema}.{dbTargetTable}
        SET barrier_id = b.id
        FROM {dbTargetSchema}.{dbBarrierTable} b
        WHERE b.update_id = {dbTargetSchema}.{dbTargetTable}.update_id::varchar;


        UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'done' WHERE update_type = 'new feature';

        -- deleted points
        DELETE FROM {dbTargetSchema}.{dbBarrierTable}
        WHERE id IN (
            SELECT barrier_id FROM {dbTargetSchema}.{dbTargetTable}
            WHERE update_type = 'delete feature'
            AND update_status = 'ready'
            );
        
        UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'done' WHERE update_type = 'delete feature';
    """

    with connection.cursor() as cursor:
        cursor.execute(newDeleteQuery)
    connection.commit()

    # passability status 
    global specCodes
    for s in specCodes:
        s = s[0]
        p_query = f"""
            INSERT INTO {dbTargetSchema}.{dbPassabilityTable} (
                barrier_id
                ,species_id
                ,passability_status
            )
            SELECT 
                b.id
                , (SELECT id
                    FROM {dbTargetSchema}.fish_species
                    WHERE code = '{s}')
                ,u.passability_status_{s}
            FROM {dbTargetSchema}.{dbBarrierTable} b
            JOIN {dbTargetSchema}.{dbTargetTable} u
                ON b.update_id = u.update_id::varchar
            WHERE u.update_type = 'new feature'
            AND update_status = 'ready';
        """

    with connection.cursor() as cursor:
        cursor.execute(p_query)
    connection.commit()

    updatequery = f"""
        UPDATE {dbTargetSchema}.barrier_passability b
        SET species_code = f.code
        FROM {dbTargetSchema}.fish_species f 
        WHERE f.id = b.species_id;
    """

    with connection.cursor() as cursor:
        cursor.execute(updatequery)
    connection.commit()

    joinBarrierUpdates(connection)

    mappingQuery = f"""
        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

        -- updated points
        UPDATE {dbTargetSchema}.{dbBarrierTable} AS b SET update_id = 
            CASE
            WHEN b.update_id IS NULL THEN a.update_id::varchar
            WHEN b.update_id IS NOT NULL THEN b.update_id::varchar || ',' || a.update_id::varchar
            ELSE NULL END
            FROM {dbTargetSchema}.{dbTargetTable} AS a
            WHERE b.id = a.barrier_id
            AND a.update_status = 'ready';

        UPDATE {dbTargetSchema}.{dbBarrierTable} AS b
        SET
            date_examined = CASE WHEN a.date_examined IS NOT NULL THEN a.date_examined ELSE b.date_examined END,
            transport_feature_name = CASE WHEN (a.road_name IS NOT NULL AND a.road_name IS DISTINCT FROM b.transport_feature_name) THEN a.road_name ELSE b.transport_feature_name END,
            crossing_subtype = CASE WHEN a.crossing_subtype IS NOT NULL THEN a.crossing_subtype ELSE b.crossing_subtype END,
            cmm_pt_exists = CASE WHEN a.cmm_pt_exists IS NOT NULL THEN a.cmm_pt_exists ELSE b.cmm_pt_exists END,
            passability_status_notes = 
                CASE
                WHEN a.notes IS NOT NULL AND b.passability_status_notes IS NULL THEN a.notes
                WHEN a.notes IS NOT NULL AND b.passability_status_notes IS NOT NULL AND b.passability_status_notes LIKE a.notes THEN b.passability_status_notes
                WHEN a.notes IS NOT NULL AND b.passability_status_notes IS NOT NULL THEN b.passability_status_notes || ';' || a.notes
                ELSE b.passability_status_notes END
        FROM {dbTargetSchema}.{dbTargetTable} AS a
        WHERE b.id = a.barrier_id
        AND a.update_status = 'ready';
    """
    # update passability
    for s in specCodes:
        s = s[0]
        mappingQuery = f"""
            {mappingQuery}

            UPDATE {dbTargetSchema}.{dbPassabilityTable} AS p
            SET
                passability_status = 
                    CASE WHEN a.passability_status_{s} IS NOT NULL 
                        AND a.passability_status_{s} IS DISTINCT FROM p.passability_status 
                        THEN a.passability_status_{s} 
                    ELSE p.passability_status END
            FROM {dbTargetSchema}.{dbTargetTable} AS a
            WHERE p.barrier_id = a.barrier_id
            AND a.update_status = 'ready'
            AND p.species_code = '{s}';
        """ 

    processMultiple(connection)

    removeDuplicatesQuery = f"""
        --delete duplicate points in a narrow tolerance
        DELETE FROM {dbTargetSchema}.{dbBarrierTable} b1
        WHERE EXISTS (SELECT FROM {dbTargetSchema}.{dbBarrierTable} b2
            WHERE b1.id > b2.id
            AND ST_DWithin(b1.snapped_point, b2.snapped_point, 1));
    """
    # print(removeDuplicatesQuery)
    with connection.cursor() as cursor:
        cursor.execute(removeDuplicatesQuery)
    connection.commit()

    if secondaryWatershedTable != 'None':
        # get secondary watershed names
        query = f"""
        UPDATE {dbTargetSchema}.{dbBarrierTable} b SET secondary_wshed_name = a.sec_name FROM {appconfig.dataSchema}.{secondaryWatershedTable} a WHERE ST_INTERSECTS(b.snapped_point, a.geometry);
        """

        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()

def matchArchive(connection):

    query = f"""
        WITH matched AS (
            SELECT
            a.update_id,
            nn.update_id as archive_id,
            nn.dist
            FROM {dbTargetSchema}.{dbTargetTable} a
            CROSS JOIN LATERAL
            (SELECT
            update_id,
            ST_Distance(a.geometry, b.geometry) as dist
            FROM {dbTargetSchema}.{dbTargetTable}_archive b
            ORDER BY a.geometry <-> b.geometry
            LIMIT 1) as nn
            WHERE nn.dist < 10
        )

        UPDATE {dbTargetSchema}.{dbTargetTable} a
            SET update_id = m.archive_id::uuid
            FROM matched m
            WHERE m.update_id = a.update_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)


def tableExists(conn):

    query = f"""
    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
    WHERE table_catalog='{appconfig.dbName}' AND 
        table_schema='{dbTargetSchema}' AND 
        table_name='{dbTargetTable}_archive');
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        result = result[0]

    return result

#--- main program ---
def main():
        
    with appconfig.connectdb() as conn:

        conn.autocommit = False

        global specCodes

        specCodes = [substring.strip() for substring in specCodes.split(',')]

        if len(specCodes) == 1:
            specCodes = f"('{specCodes[0]}')"
        else:
            specCodes = tuple(specCodes)

        query = f"""
        SELECT code
        FROM {dataSchema}.{appconfig.fishSpeciesTable}
        WHERE code IN {specCodes};
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            specCodes = cursor.fetchall()
        
        conn.commit()
        

        print("Loading Barrier Updates")
        loadBarrierUpdates(conn)
        
        print("  joining update points to barriers")
        joinBarrierUpdates(conn)
        
        print("  processing updates")
        processUpdates(conn)

        # result = tableExists(conn)
        
        # if result:
        #     matchArchive(conn)
        
    print("done")
    
if __name__ == "__main__":
    main()   