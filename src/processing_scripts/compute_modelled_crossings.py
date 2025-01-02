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
# This script computes modelled crossings (stream intersections
# with road/trail network) and attributes that can be derived
# from the stream network
#

"""
* OUTPUT
* Creates and loads
modelled_crossings
modelled_crossings_archive (if modelled_crossings exists)
* Modifies
barriers - all modelled crossings are added to the barriers table
barrier_passability - passability for all crossings are added to the passability table

"""
import appconfig

from appconfig import dataSchema

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbWatershedId = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']

roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
    
dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']
# secondaryWatershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['secondary_watershed_table']
secondaryWatershedTable = appconfig.secondaryWatershedTable

def tableExists(connection):
    """
    Returns whether the modelled crossings table exists in the database already
    Necessary for creating the archive
    """

    query = f"""
    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
    WHERE table_catalog='{appconfig.dbName}' AND 
        table_schema='{dbTargetSchema}' AND 
        table_name='{dbModelledCrossingsTable}');
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        result = result[0]

    return result

def createTable(connection):

    result = tableExists(connection)

    if result:

        # create an archive table so we can keep modelled_id stable
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable}_archive;
            CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable}_archive 
            AS SELECT * FROM {dbTargetSchema}.{dbModelledCrossingsTable};
            
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable};
            
            CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable} (
                modelled_id uuid default gen_random_uuid(),
                stream_name varchar,
                strahler_order integer,
                stream_id uuid, 
                transport_feature_name varchar,
              
                crossing_status varchar,
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,
                
                geometry geometry(Point, {appconfig.dataSrid}),
                
                primary key (modelled_id)
            );

            ALTER TABLE  {dbTargetSchema}.{dbModelledCrossingsTable}_archive OWNER TO cwf_analyst;
            ALTER TABLE  {dbTargetSchema}.{dbModelledCrossingsTable} OWNER TO cwf_analyst;
            
        """
    
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()
        
        # add species-specific passability fields
        for species in specCodes:
            code = species[0]

            colname = "passability_status_" + code
            
            query = f"""
                alter table {dbTargetSchema}.{dbModelledCrossingsTable} 
                add column if not exists {colname} numeric;
            """

            with connection.cursor() as cursor:
                cursor.execute(query)
            connection.commit()
    
    else:
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable};
            
            CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable} (
                modelled_id uuid default gen_random_uuid(),
                stream_name varchar,
                strahler_order integer,
                stream_id uuid, 
                transport_feature_name varchar,
                
                crossing_status varchar,
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,
                
                geometry geometry(Point, {appconfig.dataSrid}),
                
                primary key (modelled_id)
            );

            ALTER TABLE  {dbTargetSchema}.{dbModelledCrossingsTable} OWNER TO cwf_analyst;
            
        """
    
        with connection.cursor() as cursor:
            cursor.execute(query)

        # add species-specific passability fields 
        for species in specCodes:
            code = species[0]

            colname = "passability_status_" + code
            
            query = f"""
                alter table {dbTargetSchema}.{dbModelledCrossingsTable} 
                add column if not exists {colname} numeric;
            """

            with connection.cursor() as cursor:
                cursor.execute(query)

def computeCrossings(connection):
        
    query = f"""
        --roads
        ALTER TABLE {appconfig.dataSchema}.{roadTable} ADD COLUMN IF NOT EXISTS id uuid;
        UPDATE {appconfig.dataSchema}.{roadTable} SET id = gen_random_uuid();
        ALTER TABLE {appconfig.dataSchema}.{roadTable} DROP CONSTRAINT IF EXISTS road_pkey;
        ALTER TABLE {appconfig.dataSchema}.{roadTable} ADD PRIMARY KEY (id);

        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b.street as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{roadTable} b
                where st_intersects(a.geometry, b.geometry)
                and b.roadc_desc NOT IN ('Abandoned Rail Road', 'Active Rail Road', 'Ferry Connector', 'Trail')
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, transport_feature_name, 'ROAD', pnt 
            from points
        );
        
        --rail
        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b.street as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{roadTable} b
                where st_intersects(a.geometry, b.geometry)
                and b.roadc_desc IN ('Abandoned Rail Road', 'Active Rail Road')
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, transport_feature_name, 'RAIL', pnt 
            from points
        );
        
        --trail
        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b.street as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{roadTable} b
                where st_intersects(a.geometry, b.geometry)
                and b.roadc_desc = 'Trail'
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, transport_feature_name, 'TRAIL', pnt 
            from points
        );

        --get bridge and tunnel subtypes
        --buffer by a small distance to ensure intersection
        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable} a SET crossing_type = 'obs', crossing_subtype = 'bridge'
        FROM {appconfig.dataSchema}.{roadTable} b
        WHERE st_intersects(st_buffer(a.geometry, 0.001), b.geometry) AND b.feat_desc ILIKE '%bridge%';

        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable} a SET crossing_subtype = 'tunnel'
        FROM {appconfig.dataSchema}.{roadTable} b
        WHERE st_intersects(st_buffer(a.geometry, 0.001), b.geometry) AND b.feat_desc ILIKE '%tunnel%';

        --delete any duplicate points within a very narrow tolerance
        --duplicate points may result from transport features being broken on streams
        DELETE FROM {dbTargetSchema}.{dbModelledCrossingsTable} p1
        WHERE EXISTS (SELECT FROM {dbTargetSchema}.{dbModelledCrossingsTable} p2
            WHERE p1.modelled_id > p2.modelled_id
            AND ST_DWithin(p1.geometry,p2.geometry,0.01));

    """
    # print(query)
    with connection.cursor() as cursor:
        cursor.execute(query)

def matchArchive(connection):

    query = f"""
        WITH matched AS (
            SELECT
            a.modelled_id,
            nn.modelled_id as archive_id,
            nn.dist
            FROM {dbTargetSchema}.{dbModelledCrossingsTable} a
            CROSS JOIN LATERAL
            (SELECT
            modelled_id,
            ST_Distance(a.geometry, b.geometry) as dist
            FROM {dbTargetSchema}.{dbModelledCrossingsTable}_archive b
            ORDER BY a.geometry <-> b.geometry
            LIMIT 1) as nn
            WHERE nn.dist < 10
        )

        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable} a
            SET modelled_id = m.archive_id
            FROM matched m
            WHERE m.modelled_id = a.modelled_id;

        DROP TABLE {dbTargetSchema}.{dbModelledCrossingsTable}_archive;

    """
    with connection.cursor() as cursor:
        cursor.execute(query)

def computeAttributes(connection):
    
    #assign all modelled crossings a crossing_status and passability_status
    
    query = f"""
        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
        SET crossing_status = 'MODELLED';
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    
    for species in specCodes:
        code = species[0]

        colname = "passability_status_" + code
            
        query = f"""
            UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
            SET {colname} = 1 WHERE crossing_subtype = 'bridge';

            UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
            SET {colname} = 0 WHERE {colname} IS NULL;
        """

        with connection.cursor() as cursor:
            cursor.execute(query)

def loadToBarriers(connection):

    newCols = []

    for species in specCodes:
        code = species[0]

        col = "passability_status_" + code
        newCols.append(col)
    
    colString = ','.join(newCols)

    query = f"""
        DELETE 
        FROM {dbTargetSchema}.{dbPassabilityTable} bp
        USING {dbTargetSchema}.{dbBarrierTable} b 
        WHERE b.id = bp.barrier_id AND 
            b.type = 'stream_crossing';

        DELETE FROM {dbTargetSchema}.{dbBarrierTable} WHERE type = 'stream_crossing';

        
        INSERT INTO {dbTargetSchema}.{dbBarrierTable}(
            id,
            modelled_id, snapped_point,
            type,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype
        )
        SELECT 
            modelled_id,
            modelled_id, geometry,
            'stream_crossing',
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype
        FROM {dbTargetSchema}.{dbModelledCrossingsTable};

        UPDATE {dbTargetSchema}.{dbBarrierTable} SET wshed_name = '{dbWatershedId}';
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');  
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

    if secondaryWatershedTable != 'None':
        query = f'UPDATE {dbTargetSchema}.{dbBarrierTable} b SET secondary_wshed_name = a.sec_name FROM {appconfig.dataSchema}.{secondaryWatershedTable} a WHERE ST_INTERSECTS(b.snapped_point, a.geometry);'
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()
    else:
        # defualt to wcrp name
        query = f"UPDATE {dbTargetSchema}.{dbBarrierTable} b SET secondary_wshed_name = '{iniSection}'"
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()

    query = f"""
        SELECT id 
        FROM {dbTargetSchema}.fish_species
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        species = cursor.fetchall()
    connection.commit()

    passability_data = []
    
    query = f"""
        SELECT b.id, b.crossing_subtype
        FROM {dbTargetSchema}.{dbBarrierTable} b
        JOIN {dbTargetSchema}.{dbModelledCrossingsTable} c
            ON b.modelled_id = c.modelled_id;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        feature_data = cursor.fetchall()
    connection.commit()

    # Get data for passability table
    for feature in feature_data:
        for s in species:
            passability_feature = []
            passability_feature.append(feature[0])
            passability_feature.append(s[0])
            if feature[1] == 'bridge':
                passability_feature.append(1)
            else:
                passability_feature.append(0)
            passability_data.append(passability_feature)

    insertquery = f"""
        INSERT INTO {dbTargetSchema}.{dbPassabilityTable} (
            barrier_id
            ,species_id
            ,passability_status
        )
        VALUES(%s, %s, %s);
    """

    with connection.cursor() as cursor:
        for feature in passability_data:
            cursor.execute(insertquery, feature)
    connection.commit()




def main():                        
    #--- main program ---    
    with appconfig.connectdb() as conn:

        conn.autocommit = False
        
        print("Computing Modelled Crossings")

        query = f"""
        SELECT code
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
        """

        global specCodes

        with conn.cursor() as cursor:
            cursor.execute(query)
            specCodes = cursor.fetchall()

        result = tableExists(conn)

        print("  creating tables")
        createTable(conn)
        
        print("  computing modelled crossings")
        computeCrossings(conn)

        if result:

            print("  matching to archived crossings")
            matchArchive(conn)
            
            conn.commit()

        print("  calculating modelled crossing attributes")
        computeAttributes(conn)

        print("  loading to barriers table")
        loadToBarriers(conn)
        
        conn.commit()
                
    print("done")

if __name__ == "__main__":
    main() 