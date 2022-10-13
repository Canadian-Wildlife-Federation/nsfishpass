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
# with road/rail/trail network) and attributes that can be derived
# from the stream network
#
import appconfig

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']
orderBarrierLimit = appconfig.config['CROSSINGS']['strahler_order_barrier_limit']

roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
    
dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']

    
def createTable(connection):
    #note: we will add to this table for
    #each species as the values
    #are computed for those species
    
    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable};
        
        CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable} (
            modelled_id uuid default uuid_generate_v4(),
            stream_name varchar,
            strahler_order integer,
            stream_id uuid, 
            -- stream_measure numeric,
            -- wshed_name varchar,
            -- wshed_priority varchar,
            transport_feature_name varchar,
            -- ownership_type varchar,
            
            -- species_upstr varchar[],
            -- species_downstr varchar[],
            -- stock_upstr varchar[],
            -- stock_downstr varchar[],
            
            -- barriers_upstr varchar[],
            -- barriers_downstr varchar[],
            -- barrier_cnt_upstr integer,
            -- barrier_cnt_downstr integer,
            
            -- critical_habitat varchar[],
            
            passability_status varchar,
            -- last_inspection date,
            
            crossing_status varchar,
            crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
            crossing_type varchar,
            crossing_subtype varchar,
            
            -- habitat_quality varchar,
            -- year_planned integer,
            -- year_complete integer,
            -- comments varchar,
            
            geometry geometry(Point, {appconfig.dataSrid}),
            
            primary key (modelled_id)
        );
        
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        


def computeCrossings(connection):
        
    query = f"""
        --roads
        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b."name" as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{roadTable} b
                where st_intersects(a.geometry, b.geometry)
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
            (stream_name, strahler_order, stream_id, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{railTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, 'RAIL', pnt 
            from points
        );
        
        --trail
        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b."name" as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{trailTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, transport_feature_name, 'TRAIL', pnt 
            from points
        );
        
        --delete any duplicate points within a very narrow tolerance
        --duplicate points may result from transport features being broken on streams
        DELETE FROM {dbTargetSchema}.{dbModelledCrossingsTable} p1
        WHERE EXISTS (SELECT FROM {dbTargetSchema}.{dbModelledCrossingsTable} p2
            WHERE p1.modelled_id > p2.modelled_id
            AND ST_DWithin(p1.geometry,p2.geometry,0.01));

    """
    #print(query)
    with connection.cursor() as cursor:
        cursor.execute(query)

def computeAttributes(connection):
    
    #assign all modelled crossings on 6th order streams and above a 
    #crossing_subtype of 'bridge' and a passability_status of 'passable'
    
    query = f"""
        --set every crossing to crossing_status 'modelled'
        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
        SET crossing_status = 'MODELLED';

        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
        SET 
            crossing_type = 'obs',
            crossing_subtype = 'bridge',
            passability_status = 'PASSABLE'
        WHERE strahler_order >= {orderBarrierLimit};
        
        --set everything else to potential for now
        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
        SET passability_status = 'POTENTIAL BARRIER'
        WHERE passability_status is null;
    """
    #print(query)
    with connection.cursor() as cursor:
        cursor.execute(query)

def main():                        
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Modelled Crossings")
        
        print("  creating tables")
        createTable(conn)
        
        print("  computing modelled crossings")
        computeCrossings(conn)

        print("  calculating modelled crossing attributes")
        computeAttributes(conn)
        
        conn.commit()
                
    print("done")

if __name__ == "__main__":
    main() 