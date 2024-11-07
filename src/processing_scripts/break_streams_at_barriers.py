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
# snaps barriers to the stream table
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
#
import appconfig
from imagecodecs.imagecodecs import NONE

import sys

iniSection = appconfig.args.args[0]
dataSchema = appconfig.config['DATABASE']['data_schema']
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']
dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']
dbCrossingsTable = appconfig.config['CROSSINGS']['crossings_table']
dbVertexTable = appconfig.config['GRADIENT_PROCESSING']['vertex_gradient_table']
dbTargetGeom = appconfig.config['ELEVATION_PROCESSING']['smoothedgeometry_field']
dbGradientBarrierTable = appconfig.config['BARRIER_PROCESSING']['gradient_barrier_table']
dbHabAccessUpdates = "habitat_access_updates"
specCodes = appconfig.config[iniSection]['species']

# stream order segment weighting
w1 = 0.25
w2 = 0.75

# with appconfig.connectdb() as conn:

#     query = f"""
#     SELECT code
#     FROM {dataSchema}.{appconfig.fishSpeciesTable};
#     """

#     with conn.cursor() as cursor:
#         cursor.execute(query)
#         specCodes = cursor.fetchall()

def insertPassability(conn, passability_data):
    """
    Insert data into the barrier_passability table
    """
    if len(passability_data) == 0:
        return

    insertquery = f"""
        INSERT INTO {dbTargetSchema}.barrier_passability (
            barrier_id
            ,species_id
            ,species_code
            ,passability_status
        )
        VALUES(%s, %s, %s, %s);
    """

    with conn.cursor() as cursor:
        for feature in passability_data:
            cursor.execute(insertquery, feature)
    conn.commit()

def breakstreams (conn):
        
    # find all break points
    # all barriers regardless of passability status (dams, modelled crossings, and assessed crossings)
    # all gradient barriers (Vertex gradient > min fish gradient)
    #  -> these are a bit special as we only want to break once for
    #     a segment if vertex gradient continuously large 

    newCols = []
    for species in specCodes:
        code = species[0]
        col = "passability_status_" + code
        newCols.append(col)
    colString = ' numeric,'.join(newCols)
    colStringSimple = ','.join(newCols)
    
    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbGradientBarrierTable};
            
        CREATE TABLE {dbTargetSchema}.{dbGradientBarrierTable}(
            point geometry(POINT, {appconfig.dataSrid}),
            id uuid,
            type varchar,
            {colString} numeric
            );
    
        -- barriers
        INSERT INTO {dbTargetSchema}.{dbGradientBarrierTable} (point, id, type) 
            SELECT snapped_point, id, type
            FROM {dbTargetSchema}.{dbBarrierTable};

        --habitat and accessibility updates
        -- INSERT INTO {dbTargetSchema}.{dbGradientBarrierTable} (point, id, type)
        --    SELECT snapped_point, id, update_type
        --    FROM {dbTargetSchema}.{dbHabAccessUpdates};

        ALTER TABLE  {dbTargetSchema}.{dbGradientBarrierTable} OWNER TO cwf_analyst;
    """
        
    # print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
        
    # break at gradient points

    query = f"""
        SELECT accessibility_gradient as minvalue, code
        FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable}
        WHERE accessibility_gradient = (SELECT min(accessibility_gradient) FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable});
    """
    
    mingradient = -1
    
    with conn.cursor() as cursor:
        # print(query)
        cursor.execute(query)
        features = cursor.fetchall()
        mingradient = features[0][0]
        code = features[0][1]
        
    query = f"""    
        SELECT mainstem_id, st_Force2d(vertex_pnt), gradient
        FROM {dbTargetSchema}.{dbVertexTable}
        ORDER BY mainstem_id, downstream_route_measure
    """
        
    with conn.cursor() as cursor:
        cursor.execute(query)
        
        features = cursor.fetchall()
        
        lastmainstem = NONE
        lastgradient = -1
        
        for feature in features:
            mainstem = feature[0]
            point = feature[1]
            gradient = feature[2]
            
            insert = False
            if (lastmainstem != mainstem and gradient > mingradient):
                #we need to find what the gradient is at the downstream point here
                # and only add this as a break point
                # if downstream vertex is < 0.15
                query = f"""
                
                    with pnt as (
                        SELECT st_endpoint(a.geometry) as endpnt
                        FROM {dbTargetSchema}.{dbTargetStreamTable} a
                        WHERE st_intersects( a.geometry, '{point}')
                    )
                    SELECT gradient
                    FROM {dbTargetSchema}.{dbVertexTable} a, pnt
                    WHERE a.vertex_pnt && pnt.endpnt
                    AND gradient <= {mingradient}
                """ 
                #print(query)
                with conn.cursor() as cursor3:
                    cursor3.execute(query)
                    features3 = cursor3.fetchall()
                    if (len(features3) > 0):
                        insert = True
    
                

            elif (gradient > mingradient) and \
                not((lastgradient > mingradient and gradient > mingradient and lastmainstem == mainstem)):
                
                insert = True
            
            if insert:
                # this is a point that is not the first point on a new mainstem 
                # has a gradient larger than required values
                # and has a downstream gradient that is less than required values  
                query = f"""INSERT INTO {dbTargetSchema}.{dbGradientBarrierTable} (point, id, type, passability_status_{code}) values ('{point}', gen_random_uuid(), 'gradient_barrier', 0);""" 
                with conn.cursor() as cursor2:
                    cursor2.execute(query)

                # set gradient barriers to be passable for all other species
                for species in specCodes:
                    if species[0] != code:
                        foo = species[0]
                        col = "passability_status_" + foo
                        query = f"""
                            UPDATE {dbTargetSchema}.{dbGradientBarrierTable} SET {col} = 1;
                        """
                        with conn.cursor() as cursor2:
                            cursor2.execute(query)
                    else:
                        continue

            lastmainstem = mainstem
            lastgradient = gradient

        # add gradient barriers to passability table
        query = f"""
            SELECT id
            FROM {dbTargetSchema}.{dbGradientBarrierTable}
            WHERE id NOT IN (SELECT barrier_id FROM {dbTargetSchema}.barrier_passability)
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            feature_data = cursor.fetchall()
        conn.commit()

        query = f"""
            SELECT id, code
            FROM {dbTargetSchema}.fish_species
            WHERE code = '{code}'
        """ 
        # print(query)
        with conn.cursor() as cursor:
            cursor.execute(query)
            species = cursor.fetchall()
        conn.commit()

        query = f"""
            SELECT id, code
            FROM {dbTargetSchema}.fish_species
            WHERE code != '{code}'
        """
        # print(query)
        with conn.cursor() as cursor:
            cursor.execute(query)
            other_species = cursor.fetchall()
        conn.commit()

        passability_data = []
        other_passability_data = [] # barriers passable for all other species

        for feature in feature_data:
            passability_feature = []
            other_passability_feature = []
            for s in species:
                passability_feature.append(feature[0])
                passability_feature.append(s[0])
                passability_feature.append(s[1])
                passability_feature.append(0)
            for s in other_species:
                other_passability_feature.append(feature[0])
                other_passability_feature.append(s[0])
                other_passability_feature.append(s[1])
                other_passability_feature.append(1)
            if len(passability_feature) != 0:
                passability_data.append(passability_feature)
            if len(other_passability_feature) != 0:
                other_passability_data.append(other_passability_feature)
        
        insertPassability(conn, passability_data)
        insertPassability(conn, other_passability_data)
   
            
    #break streams at snapped points
    #todo: may want to ensure this doesn't create small stream segments - 
    #ensure barriers are not on top of each other
    conn.commit()
    print("breaking streams")
    
    query = f"""
        CREATE TABLE {dbTargetSchema}.newstreamlines AS
        
        with breakpoints as (
            SELECT a.{appconfig.dbIdField} as id, 
                a.geometry,
                st_collect(st_lineinterpolatepoint(a.geometry, st_linelocatepoint(a.geometry, b.point))) as rawpnt
            FROM 
                {dbTargetSchema}.{dbTargetStreamTable} a,  
                {dbTargetSchema}.{dbGradientBarrierTable} b 
            WHERE st_distance(st_force2d(a.geometry_smoothed3d), b.point) < 0.01
            GROUP BY a.{appconfig.dbIdField}
        ),
        newlines as (
            SELECT {appconfig.dbIdField},
                st_split(st_snap(geometry, rawpnt, 0.001), rawpnt) as geometry
            FROM breakpoints 
        )
        
        SELECT z.{appconfig.dbIdField},
                y.source_id,
                y.{appconfig.dbWatershedIdField},
                y.sec_code,
                y.sec_name,
                y.stream_name,
                y.strahler_order,
                {appconfig.streamTableChannelConfinementField},
                {appconfig.streamTableDischargeField},
                y.mainstem_id,
                st_geometryn(z.geometry, generate_series(1, st_numgeometries(z.geometry))) as geometry
        FROM newlines z JOIN {dbTargetSchema}.{dbTargetStreamTable} y 
             ON y.{appconfig.dbIdField} = z.{appconfig.dbIdField};
        
        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} 
        WHERE {appconfig.dbIdField} IN (SELECT {appconfig.dbIdField} FROM {dbTargetSchema}.newstreamlines);
        
              
        INSERT INTO  {dbTargetSchema}.{dbTargetStreamTable} 
            (id, source_id, {appconfig.dbWatershedIdField}, sec_code, sec_name, stream_name, strahler_order, 
            segment_length, w_segment_length,
            {appconfig.streamTableChannelConfinementField},{appconfig.streamTableDischargeField},
            mainstem_id, geometry)
        SELECT gen_random_uuid(), a.source_id, a.{appconfig.dbWatershedIdField}, a.sec_code, a.sec_name,
            a.stream_name, a.strahler_order,
            st_length2d(a.geometry) / 1000.0, 
            case strahler_order 
            when 1 then (st_length2d(a.geometry) / 1000.0) * {w1}
            when 2 then (st_length2d(a.geometry) / 1000.0) * {w2}
            else (st_length2d(a.geometry) / 1000.0)
            end,
            a.{appconfig.streamTableChannelConfinementField},
            a.{appconfig.streamTableDischargeField}, 
            mainstem_id, a.geometry
        FROM {dbTargetSchema}.newstreamlines a;

        UPDATE {dbTargetSchema}.{dbTargetStreamTable} set geometry = st_snaptogrid(geometry, 0.01);
        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE ST_IsEmpty(geometry);

        DROP INDEX IF EXISTS {dbTargetSchema}."smooth_geom_idx";
        CREATE INDEX smooth_geom_idx ON {dbTargetSchema}.{dbTargetStreamTable} USING gist({dbTargetGeom});
        
        DROP TABLE {dbTargetSchema}.newstreamlines;
    
    """
        
    # print(query)
        
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def recomputeMainstreamMeasure(connection):
    
    query = f"""
        WITH mainstems AS (
            SELECT st_reverse(st_linemerge(st_collect(geometry))) as geometry, mainstem_id
            FROM {dbTargetSchema}.{dbTargetStreamTable}
            GROUP BY mainstem_id
        ),
        measures AS (
            SELECT 
                (st_linelocatepoint(b.geometry, st_startpoint(a.geometry)) * st_length(b.geometry)) / 1000.0 as startpct, 
                (st_linelocatepoint(b.geometry, st_endpoint(a.geometry)) * st_length(b.geometry))  / 1000.0 as endpct,
                a.id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a, mainstems b
            WHERE a.mainstem_id = b.mainstem_id
        )
        UPDATE {dbTargetSchema}.{dbTargetStreamTable}
        SET downstream_route_measure = measures.endpct, 
            upstream_route_measure = measures.startpct
        FROM measures
        WHERE measures.id = {dbTargetSchema}.{dbTargetStreamTable}.id
    """
    # print(query)
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)

def updateBarrier(connection):
    
    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS stream_id_up uuid;
        
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET stream_id_up = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbBarrierTable} b
            WHERE st_dwithin(a.geometry, b.snapped_point, 0.01) and
                st_dwithin(st_endpoint(a.geometry), b.snapped_point, 0.01)
        )
        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET stream_id_up = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbBarrierTable}.id;
            
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS stream_id_down uuid;

        UPDATE {dbTargetSchema}.{dbBarrierTable} SET stream_id_down = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbBarrierTable} b
            WHERE st_dwithin(a.geometry, b.snapped_point, 0.01) and
                st_dwithin(st_startpoint(a.geometry), b.snapped_point, 0.01)
        )
        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET stream_id_down = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbBarrierTable}.id;
    """

    # print(query)
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()           
                        
def main():
    with appconfig.connectdb() as connection:

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

        with connection.cursor() as cursor:
            cursor.execute(query)
            specCodes = cursor.fetchall()

        print("    breaking streams at barrier points")
        breakstreams(connection)
        
        print("    recomputing mainstem measures")
        recomputeMainstreamMeasure(connection)
    
        print("    updating barrier stream references")
        updateBarrier(connection)
    
    print("Breaking stream complete.")
    
if __name__ == "__main__":
    main()     