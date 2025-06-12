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
# Removes isolated flowpaths from the stream network
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
#
import appconfig
import networkx as nx
import shapely.wkb
from tqdm import tqdm
import psycopg2.extras as pg2e

import sys

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']



def createTable(connection):
    """
    Creates a table to store the new version of the streams table
    """

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable}_copy;

        CREATE TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy 
        AS SELECT * FROM {dbTargetSchema}.{dbTargetStreamTable};

        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy OWNER TO cwf_analyst;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def disconnectedIslands(conn):
    """
    Creates an undirected graph of stream network and 
    groups by connected portions.
    Result is an additional column in the streams_copy table indicating
    which network group the stream portion belongs to

    Based on algorithm in disconnected islands plugin
    :see: https://github.com/AfriGIS-South-Africa/disconnected-islands/blob/master/disconnected_islands.py
    """
    G = nx.Graph()
    tolerance = 0.000001

    # Get the stream network
    query = f"""
        SELECT id, geometry 
        FROM {dbTargetSchema}.{dbTargetStreamTable}_copy
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()

    for feat in features:
        geom = shapely.wkb.loads(feat[1], hex=True) # linestring from feature of stream network


        for i in range(len(geom.coords)-1): 
            G.add_edges_from(
                [
                    ((int(geom.coords[i][0]/tolerance), int(geom.coords[i][1]/tolerance)),
                     (int(geom.coords[i+1][0]/tolerance), int(geom.coords[i+1][1]/tolerance)),
                     {'fid': feat[0]})
                ]
            )

    query = f"""
        DROP TABLE IF EXISTS network_groups;
        CREATE TABLE network_groups
        (
            id uuid,
            ng int
        );
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

    print("    finding connected subgraphs")
    connected_components = list(G.subgraph(c) for c in sorted(nx.connected_components(G), key=len, reverse=True))
    numGroups = len(connected_components)

    print("    writing results")

    insertQuery =f"""
        INSERT INTO network_groups (id, ng)
        VALUES %s;
    """

    with conn.cursor() as cursor:
        for i, graph in enumerate(tqdm(connected_components, miniters=1000)):
            # ignore streams on largest network group (networkGrp = 0)
            # since most will be on this group
            # and default value is already 0
            # this speeds up the loop to only find streams off main network
            if i == 0:
                continue
            data = []
            for edge in graph.edges(data=True):
                    data.append(tuple([edge[2].get('fid'), i]))
                    # cursor.execute(updateQuery, (i, edge[2].get('fid')))
                    # pg2e.execute_values(cursor, insertQuery, data, template='(%s, %s::uuid)')
                    conn.commit()
            pg2e.execute_values(cursor, insertQuery, data, template='(%s::uuid, %s)')
            # conn.commit()

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy
            ADD COLUMN IF NOT EXISTS networkGrp int DEFAULT 0;

        UPDATE {dbTargetSchema}.{dbTargetStreamTable}_copy AS s
        SET networkGrp = ng.ng
        FROM (SELECT id, ng FROM network_groups) AS ng
        WHERE s.id = ng.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
    

    # REMOVE IF NO ISSUES WHEN RUN ON CMM, MARGAREE, ETC
    # print("    finding connected subgraphs")
    # connected_components = list(G.subgraph(c) for c in sorted(nx.connected_components(G), key=len, reverse=True))

    # query = f"""
    #     ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy
    #         ADD COLUMN IF NOT EXISTS networkGrp int DEFAULT 0
    # """
    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    #     conn.commit()

    # print("    writing results")
    # newData = []
    # updateQuery = f"""
    #     UPDATE {dbTargetSchema}.{dbTargetStreamTable}_copy
    #     SET networkGrp = %s
    #     WHERE id = %s;
    # """
    # updates = []
    # conn.autocommit = False
    # with conn.cursor() as cursor:
    #     for i, graph in enumerate(tqdm(connected_components)):
    #         # ignore streams on largest network group (networkGrp = 0)
    #         # since most will be on this group
    #         # and default value is already 0
    #         # this speeds up the loop to only label streams off main network
    #         if i == 0:
    #             continue
    #         for edge in graph.edges(data=True):
    #             updates.append((i, edge[2].get('fid')))
    #             # cursor.execute(updateQuery, (i, edge[2].get('fid')))
    #             if len(updates) >= 1000:
    #                 cursor.executemany(updateQuery, updates)
    #                 updates.clear()
    #     if updates:
    #         cursor.executemany(updateQuery, updates)
    # conn.commit()
    # conn.clost()
    # with conn.cursor() as cursor:
    #     cursor.executemany(updateQuery, updates)
    # conn.commit()
                
    


def dissolveFeatures(conn):
    """ Dissolve streams by network group """

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable}_dissolved;

        CREATE TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved 
        AS SELECT networkGrp, ST_UNION(geometry)::GEOMETRY(Geometry, 2961) as geometry
            FROM {dbTargetSchema}.{dbTargetStreamTable}_copy
            WHERE networkGrp != 0
            GROUP BY networkGrp;
        
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved ADD COLUMN id SERIAL PRIMARY KEY;
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved ALTER COLUMN geometry 
            SET DATA TYPE geometry(MultiLineString, 2961) USING ST_Multi(geometry);

        CREATE INDEX ON {dbTargetSchema}.{dbTargetStreamTable}_dissolved using gist (geometry);

        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved OWNER TO cwf_analyst;

        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable}_dissolved d
        USING public.chyf_shoreline as sh
        WHERE st_intersects(st_buffer(st_transform(sh.geometry, {appconfig.dataSrid}), 0.01), d.geometry);

    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    
    conn.commit()

def deleteIsolated(conn):
    """ Delete isolated streams from streams table """

    query = f"""
        DELETE 
        FROM {dbTargetSchema}.{dbTargetStreamTable} s
        USING {dbTargetSchema}.{dbTargetStreamTable}_dissolved d
        WHERE st_intersects(s.geometry, d.geometry);

        DROP TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy;
        DROP TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


def main():
    if iniSection == 'cheticamp':
        return
    
    with appconfig.connectdb() as conn:

        conn.autocommit = False

        print("Removing Isolated Flowpaths")

        print("  copying stream table")
        createTable(conn)

        print("  grouping networks")
        disconnectedIslands(conn)

        print("  dissolving features")
        dissolveFeatures(conn)

        print("  deleting isolated flowpaths")
        deleteIsolated(conn)

    print("done")


        

if __name__ == "__main__":
    main()     