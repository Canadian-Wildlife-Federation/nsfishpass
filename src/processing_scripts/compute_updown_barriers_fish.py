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
# This script calculates how many barriers exist upstream and downstream of each stream segment in the network, tracking this information separately for each fish species. The result from this script is a streams table updated with species-specific barrier counts and IDs for every segment. There are 6 main steps to how this script operates.
# 
# DESCRIPTION
# 
# 1.	Build a network graph structure: 
# -	Reads all stream segments from the database
# -	Creates nodes and edges forming a connected network
# -	Each edge stores its geometry, database ID, and sets to hold barrier information
# -	Each node can have multiple incoming edges (tributaries) and outgoing edges (downstream connections)
# 2.	Add barriers to the network: 
# -	Queries for regular barriers (dams, assessed crossings) that are impassable for the current species
# -	Identifies whether each barrier sits at the upstream end (start point) or downstream end (end point) of a stream segment
# -	Stores barrier IDs in the appropriate nodes
# -	Separately queries for gradient barriers (steep sections, waterfalls) using the same logic
# -	Only considers barriers where passability_status != 1 (not quite passable) for the specific fish species
# 3.	Traverse downstream (headwaters to outlet): 
# -	Uses a queue-based algorithm starting from headwater nodes (those with no incoming edges)
# -	For each node, waits until all upstream edges have been visited to maintain order
# -	Collects all barriers found upstream by: 
#   -	Combining barriers from all incoming tributary edges
#   -	Adding any barriers located at this node itself
# -	Propagates this cumulative set of upstream barriers to all outgoing (downstream) edges
# -	Continues until all edges know their complete set of upstream barriers
# 4.	Traverse upstream (outlet to headwaters): 
# -	Uses a queue-based algorithm starting from outlet nodes (those with no outgoing edges)
# -	For each node, waits until all downstream edges have been visited
# -	Collects all barriers found downstream by: 
#   -	Combining barriers from all outgoing edges
#   -	Adding any barriers located at this node itself
# -	Propagates this cumulative set of downstream barriers to all incoming (upstream) edges
# -	Continues until all edges know their complete set of downstream barriers
# 5.	Write results back to database: 
# -	For each stream segment, updates columns with: 
#   -	Count of upstream barriers for this species
#   -	Count of downstream barriers for this species
#   -	Array of upstream barrier IDs
#   -	Array of downstream barrier IDs
#   -	Count of upstream gradient barriers
#   -	Count of downstream gradient barriers
# 6.	Repeat for all species: 
# -	Clears the network and rebuilds it for each fish species
# -	This is necessary because the same physical barrier might be passable for one species but impassable for another
# -	Creates separate columns for each species (for example: barrier_up_as_cnt for Atlantic Salmon)

#
import appconfig
import shapely.wkb
from collections import deque
import psycopg2.extras


iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dataSchema = appconfig.config['DATABASE']['data_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbGradientBarrierTable = appconfig.config['BARRIER_PROCESSING']['gradient_barrier_table']
dbPassabiltyTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']
species = appconfig.config[iniSection]['species']

edges = []
nodes = dict()

# with appconfig.connectdb() as conn:

#     query = f"""
#     SELECT code, name
#     FROM {dataSchema}.{appconfig.fishSpeciesTable};
#     """

#     with conn.cursor() as cursor:
#         cursor.execute(query)
#         specCodes = cursor.fetchall()

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.barrierids = set()
        self.gradientbarrierids = set()
   
    def addInEdge(self, edge):
        self.inedges.append(edge)
   
    def addOutEdge(self, edge):
        self.outedges.append(edge)
    
   
    
class Edge:
    def __init__(self, fromnode, tonode, fid, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.fid = fid
        self.visited = False
        self.upbarriers = set()
        self.downbarriers = set()
        self.upgradient = set()
        self.downgradient = set()
        
def createNetwork(connection, code): 
    # Currenty the queries take very long to run could see if this can be improved in the future
    
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, a.{appconfig.dbGeomField}
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            fid = feature[0]
            geom = shapely.wkb.loads(feature[1] , hex=True)
            
            startc = geom.coords[0]
            endc = geom.coords[len(geom.coords)-1]
            
            startt = (startc[0], startc[1])
            endt = (endc[0], endc[1])            
            
            if (startt in nodes.keys()):
                fromNode = nodes[startt]
            else:
                #create new node
                fromNode = Node(startc[0], startc[1])
                nodes[startt] = fromNode
            
            if (endt in nodes.keys()):
                toNode = nodes[endt]
            else:
                #create new node
                toNode = Node(endc[0], endc[1])
                nodes[endt] = toNode
            
            edge = Edge(fromNode, toNode, fid, geom)
            edges.append(edge)
            
            fromNode.addOutEdge(edge)
            toNode.addInEdge(edge)     
            
    #add barriers
    # query = f"""
    #     select 'up', a.id, b.id
    #     from {dbTargetSchema}.{dbBarrierTable} a, {dbTargetSchema}.{dbTargetStreamTable} b
    #     where st_dwithin(b.geometry, a.snapped_point, 0.01)
    #         and st_dwithin(st_startpoint(b.geometry), a.snapped_point, 0.01)
    #         and a.passability_status_{code} != 1
    #     union 
    #     select 'down', a.id, b.id 
    #     from {dbTargetSchema}.{dbBarrierTable} a, {dbTargetSchema}.{dbTargetStreamTable} b
    #     where st_dwithin(b.geometry, a.snapped_point, 0.01)
    #         and st_dwithin(st_endpoint(b.geometry), a.snapped_point, 0.01)
    #         and a.passability_status_{code} != 1
    # """

    #add barriers
    query = f"""
        select 'up', a.id, b.id
        from {dbTargetSchema}.{dbBarrierTable} a
        join {dbTargetSchema}.{dbPassabiltyTable} p on a.id = p.barrier_id
        join {dbTargetSchema}.fish_species f on p.species_id = f.id, 
        {dbTargetSchema}.{dbTargetStreamTable} b
        where st_dwithin(b.geometry, a.snapped_point, 0.01)
            and st_dwithin(st_startpoint(b.geometry), a.snapped_point, 0.01)
            and f.code = '{code}'
            and p.passability_status != '1'
        union 
        select 'down', a.id, b.id 
        from {dbTargetSchema}.{dbBarrierTable} a
        join {dbTargetSchema}.{dbPassabiltyTable} p on a.id = p.barrier_id
        join {dbTargetSchema}.fish_species f on p.species_id = f.id, 
        {dbTargetSchema}.{dbTargetStreamTable} b
        where st_dwithin(b.geometry, a.snapped_point, 0.01)
            and st_dwithin(st_endpoint(b.geometry), a.snapped_point, 0.01)
            and f.code = '{code}'
            and p.passability_status != '1'
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            etype = feature[0]
            bid = feature[1]
            sid = feature[2]
            
            for edge in edges:
                if (edge.fid == sid):
                    if (etype == 'up'):
                        edge.fromNode.barrierids.add(bid)
                    elif (etype == 'down'):
                        edge.toNode.barrierids.add(bid)
                        
    #add gradient barriers
    query = f"""
        select 'up', a.id, b.id 
        from {dbTargetSchema}.{dbGradientBarrierTable} a
        join {dbTargetSchema}.{dbPassabiltyTable} p on a.id = p.barrier_id
        join {dbTargetSchema}.fish_species f on p.species_id = f.id, 
        {dbTargetSchema}.{dbTargetStreamTable} b
        where st_dwithin(b.geometry, a.point, 0.01)
            and st_dwithin(st_startpoint(b.geometry), a.point, 0.01)
            and (a.type = 'gradient_barrier' or a.type = 'waterfall')
            and f.code = '{code}'
            and p.passability_status != '1'
        union 
        select 'down', a.id, b.id 
        from {dbTargetSchema}.{dbGradientBarrierTable} a
        join {dbTargetSchema}.{dbPassabiltyTable} p on a.id = p.barrier_id
        join {dbTargetSchema}.fish_species f on p.species_id = f.id, 
        {dbTargetSchema}.{dbTargetStreamTable} b
        where st_dwithin(b.geometry, a.point, 0.01)
            and st_dwithin(st_endpoint(b.geometry), a.point, 0.01)
            and (a.type = 'gradient_barrier' or a.type = 'waterfall')
            and f.code = '{code}'
            and p.passability_status != '1'
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            etype = feature[0]
            bid = feature[1]
            sid = feature[2]
            
            for edge in edges:
                if (edge.fid == sid):
                    if (etype == 'up'):
                        edge.fromNode.gradientbarrierids.add(bid)
                    elif (etype == 'down'):
                        edge.toNode.gradientbarrierids.add(bid)         

def processNodes():
    
    
    #walk down network        
    toprocess = deque()
    for edge in edges:
        edge.visited = False
        
    for node in nodes.values():
        if (len(node.inedges) == 0):
            toprocess.append(node)
            
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        
        upbarriers = set()
        upgradient = set()
         
        for inedge in node.inedges:
               
            if not inedge.visited:
                allvisited = False
                break
            else:
                upbarriers.update(inedge.upbarriers)
                upgradient.update(inedge.upgradient)
                
        if not allvisited:
            toprocess.append(node)
        else:
            upbarriers.update(node.barrierids)
            upgradient.update(node.gradientbarrierids)
        
            for outedge in node.outedges:
                outedge.upbarriers.update(upbarriers)
                outedge.upgradient.update(upgradient)
                
                outedge.visited = True
                if (not outedge.toNode in toprocess):
                    toprocess.append(outedge.toNode)
            
            
    #walk up computing mainstem id
    for edge in edges:
        edge.visited = False
        
    toprocess = deque()
    for node in nodes.values():
        if (len(node.outedges) == 0):
            toprocess.append(node)
    
    while (toprocess):
        node = toprocess.popleft()
        
        if (len(node.inedges) == 0):
            continue
        
        downbarriers = set()
        downbarriers.update(node.barrierids)

        downgradient = set()
        downgradient.update(node.gradientbarrierids)
        
        allvisited = True
        
        for outedge in node.outedges:
            if not outedge.visited:
                allvisited = False
                break
            else:
                downbarriers.update(outedge.downbarriers)
                downgradient.update(outedge.downgradient)

        if not allvisited:
            toprocess.append(node)
        else:
            for inedge in node.inedges:
                inedge.downbarriers.update(downbarriers)
                inedge.downgradient.update(downgradient)             
                inedge.visited = True
                if (not inedge.toNode in toprocess):
                    toprocess.append(inedge.fromNode)
    
        
def writeResults(connection, code):
      
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET 
            barrier_up_{code}_cnt = %s,
            barrier_down_{code}_cnt = %s,
            barriers_up_{code} = %s,
            barriers_down_{code} = %s,
            gradient_barrier_up_{code}_cnt = %s,
            gradient_barrier_down_{code}_cnt = %s
            
        WHERE id = %s;
    """
    
    newdata = []
    
    for edge in edges:
        upbarriersstr = (list(edge.upbarriers),)  
        downbarriersstr = (list(edge.downbarriers),)
        
        newdata.append( (len(edge.upbarriers), len(edge.downbarriers), upbarriersstr, downbarriersstr, len(edge.upgradient), len(edge.downgradient), edge.fid))

    
    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata)
            
    connection.commit()


#--- main program ---
def main():
    
    with appconfig.connectdb() as conn:

        conn.autocommit = False

        # query = f"""
        # SELECT code, name
        # FROM {dataSchema}.{appconfig.fishSpeciesTable};
        # """

        global specCodes
        global species

        specCodes = [substring.strip() for substring in species.split(',')]

        # with conn.cursor() as cursor:
        #     cursor.execute(query)
        #     specCodes = cursor.fetchall()

        for species in specCodes:
            code = species
            # name = species[1]
        
            

            edges.clear()
            nodes.clear()
            
            print("Computing Upstream/Downstream Barriers")
            print("  processing barriers for", code)
            print("  creating output column")

            query = f"""
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barrier_up_{code}_cnt;
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barrier_down_{code}_cnt;
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barriers_up_{code};
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barriers_down_{code};

                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS gradient_barrier_up_{code}_cnt;
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS gradient_barrier_down_{code}_cnt;
                
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barrier_up_{code}_cnt int;
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barrier_down_{code}_cnt int;
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barriers_up_{code} varchar[];
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barriers_down_{code} varchar[];

                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN gradient_barrier_up_{code}_cnt int;
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN gradient_barrier_down_{code}_cnt int;
                
            """
            
            with conn.cursor() as cursor:
                cursor.execute(query)
            
            print("  creating network")
            createNetwork(conn, code)
            
            print("  processing nodes")
            processNodes()
                
            print("  writing results")
            writeResults(conn, code)
        
    print("done")
    
if __name__ == "__main__":
    main()      