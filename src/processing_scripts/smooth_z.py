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
# Smooths raw elevation values to ensure hydro network flows downhill
# 
# DESCRIPTION
# 
#This Python script smooths elevation values along a stream network to ensure water flows consistently downhill, correcting any anomalies in the raw elevation data. The script will update the streams table with a single gradient value for each complete stream segment. The output of the script is a new geometry column containing smoothed 3D stream segments where elevations decrease consistently from upstream to downstream, eliminating any artificial "uphill" sections that would be physically impossible. There are four main steps to how this script operates.
# 1.	Build a network structure: 
# -	Reads all stream segments from the database
# -	Creates a graph structure with nodes (junction points where streams meet) and edges (stream segments connecting nodes)
# -	Each node stores its x, y, and z coordinates
# -	Each edge stores its geometry and connects two nodes
# 2.	Process nodes in two passes: 
# -	Upstream pass (walking up the network): 
#   -	Starts at outlet points (nodes with no downstream connections)
#   -	Works upstream, visiting each node
#   -	Calculates a "maximum elevation" value for each node by looking at all downstream elevations
#   -	Ensures no node is higher than anything downstream from it
# -	Downstream pass (walking down the network): 
#   -	Starts at headwater points (nodes with no upstream connections)
#   -	Works downstream, visiting each node
#   -	Calculates a "minimum elevation" value for each node by looking at all upstream elevations
#   -	Ensures no node is lower than anything upstream from it
# -	Final node elevation: 
#   -	Averages the min and max values to get a smoothed elevation for each node
#   -	This balances upstream and downstream constraints
# 3.	Process edges (stream segments): 
# -	For each stream segment, smooths the elevation profile between its start and end nodes
# -	Uses a bidirectional approach: 
#   -	From downstream: ensures each point isn't lower than points downstream
#   -	From upstream: ensures each point isn't higher than points upstream
# -	Averages these constraints to create a smooth, monotonically decreasing elevation profile along each stream
# 4.	Write results back to database: 
# -	Updates the database with new smoothed geometries
# -	Creates spatial indexes for efficient querying

import appconfig
import shapely.wkb
import shapely.geometry
import psycopg2.extras
from collections import deque

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetTable = appconfig.config['PROCESSING']['stream_table']

dbSourceGeom = appconfig.config['ELEVATION_PROCESSING']['3dgeometry_field']
dbTargetGeom = appconfig.config['ELEVATION_PROCESSING']['smoothedgeometry_field']
    
edges = []
nodes = dict()

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.z = appconfig.NODATA
        self.minvalue = self.z
        self.maxvalue = self.z
        

    def addInEdge(self, edge):
        self.inedges.append(edge)
        self.addZ(edge.ls.coords[len(edge.ls.coords) - 1][2])

    def addOutEdge(self, edge):
        self.outedges.append(edge)
        self.addZ(edge.ls.coords[0][2])
    
    def addZ(self, z):
        if (self.z == appconfig.NODATA or self.z == z):
            self.z = z
        else:
            print("DIFFERENT Z VALUES AT SAME POSITION: POINT(" + str(self.x) + " " + str(self.y) + "): " +str(self.x) + " " +str(z))
    
class Edge:
    def __init__(self, fromnode, tonode, fid, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.fid = fid
        self.visited = False
        self.newz = [appconfig.NODATA for i in range(len(ls.coords))]
        
def createNetwork(connection):
    query = f"""
        SELECT {appconfig.dbIdField}, {dbSourceGeom}
        FROM {dbTargetSchema}.{dbTargetTable}
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            geom = shapely.wkb.loads(feature[1] , hex=True)
            fid = feature[0]
            
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

def processNodes():
    
    #walk up network
    for edge in edges:
        edge.visited = False
        
    toprocess = deque()
    for node in nodes.values():
        if (len(node.outedges) == 0):
            toprocess.append(node)
            node.maxvalue = node.z
    
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        for outedge in node.outedges:
            if not outedge.visited:
                allvisited = False;
                break;
        if not allvisited:
            toprocess.append(node)
        else:
            #visit this node
            for inedge in node.inedges:
                inedge.fromNode.maxvalue = max(node.maxvalue, inedge.fromNode.z)
                inedge.visited = True
                toprocess.append(inedge.fromNode)
    
    #walk down network        
    toprocess = deque()
    for edge in edges:
        edge.visited = False
        
    for node in nodes.values():
        node.minvalue = node.z
        if (len(node.inedges) == 0):
            toprocess.append(node)
     
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        for inedge in node.inedges:
            if not inedge.visited:
                allvisited = False;
                break;
        if not allvisited:
            toprocess.append(node)
        else:
            #visit this node
            for outedge in node.outedges:
                if (node.minvalue == appconfig.NODATA):
                    outedge.toNode.minvalue = outedge.toNode.minvalue 
                elif (outedge.toNode.minvalue == appconfig.NODATA):
                    outedge.toNode.minvalue = node.minvalue
                else: 
                    outedge.toNode.minvalue = min(node.minvalue, outedge.toNode.minvalue)
                                   
                outedge.visited = True
                
                if (outedge.toNode in toprocess):
                    toprocess.remove(outedge.toNode)
                toprocess.append(outedge.toNode)     
    
    #update z values 
    for node in nodes.values():
        if (node.maxvalue == appconfig.NODATA or node.minvalue == appconfig.NODATA):
            node.z = appconfig.NODATA
        else:
            node.z = (node.maxvalue + node.minvalue) / 2.0
        
    for edge in edges:
        edge.newz[0] = edge.fromNode.z
        edge.newz[len(edge.ls.coords)-1] = edge.toNode.z


def processEdges():   
    
    for edge in edges:             
        ls = edge.ls
        
        size = len(ls.coords)
        
        minvalues = [appconfig.NODATA] * size
        maxvalues = [appconfig.NODATA] * size
        
        absmax = edge.newz[0]
        absmin = edge.newz[size - 1]
        
        minvalues [0] = edge.newz[0]
        maxvalues [size - 1] = edge.newz[size - 1]
        
        
        for i in range(1, size):
            temp = ls.coords[i][2]
            if (temp < absmin):
                temp = absmin
            minv = min(temp, minvalues[i-1])
            minvalues[i] = minv
            
            temp = ls.coords[size - 1 - i][2]
            if (temp > absmax):
                temp = absmax
            maxv = max(temp, maxvalues[size - i])
            maxvalues [size - 1 - i ] = maxv
        
        for i in range(0, size):
            if minvalues[i] == appconfig.NODATA or maxvalues[i] == appconfig.NODATA:
                edge.newz[i] = appconfig.NODATA
            else:
                edge.newz[i] = ((minvalues[i] + maxvalues[i]) / 2.0)
        
        
def writeResults(connection):
    
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetTable} 
        set {dbTargetGeom} = st_setsrid(st_geomfromwkb(%s),{appconfig.dataSrid})
        WHERE  {appconfig.dbIdField} = %s
    """
    
    newdata = []
    
    for edge in edges:
        newpnts = [];
        for i in range(0, len(edge.ls.coords)):
            x = edge.ls.coords[i][0]
            y = edge.ls.coords[i][1]
            z = edge.newz[i]
            newpnts.append((x,y,z))
        ls = shapely.geometry.LineString(newpnts)
        newdata.append( (shapely.wkb.dumps(ls), edge.fid))
    
    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata);
            
    connection.commit()
    

#--- main program ---    
def main():
    
    edges.clear()
    nodes.clear()

    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Smoothing Elevation Values")
        print("  creating output column")
        #add a new geometry column for output removing existing one
        query = f"""
            ALTER TABLE {dbTargetSchema}.{dbTargetTable} drop column if exists {dbTargetGeom};        
            ALTER TABLE {dbTargetSchema}.{dbTargetTable} add column {dbTargetGeom} geometry(linestringz, {appconfig.dataSrid});
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        
        print("  creating network")
        createNetwork(conn)
        
        print("  processing nodes")
        processNodes()
        
        print("  processing edges")
        processEdges()
        
        print("  writing results")
        writeResults(conn)
        # replace index on geometry field
        query = f"""
            DROP INDEX IF EXISTS {dbTargetSchema}.{dbTargetSchema}_{dbTargetTable}_geometry_idx;
            CREATE INDEX IF NOT EXISTS {dbTargetSchema}_{dbTargetTable}_geometry_idx
                ON {dbTargetSchema}.{dbTargetTable} USING gist
                ({dbTargetGeom});
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        
    print("done")


if __name__ == "__main__":
    main()      
#drop table working.points3d; 
#
#create table working.points3d as 
#select geom, st_z(geom) as z
#from (
#  SELECT (ST_DumpPoints(geometry_raw3d)).geom  AS geom
#  FROM working.stream3d
#) foo
    