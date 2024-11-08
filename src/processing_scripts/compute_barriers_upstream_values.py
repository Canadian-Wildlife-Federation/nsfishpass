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
# This script computes barrier attributes that
# require stream network traversal (upstream length per habitat type)
#

import appconfig
import shapely.wkb
from collections import deque
import psycopg2.extras
import numpy as np

import sys

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
species_codes = appconfig.config[iniSection]['species']

edges = []
nodes = dict()
species = []

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.barrierids = set()
   
    def addInEdge(self, edge):
        self.inedges.append(edge)
   
    def addOutEdge(self, edge):
        self.outedges.append(edge)
    
class Edge:
    def __init__(self, fromnode, tonode, fid, length, strahler_order, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.length = length
        self.fid = fid
        self.visited = False
        self.speca = {} # species accessibility
        self.specaup = {} # species accessibility upstream
        self.spawn_habitat = {}
        self.spawn_habitatup = {}
        self.rear_habitat = {}
        self.rear_habitatup = {}
        self.habitat = {}
        self.habitatup = {}
        self.spawn_funchabitatup = {}
        self.rear_funchabitatup = {}
        self.funchabitatup = {}
        self.spawn_habitat_all = None
        self.spawn_habitatup_all = {}
        self.rear_habitat_all = None
        self.rear_habitatup_all = {}
        self.habitat_all = None
        self.habitatup_all = {}
        self.spawn_funchabitat_all = {}
        self.rear_funchabitat_all = {}
        self.funchabitat_all = {}
        self.spawn_funchabitatup_all = {}
        self.rear_funchabitatup_all = {}
        self.funchabitatup_all = {}

        self.upbarriercnt = {}
        self.downbarriers = {}
        self.downpassability = {}
        self.dci = {}

        # weighted length for ranking calculation
        if strahler_order == 1:
            self.w_length = self.length * 0.25
        elif strahler_order == 2:
            self.w_length = self.length * 0.75
        else:
            self.w_length = self.length

        # weighted habitat for rankings
        self.w_habitatup = {}
        self.w_funchabitatup = {}
    
    def print(self):
        print("fid:", self.fid)
        print("upbarriercnt:", self.upbarriercnt)
        print("downbarriers:", self.downbarriers)
        print("downpassability:", self.downpassability)
        print("species accessibility:", self.speca)
        print("spawn_habitat:", self.spawn_habitat)
        print("rear_habitat:", self.rear_habitat)
        print("habitat:", self.habitat)
        print("spawn_habitatup:", self.spawn_habitatup)
        print("rear_habitatup:", self.rear_habitatup)
        print("habitatup:", self.habitatup)
        print("spawn_habitat_all:", self.spawn_habitat_all)
        print("rear_habitat_all:", self.rear_habitat_all)
        print("habitat_all:", self.habitat_all)

    def check_spawn_habitat_all(self):
        result = any(val == True for val in self.spawn_habitat.values())
        return result
    
    def check_rear_habitat_all(self):
        result = any(val == True for val in self.rear_habitat.values())
        return result

    def check_habitat_all(self):
        result = any(val == True for val in self.habitat.values())
        return result

    def __iter__(self):
        return iter([self.fid, self.length, self.downbarriers, self.downpassability, self.habitat])

def createNetwork(connection):
    # Takes longest to run, could look to improve in future

    global specCodes
    global species_codes

    specCodes = [substring.strip() for substring in species_codes.split(',')]

    if len(specCodes) == 1:
        specCodes = f"('{specCodes[0]}')"
    else:
        specCodes = tuple(specCodes)
    
    query = f"""
        SELECT a.code
        FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable} a
        WHERE code IN {specCodes};
    """
    
    barrierupcntmodel = ''
    barrierdownmodel = ''
    accessibilitymodel = ''
    spawnhabitatmodel = ''
    rearhabitatmodel = ''
    habitatmodel = ''
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        for feature in features:
            species.append(feature[0])
            barrierupcntmodel = barrierupcntmodel + ', barrier_up_' + feature[0] + '_cnt'
            barrierdownmodel = barrierdownmodel + ', barriers_down_' + feature[0]
            accessibilitymodel = accessibilitymodel + ', ' + feature[0] + '_accessibility'
            spawnhabitatmodel = spawnhabitatmodel + ', habitat_spawn_' + feature[0]
            rearhabitatmodel = rearhabitatmodel + ', habitat_rear_' + feature[0]
            habitatmodel = habitatmodel + ', habitat_' + feature[0]

    
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, 
            st_length(a.{appconfig.dbGeomField}), a.{appconfig.dbGeomField}
            {barrierupcntmodel} {barrierdownmodel}
            {accessibilitymodel} {spawnhabitatmodel} {rearhabitatmodel} {habitatmodel}
            ,a.strahler_order
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        LEFT JOIN {dbTargetSchema}.{dbBarrierTable} b
        ON a.id = b.stream_id_up;
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            fid = feature[0]
            length = feature[1]
            geom = shapely.wkb.loads(feature[2] , hex=True)
            strahler_order = feature[-1]
            
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

            edge = Edge(fromNode, toNode, fid, length, strahler_order, geom)
            index = 3
            for fish in species:
                edge.upbarriercnt[fish] = feature[index]
                edge.downbarriers[fish] = feature[index + len(species)]

                passabilities = []

                for barrier in edge.downbarriers[fish]:
                    # query = f"""
                    # SELECT passability_status_{fish} FROM {dbTargetSchema}.{dbBarrierTable} WHERE id = '{barrier}';
                    # """
                    query = f"""
                    SELECT passability_status 
                    FROM {dbTargetSchema}.{dbPassabilityTable} p
                    JOIN {dbTargetSchema}.fish_species s
                        ON p.species_id = s.id
                    WHERE p.barrier_id = '{barrier}'
                    AND s.code = '{fish}'
                    """

                    with connection.cursor() as cursor2:
                        cursor2.execute(query)
                        status = cursor2.fetchone()
                        val = float(0 if status[0] is None else status[0])
                        passabilities.append(val)
                
                edge.downpassability[fish] = np.prod(passabilities)

                edge.speca[fish] = feature[index + len(species)*2]
                edge.spawn_habitat[fish] = feature[index + (len(species)*3)]
                edge.rear_habitat[fish] = feature[index + (len(species)*4)]
                edge.habitat[fish] = feature[index + (len(species)*5)]
                index = index + 1

            edge.spawn_habitat_all = edge.check_spawn_habitat_all()
            edge.rear_habitat_all = edge.check_rear_habitat_all()
            edge.habitat_all = edge.check_habitat_all()

            edges.append(edge)
            
            fromNode.addOutEdge(edge)
            toNode.addInEdge(edge)


def processNodes(connection):
    
    
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
        
        uplength = {}
        spawn_habitat = {}
        rear_habitat = {}
        habitat = {}
        spawn_funchabitat = {}
        rear_funchabitat = {}
        funchabitat = {}
        spawn_habitat_all = 0
        rear_habitat_all = 0
        habitat_all = 0
        spawn_funchabitat_all = 0
        rear_funchabitat_all = 0
        funchabitat_all = 0
        outbarriercnt = {}
        dci = {}
        total_length = {}

        # weighted
        w_habitat = {}
        w_funchabitat = {}
        
        for fish in species:
            uplength[fish] = 0
            spawn_habitat[fish] = 0
            rear_habitat[fish] = 0
            habitat[fish] = 0
            spawn_funchabitat[fish] = 0
            rear_funchabitat[fish] = 0
            funchabitat[fish] = 0
            outbarriercnt[fish] = 0
            dci[fish] = 0
            total_length[fish] = sum(edge.length for edge in edges if edge.habitat[fish])
            # weighted
            w_habitat[fish] = 0
            w_funchabitat[fish] = 0

        for inedge in node.inedges:

            for fish in species:
                outbarriercnt[fish] += inedge.upbarriercnt[fish]

                if inedge.habitat[fish]:
                    inedge.dci[fish] = ((inedge.length / total_length[fish]) * inedge.downpassability[fish]) * 100
                else:
                    inedge.dci[fish] = 0
                
            if not inedge.visited:
                allvisited = False
                break
            else:
                for fish in species:
                    uplength[fish] = uplength[fish] + inedge.specaup[fish]
                    spawn_habitat[fish] = spawn_habitat[fish] + inedge.spawn_habitatup[fish]
                    rear_habitat[fish] = rear_habitat[fish] + inedge.rear_habitatup[fish]
                    habitat[fish] = habitat[fish] + inedge.habitatup[fish]
                    spawn_funchabitat[fish] = spawn_funchabitat[fish] + inedge.spawn_funchabitatup[fish]
                    rear_funchabitat[fish] = rear_funchabitat[fish] + inedge.rear_funchabitatup[fish]
                    funchabitat[fish] = funchabitat[fish] + inedge.funchabitatup[fish]
                    # weighted habitat gain
                    w_habitat[fish] = w_habitat[fish] + inedge.w_habitatup[fish]
                    w_funchabitat[fish] = w_funchabitat[fish] + inedge.w_funchabitatup[fish] 
                
                spawn_habitat_all = spawn_habitat_all + inedge.spawn_habitatup_all
                rear_habitat_all = rear_habitat_all + inedge.rear_habitatup_all
                habitat_all = habitat_all + inedge.habitatup_all

                spawn_funchabitat_all = spawn_funchabitat_all + inedge.spawn_funchabitatup_all
                rear_funchabitat_all = rear_funchabitat_all + inedge.rear_funchabitatup_all
                funchabitat_all = funchabitat_all + inedge.funchabitatup_all
                
        if not allvisited:
            toprocess.append(node)
        else:
        
            for outedge in node.outedges:

                for fish in species:

                    if outedge.habitat[fish]:
                        outedge.dci[fish] = ((outedge.length / total_length[fish]) * outedge.downpassability[fish]) * 100
                    else:
                        outedge.dci[fish] = 0


                    if (outedge.speca[fish] == appconfig.Accessibility.ACCESSIBLE.value or outedge.speca[fish] == appconfig.Accessibility.POTENTIAL.value):
                        outedge.specaup[fish] = uplength[fish] + outedge.length
                    else:
                        outedge.specaup[fish] = uplength[fish]
                        
                    if outedge.spawn_habitat[fish]:
                        outedge.spawn_habitatup[fish] = spawn_habitat[fish] + outedge.length
                    else:
                        outedge.spawn_habitatup[fish] = spawn_habitat[fish]
                    

                    if outedge.rear_habitat[fish]:
                        outedge.rear_habitatup[fish] = rear_habitat[fish] + outedge.length
                    else:
                        outedge.rear_habitatup[fish] = rear_habitat[fish]
                    

                    if outedge.habitat[fish]:
                        outedge.habitatup[fish] = habitat[fish] + outedge.length
                    else:
                        outedge.habitatup[fish] = habitat[fish]


                    if outedge.upbarriercnt[fish] != outbarriercnt[fish]:
                        if outedge.spawn_habitat[fish]:
                            outedge.spawn_funchabitatup[fish] = outedge.length
                        else:
                            outedge.spawn_funchabitatup[fish] = 0 
                    elif outedge.spawn_habitat[fish]:
                        outedge.spawn_funchabitatup[fish] = spawn_funchabitat[fish] + outedge.length
                    else:
                        outedge.spawn_funchabitatup[fish] = spawn_funchabitat[fish]


                    if outedge.upbarriercnt[fish] != outbarriercnt[fish]:
                        if outedge.rear_habitat[fish]:
                            outedge.rear_funchabitatup[fish] = outedge.length
                        else:
                            outedge.rear_funchabitatup[fish] = 0 
                    elif outedge.rear_habitat[fish]:
                        outedge.rear_funchabitatup[fish] = rear_funchabitat[fish] + outedge.length
                    else:
                        outedge.rear_funchabitatup[fish] = rear_funchabitat[fish]


                    if outedge.upbarriercnt[fish] != outbarriercnt[fish]:
                        if outedge.habitat[fish]:
                            outedge.funchabitatup[fish] = outedge.length
                        else:
                            outedge.funchabitatup[fish] = 0
                    elif outedge.habitat[fish]:
                        outedge.funchabitatup[fish] = funchabitat[fish] + outedge.length
                    else: 
                        outedge.funchabitatup[fish] = funchabitat[fish]

                    # weighted habitat for ranking
                    if outedge.habitat[fish]:
                        outedge.w_habitatup[fish] = w_habitat[fish] + outedge.w_length
                    else:
                        outedge.w_habitatup[fish] = w_habitat[fish]

                    if outedge.upbarriercnt[fish] != outbarriercnt[fish]:
                        if outedge.habitat[fish]:
                            outedge.w_funchabitatup[fish] = outedge.w_length
                        else:
                            outedge.w_funchabitatup[fish] = 0
                    elif outedge.habitat[fish]:
                        outedge.w_funchabitatup[fish] = w_funchabitat[fish] + outedge.w_length
                    else: 
                        outedge.w_funchabitatup[fish] = w_funchabitat[fish]
                
                if outedge.spawn_habitat_all:
                    outedge.spawn_habitatup_all = spawn_habitat_all + outedge.length
                else:
                    outedge.spawn_habitatup_all = spawn_habitat_all

                if outedge.rear_habitat_all:
                    outedge.rear_habitatup_all = rear_habitat_all + outedge.length
                else:
                    outedge.rear_habitatup_all = rear_habitat_all

                if outedge.habitat_all:
                    outedge.habitatup_all = habitat_all + outedge.length
                else:
                    outedge.habitatup_all = habitat_all
                
                if outedge.upbarriercnt != outbarriercnt:
                    if outedge.spawn_habitat_all:
                        outedge.spawn_funchabitatup_all = outedge.length
                    else:
                        outedge.spawn_funchabitatup_all = 0
                elif outedge.spawn_habitat_all:
                    outedge.spawn_funchabitatup_all = spawn_funchabitat_all + outedge.length
                else: 
                    outedge.spawn_funchabitatup_all = spawn_funchabitat_all


                if outedge.upbarriercnt != outbarriercnt:
                    if outedge.rear_habitat_all:
                        outedge.rear_funchabitatup_all = outedge.length
                    else:
                        outedge.rear_funchabitatup_all = 0
                elif outedge.rear_habitat_all:
                    outedge.rear_funchabitatup_all = rear_funchabitat_all + outedge.length
                else: 
                    outedge.rear_funchabitatup_all = rear_funchabitat_all


                if outedge.upbarriercnt != outbarriercnt:
                    if outedge.habitat_all:
                        outedge.funchabitatup_all = outedge.length
                    else:
                        outedge.funchabitatup_all = 0
                elif outedge.habitat_all:
                    outedge.funchabitatup_all = funchabitat_all + outedge.length
                else: 
                    outedge.funchabitatup_all = funchabitat_all


                outedge.visited = True
                if (not outedge.toNode in toprocess):
                    toprocess.append(outedge.toNode)
        
def writeResults(connection):
      
    tablestr = ''
    inserttablestr = ''
    for fish in species:
        tablestr = tablestr + ', total_upstr_pot_access_' + fish + ' double precision'
        tablestr = tablestr + ', total_upstr_hab_spawn_' + fish + ' double precision'
        tablestr = tablestr + ', total_upstr_hab_rear_' + fish + ' double precision'
        tablestr = tablestr + ', total_upstr_hab_' + fish + ' double precision'
        tablestr = tablestr + ', func_upstr_hab_spawn_' + fish + ' double precision'
        tablestr = tablestr + ', func_upstr_hab_rear_' + fish + ' double precision'
        tablestr = tablestr + ', func_upstr_hab_' + fish + ' double precision'
        tablestr = tablestr + ', dci_' + fish + ' double precision'
        tablestr = tablestr + ', w_total_upstr_hab_' + fish + ' double precision' # weighted habitat
        tablestr = tablestr + ', w_func_upstr_hab_' + fish + ' double precision' # weighted habitat
        inserttablestr = inserttablestr + ",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"

    tablestr = tablestr + ', total_upstr_hab_spawn_all' + ' double precision'
    tablestr = tablestr + ', total_upstr_hab_rear_all' + ' double precision'
    tablestr = tablestr + ', total_upstr_hab_all' + ' double precision'
    tablestr = tablestr + ', func_upstr_hab_spawn_all' + ' double precision'
    tablestr = tablestr + ', func_upstr_hab_rear_all' + ' double precision'
    tablestr = tablestr + ', func_upstr_hab_all' + ' double precision'
    inserttablestr = inserttablestr + ",%s,%s,%s,%s,%s,%s"

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.temp;
        
        CREATE TABLE {dbTargetSchema}.temp (
            stream_id uuid
            {tablestr}
        );

        ALTER TABLE {dbTargetSchema}.temp OWNER TO cwf_analyst;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    
    
    updatequery = f"""    
        INSERT INTO {dbTargetSchema}.temp VALUES (%s {inserttablestr}) 
    """

    newdata = []
    
    for edge in edges:
        
        data = []
        data.append(edge.fid)
        for fish in species:
            data.append (edge.specaup[fish])
            data.append (edge.spawn_habitatup[fish])
            data.append (edge.rear_habitatup[fish])
            data.append (edge.habitatup[fish])
            data.append (edge.spawn_funchabitatup[fish])
            data.append (edge.rear_funchabitatup[fish])
            data.append (edge.funchabitatup[fish])
            data.append (edge.dci[fish])
            data.append(edge.w_habitatup[fish]) # weighted habitat
            data.append(edge.w_funchabitatup[fish]) # weighted habitat
        
        data.append(edge.spawn_habitatup_all)
        data.append(edge.rear_habitatup_all)
        data.append(edge.habitatup_all)
        data.append(edge.spawn_funchabitatup_all)
        data.append(edge.rear_funchabitatup_all)
        data.append(edge.funchabitatup_all)

        newdata.append( data )

    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata)
            
    for fish in species:
        
        query = f"""
            --upstream potentially accessible
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_pot_access_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_pot_access_{fish} double precision;
            
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_pot_access_{fish} = a.total_upstr_pot_access_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a, {dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND
                  a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

            --total upstream habitat
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_spawn_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_spawn_{fish} double precision;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_spawn_{fish} = a.total_upstr_hab_spawn_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_rear_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_rear_{fish} double precision;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_rear_{fish} = a.total_upstr_hab_rear_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_{fish} double precision;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_{fish} = a.total_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

            --- WEIGHTED TOTAL HABITAT -----
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS w_total_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN w_total_upstr_hab_{fish} double precision;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET w_total_upstr_hab_{fish} = a.w_total_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            
            --functional upstream habitat
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_spawn_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_spawn_{fish} double precision;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_spawn_{fish} = a.func_upstr_hab_spawn_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_rear_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_rear_{fish} double precision;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_rear_{fish} = a.func_upstr_hab_rear_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_{fish} double precision;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_{fish} = a.func_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

            --- WEIGHTED FUNC HABITAT ---
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS w_func_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN w_func_upstr_hab_{fish} double precision;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET w_func_upstr_hab_{fish} = a.w_func_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS dci_{fish};
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN dci_{fish} double precision;
            
            UPDATE {dbTargetSchema}.{dbTargetStreamTable}
            SET dci_{fish} = a.dci_{fish}
            FROM {dbTargetSchema}.temp a
            WHERE a.stream_id = id;

        """
        with connection.cursor() as cursor:
            cursor.execute(query)
    
    query = f"""
        --total upstream habitat - all
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_spawn_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_spawn_all double precision;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET total_upstr_hab_spawn_all = a.total_upstr_hab_spawn_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_rear_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_rear_all double precision;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET total_upstr_hab_rear_all = a.total_upstr_hab_rear_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_all double precision;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET total_upstr_hab_all = a.total_upstr_hab_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;


        --functional upstream habitat - all
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_spawn_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_spawn_all double precision;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET func_upstr_hab_spawn_all = a.func_upstr_hab_spawn_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_rear_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_rear_all double precision;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET func_upstr_hab_rear_all = a.func_upstr_hab_rear_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
        
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_all double precision;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET func_upstr_hab_all = a.func_upstr_hab_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)

    query = f"""
        DROP TABLE {dbTargetSchema}.temp;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)        

    connection.commit()


def assignBarrierCounts(connection):

    global specCodes

    specCodes = [substring.strip() for substring in species_codes.split(',')]

    if len(specCodes) == 1:
        specCodes = f"('{specCodes[0]}')"
    else:
        specCodes = tuple(specCodes)

    query = f"""
        SELECT a.code
        FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable} a
        WHERE code IN {specCodes};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        species = cursor.fetchall()

    for code in species:
        fish = code[0]

        query = f"""
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS barrier_cnt_upstr_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS barriers_upstr_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS gradient_barrier_cnt_upstr_{fish};

            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS barrier_cnt_downstr_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS barriers_downstr_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS gradient_barrier_cnt_downstr_{fish};

            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS barrier_cnt_upstr_{fish} integer;
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS barriers_upstr_{fish} varchar[];
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS gradient_barrier_cnt_upstr_{fish} integer;

            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS barrier_cnt_downstr_{fish} integer;
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS barriers_downstr_{fish} varchar[];
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS gradient_barrier_cnt_downstr_{fish} integer;

            UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET 
                barrier_cnt_upstr_{fish} = a.barrier_up_{fish}_cnt,
                barriers_upstr_{fish} = a.barriers_up_{fish},
                gradient_barrier_cnt_upstr_{fish} = a.gradient_barrier_up_{fish}_cnt
            FROM {dbTargetSchema}.{dbTargetStreamTable} a
            WHERE a.id =  {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET
                barrier_cnt_downstr_{fish} = a.barrier_down_{fish}_cnt,
                barriers_downstr_{fish} = a.barriers_down_{fish},
                gradient_barrier_cnt_downstr_{fish} = a.gradient_barrier_down_{fish}_cnt
            FROM {dbTargetSchema}.{dbTargetStreamTable} a
            WHERE a.id =  {dbTargetSchema}.{dbBarrierTable}.stream_id_down;
            
        """
        with connection.cursor() as cursor:
            cursor.execute(query)             

    connection.commit()
    
    
#--- main program ---
def main():

    edges.clear()
    nodes.clear()
    species.clear()    
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Habitat Models for Barriers")
        
        print("  assigning barrier counts")
        assignBarrierCounts(conn)
        
        print("  creating network")
        createNetwork(conn)
        
        print("  processing nodes")
        processNodes(conn)
            
        print("  writing results")
        writeResults(conn)
        
    print("done")
    

if __name__ == "__main__":
    main()      
