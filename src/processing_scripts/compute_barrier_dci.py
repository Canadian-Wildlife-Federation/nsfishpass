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

###
# This script computes the dci value for each barrier.
# 
# The Dendritic Connectivity Index (DCI) value for the barrier for the species. 
# This is a measure of the effect this barrier would have on connectivity for that species, if it were made fully passable or removed.
#
# This is calculated as the difference between the current watershed DCI and the watershed DCI if the barrier was removed or made fully passable
#
# A high DCI value for a barrier indicates a greater improvement to connectivity than a low DCI value.
#
# See: https://www.notion.so/cwf-spatial/Connectivity-Stats-14641376668e80f1bd72f1f701888a5f?source=copy_link#14641376668e80588a17fd2b333a7dcb
#
###


import appconfig
import psycopg2.extras
import numpy as np
import uuid

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
specCodes = appconfig.config[iniSection]['species']

class StreamData:
    def __init__(self, fid, length, downbarriers, habitat):
        self.fid = fid
        self.length = length
        self.downbarriers = downbarriers
        self.habitat = habitat
        self.downpassability = {}
        self.dci = {}
    
    def print(self):
        print("fid:", self.fid)
        print("downbarriers:", self.downbarriers)
        print("downpassability:", self.downpassability)
        print("habitat:", self.habitat)

class BarrierData:
    def __init__(self, bid, passabilitystatus):
        self.bid = bid
        self.passabilitystatus = passabilitystatus
        self.dci = {}
    
    def print(self):
        print("bid:", self.bid)
        print("passability status:", self.passabilitystatus)
        print("dci:", self.dci)

def getSpeciesConnectivity(conn, species):
    """
    Generates a dictionary of per-species dci for entire stream network
    
    :param conn: db conneciton
    :param species: list of species
    :return: dictionary of dci for each species over the entire stream network
    """

    dci_base = {}

    for fish in species:

        query = f"""
            SELECT SUM(dci_{fish}) FROM {dbTargetSchema}.{dbTargetStreamTable};
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            d = cursor.fetchone()
            dcib = float(d[0])
            dci_base[fish] = dcib

    return dci_base

def getBarrierDCI(barrier, barrierData, streamData, species, speciesDCI, totalHabitat):
    """
    Docstring for getBarrierDCI
    
    :param barrier: one barrier in the barrierData list
    :param barrierData: List of BarrierData objects
    :param streamData: List of StreamData objects
    :param species: List of species codes
    :param speciesDCI: Dict of dci per species for the entire stream network
    :param totalHabitat: Dict of length of habitat per species over entire stream network
    :return: dci for barrier
    """

    newStreamArray = []
    dci_sum = {}

    # TODO: This loop is not a significant bottleneck, nor is the DCI used that much by the biologists
    # However, this could probably be improved using dynamic programming. 
    # The DCI values for every stream segment in the entire network should not need to be recalculated every time we want to get the dci for one barrier
    # Rather, we should keep a table of each stream segment's dci and populate it initially with the dci of the network
    # Then, when we want to find the dci for a barrier, we should find only the stream segments with that barrier id as a downbarrier and only recalculate those
    # Then sum the dci's for the stream network using a combination of the previously calculated dcis and the new dcis
    for stream in streamData:
        streamDCI = {}
        downbarriers = stream.downbarriers
        downpassability = stream.downpassability
        newStreamData = StreamData(stream.fid, stream.length, downbarriers, stream.habitat)         # This will store the stream info if barrier were removed

        for fish in species:
            downbarriers[fish] = stream.downbarriers[fish]
            passabilities = []
            
            for b in downbarriers[fish]:                # passability values for downstream barriers aside from current barrier
                if str(b) == str(barrier.bid):
                    pass
                else:
                    passabilities.append(barrierData[uuid.UUID(b)].passabilitystatus[fish])
            
            downpassability[fish] = np.prod(passabilities)
            newStreamData.downpassability[fish] = downpassability[fish]

            if newStreamData.habitat[fish]:
                streamDCI[fish] = ((newStreamData.length / totalHabitat[fish]) * newStreamData.downpassability[fish]) * 100  # DCI calculation for stream segment
            else:
                streamDCI[fish] = 0
            
            newStreamData.dci[fish] = streamDCI[fish]
            
        newStreamArray.append(newStreamData) # list of dci for each stream segment 
    
    for fish in species:
        dci_sum[fish] = sum(newStream.dci[fish] for newStream in newStreamArray)
        barrier.dci[fish] = round((dci_sum[fish] - speciesDCI[fish]),4)

    return barrier.dci

def getHabitatLength(conn, species):

    totalLength = {}

    for fish in species:
        query = f"""
            SELECT SUM(segment_length) FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE habitat_{fish} = true;
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            length = cursor.fetchone()
            totalLength[fish] = length[0]
    
    return totalLength

def generateStreamData(conn, species):
    """
    Generates an array of StreamData objects over entire stream network. Each StreamData object is a stream segment with the following attributes:
    fid             - id of stream segment
    length          - length of stream segment
    downbarriers    - a dictionary containing arrays of downstream barrier ids per species from that stream segment
    habitat         - dict containing each species and a boolean indicating whether the stream segment is habitat for that species
    
    :param conn: db conenciton
    :param species: array of species
    :return: Array of StreamData objects
    """

    streamArray = []

    barrierdownmodel = ''
    habitatmodel = ''

    for fish in species:
        barrierdownmodel = barrierdownmodel + ', barriers_down_' + fish
        habitatmodel = habitatmodel + ', habitat_' + fish

    query = f"""
    SELECT a.{appconfig.dbIdField} as id,
        segment_length
        {barrierdownmodel}
        {habitatmodel}
    FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        allstreamdata = cursor.fetchall()
        
        for stream in allstreamdata:

            fid = stream[0]
            length = stream[1]
            downbarriers = {}
            habitat = {}

            index = 2

            for fish in species:
                downbarriers[fish] = stream[index]
                habitat[fish] = stream[index + len(species)]
                index = index + 1
            
            streamArray.append(StreamData(fid, length, downbarriers, habitat))

    return streamArray

def generateBarrierData(conn, species):
    """
    Generates a list of BarrierData objects. Each BarrierData object is a barrier with the following attributes:
    bid                 - id of the barrier
    passabilitystatus   - dictionary of passability for each species
    dci                 - dendritic connectivity index (not assigned in this function)
    
    :param conn: db connection
    :param species: list of species
    :return: List of BarrierData objects with id and passabilitystatus defined
    """

    barrierDict = {}

    passabilitymodel = ''

    for fish in species:
        passabilitymodel = passabilitymodel + ', MAX(CASE WHEN code = \''+fish+'\' THEN passability_status ELSE NULL END) AS passability_'+fish

    # query = f"""
    # SELECT id {passabilitymodel} FROM {dbTargetSchema}.{dbBarrierTable};
    # """
    query = f"""
    WITH pass AS (
        SELECT b.id, p.passability_status, f.code
        FROM {dbTargetSchema}.{dbBarrierTable} b
        LEFT OUTER JOIN {dbTargetSchema}.{dbPassabilityTable} p
            ON b.id = p.barrier_id
        JOIN {dbTargetSchema}.fish_species f
            ON p.species_id = f.id
        ORDER BY b.id, f.code
    )
    SELECT id {passabilitymodel}
    FROM pass
    GROUP BY id;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        allbarrierdata = cursor.fetchall()


        # bid = ''
        # for i in range(len(allbarrierdata)):
        #     barrier = allbarrierdata[i]
        #     passabilitystatus = {}

        #     # If we have reached the next barrier in the passability table
        #     # then add the previous barrier to barrierDict and update the 
        #     # bid to build the next entry for the next rows
        #     if bid != barrier[0]:
        #         if bid != '':
        #             # check for species not in barrier table
        #             # in which case, the passability is 0
        #             for fish in species:
        #                 if fish not in passabilitystatus:
        #                     passabilitystatus[fish] = float(0)
        #             barrierDict[bid] = BarrierData(bid, passabilitystatus)
        #         bid = barrier[0]
        #         print(bid)

        #     fish = barrier[2]

        #     passabilitystatus[fish] = float(0 if barrier[1] is None else barrier[1])

        # # Don't forget to add the last barrier
        # for fish in species:
        #     if fish not in passabilitystatus:
        #         passabilitystatus[fish] = float(0)
        # barrierDict[bid] = BarrierData(bid, passabilitystatus)

        
        for barrier in allbarrierdata:

            bid = barrier[0]
            passabilitystatus = {}

            index = 1

            for fish in species:
                passabilitystatus[fish] = float(0 if barrier[index] is None else barrier[index])
                index = index + 1
            
            barrierDict[bid] = BarrierData(bid, passabilitystatus)

    return barrierDict

def writeResults(conn, newAllBarrierData, species):
    
    tablestr = ''
    inserttablestr = ''

    for fish in species:
        tablestr = tablestr + ', dci_' + fish + ' double precision'
        inserttablestr = inserttablestr + ",%s"

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.temp;
        
        CREATE TABLE {dbTargetSchema}.temp (
            barrier_id uuid
            {tablestr}
        );

        ALTER TABLE  {dbTargetSchema}.temp OWNER TO cwf_analyst;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    
    updatequery = f"""    
        INSERT INTO {dbTargetSchema}.temp VALUES (%s {inserttablestr}) 
    """

    newdata = []
    
    for record in newAllBarrierData:
        data = []
        data.append(record.bid)
        for fish in species:
            data.append(record.dci[fish])

        newdata.append(data)

    with conn.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata)

    for fish in species:
        
        query = f"""
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS dci_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN dci_{fish} double precision;
            
            UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET dci_{fish} = a.dci_{fish}
            FROM {dbTargetSchema}.temp a
            WHERE a.barrier_id = id;

        """
        with conn.cursor() as cursor:
            cursor.execute(query)

    conn.commit()

def main():

    print("Started!")
    with appconfig.connectdb() as conn:
        conn.autocommit = False

        global specCodes

        specCodes = [substring.strip() for substring in specCodes.split(',')]

        if len(specCodes) == 1:
            specCodes = f"('{specCodes[0]}')"
        else:
            specCodes = tuple(specCodes)

        species = []

        query = f"""
            SELECT a.code
            FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable} a
            WHERE code IN {specCodes};
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            spec = cursor.fetchall()
            for s in spec:
                species.append(s[0])

        print("species list: ", species)
        
        speciesDCI = getSpeciesConnectivity(conn, species)

        totalHabitat = getHabitatLength(conn, species)

        streamData = generateStreamData(conn, species)

        barrierData = generateBarrierData(conn, species)

        newAllBarrierData = []

        for barrierid in barrierData:
            print("barrier id:", barrierid, "object:", barrierData[barrierid])
            dci = getBarrierDCI(barrierData[barrierid], barrierData, streamData, species, speciesDCI, totalHabitat)
            newBarrierData = BarrierData(barrierData[barrierid].bid, barrierData[barrierid].passabilitystatus)
            newBarrierData.dci = dci
            newAllBarrierData.append(newBarrierData)

        writeResults(conn, newAllBarrierData, species)

        print("Done!")

if __name__ == "__main__":
    main()      