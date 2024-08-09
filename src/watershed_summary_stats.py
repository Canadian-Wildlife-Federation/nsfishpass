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
# This script summarizes watershed data creating a statistic createTable
# and populating it with various stats 
#
 

import appconfig
import sys


iniSection = appconfig.args.args[0]

wsStreamTable = appconfig.config['PROCESSING']['stream_table']
sheds = appconfig.config['HABITAT_STATS']['watershed_data_schemas'].split(",")
dbTargetSchema = appconfig.config[iniSection]['output_schema']

species = []
sec_sheds = []
statTable = 'habitat_stats'

def createTable():
    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{statTable};
        
        CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{statTable}(
            watershed_id varchar,
            sec_wshed_name varchar,
            total_km double precision,
            accessible_all_km double precision,
            potentially_accessible_all_km double precision,
            accessible_spawn_all_km double precision,
            accessible_rear_all_km double precision,
            accessible_habitat_all_km double precision,
            potentially_accessible_habitat_all_km double precision,
            total_spawn_all_km double precision,
            total_rear_all_km double precision,
            total_habitat_all_km double precision,

            primary key (watershed_id, sec_wshed_name)
        );

        ALTER TABLE  {dbTargetSchema}.{statTable} OWNER TO cwf_analyst;
    """
    with appconfig.connectdb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()


def getSecSheds(wshed):
    """
    Get secondary watersheds for a watershed if they exist
    """
    global sec_sheds
    sec_sheds = []
    q_get_sec_sheds = f"""
        SELECT DISTINCT sec_name 
        FROM {wshed}.{wsStreamTable};
    """

    with appconfig.connectdb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(q_get_sec_sheds)
            for row in cursor.fetchall():
                    sec_sheds.append(row[0])

def getSpecies(wshed):
    global species
    species = []
    q_get_species = f"""
        SELECT code
        FROM {wshed}.fish_species;
    """

    with appconfig.connectdb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(q_get_species)
            for row in cursor.fetchall():
                    species.append(row[0])

def makeAccessClause(allFishAccess, fish, access, spawn=False, rear=False, habitat=False):
    if allFishAccess is None:
        allFishAccess = ""
    else:
        allFishAccess = allFishAccess + " OR "

    allFishAccess = allFishAccess + f"({fish}_accessibility = '{access}' "

    if not habitat:
        return f"{allFishAccess})"
    elif spawn and not rear:
        return f'{allFishAccess} AND habitat_spawn_{fish} = true)'
    elif rear and not spawn:
        return f'{allFishAccess} AND habitat_rear_{fish} = true)'
    elif rear and spawn:
        return f'{allFishAccess} AND habitat_{fish} = true)'
    
def makeHabitatClause(clause, fish, spawn=False, rear=False):
    if clause is None:
        clause = ""
    else:
        clause = clause + " OR "

    if spawn and rear:
        return clause + f"habitat_{fish} = true"
    elif spawn:
        return clause + f"habitat_spawn_{fish} = true"
    elif rear:
        return clause + f"habitat_rear_{fish} = true"
    else:
        return


def runStats():
    global sec_sheds
    global species
    for shed in sheds:
        getSecSheds(shed)
        getSpecies(shed)
        q_wshed_id = f"SELECT DISTINCT watershed_id FROM {shed}.{wsStreamTable}"
        with appconfig.connectdb() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(q_wshed_id )
                    row = cursor.fetchone()
                    watershed_id = row[0]

        for sec_shed in sec_sheds:
            if sec_shed is None: continue
            q_all_stream_data = f"SELECT * FROM {shed}.{wsStreamTable} WHERE sec_name ILIKE \'{sec_shed}\'"
            query = f"""
                INSERT INTO {dbTargetSchema}.{statTable} (watershed_id, sec_wshed_name)
                VALUES ('{watershed_id}', '{sec_shed}');
            """
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
            
            col_query = ''
            fishaccess_query = ''
            allfishaccess_query = ''
            allfishpotentialaccess_query = ''
            allfishaccessspawn_query = None
            allfishaccessrear_query = None
            allfishaccesshabitat_query = ''
            fishspawnhabitat_query = ''
            fishrearhabitat_query = ''
            fishhabitat_query = ''
            connectivity_status_query = ''
            dci_query = ''
            allfishspawn_query = ''
            allfishrear_query = ''
            allfishhabitat_query = ''
            allfishpotentialaccesshabitat_query = ''

            allfishaccess = None
            allfishpotentialaccess = None
            allfishaccessspawn = None
            allfishaccessrear = None
            allfishaccesshabitat = None
            allfishspawn = None
            allfishrear = None
            allfishhabitat = None
            allfishpotentialaccesshabitat = None

            for fish in species:
                col_query = f"""
                    {col_query}
                    ALTER TABLE {dbTargetSchema}.{statTable}
                    ADD COLUMN IF NOT EXISTS {fish}_accessible_spawn_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_potentially_accessible_spawn_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_accessible_rear_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_potentially_accessible_rear_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_accessible_habitat_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_potentially_accessible_habitat_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_total_spawn_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_total_rear_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_total_habitat_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_connectivity_status double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_dci double precision;
                """

                fishaccess_query = f"""
                    {fishaccess_query}  
                    UPDATE {dbTargetSchema}.{statTable} 
                    SET
                        {fish}_accessible_spawn_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND habitat_spawn_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_potentially_accessible_spawn_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}' AND habitat_spawn_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_accessible_rear_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND habitat_rear_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_potentially_accessible_rear_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}' AND habitat_rear_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_accessible_habitat_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND habitat_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_potentially_accessible_habitat_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}' AND habitat_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                    WHERE watershed_id = '{watershed_id}'
                    AND sec_wshed_name ILIKE '{sec_shed}';
                """

                fishhabitat_query = f"""
                    {fishhabitat_query}
                    UPDATE {dbTargetSchema}.{statTable}
                    SET
                        {fish}_total_spawn_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE habitat_spawn_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_total_rear_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE habitat_rear_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_total_habitat_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE habitat_{fish} = true AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,{fish}_connectivity_status = (SELECT ({fish}_accessible_habitat_km / ({fish}_accessible_habitat_km + {fish}_potentially_accessible_habitat_km))*100 FROM {dbTargetSchema}.{statTable} WHERE sec_wshed_name ILIKE '{sec_shed}')
                        ,{fish}_dci = (SELECT coalesce(sum(dci_{fish}) FILTER (WHERE sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                    WHERE watershed_id = '{watershed_id}'
                    AND sec_wshed_name ILIKE '{sec_shed}';
                """

                allfishaccess = makeAccessClause(allfishaccess, fish, appconfig.Accessibility.ACCESSIBLE.value, habitat=False)
                allfishpotentialaccess = makeAccessClause(allfishpotentialaccess, fish, appconfig.Accessibility.POTENTIAL.value, habitat=False)
                allfishaccessspawn = makeAccessClause(allfishaccessspawn, fish, appconfig.Accessibility.ACCESSIBLE.value, spawn=True, habitat=True)
                allfishaccessrear = makeAccessClause(allfishaccessrear, fish, appconfig.Accessibility.ACCESSIBLE.value, rear=True, habitat=True)
                allfishaccesshabitat = makeAccessClause(allfishaccesshabitat, fish, appconfig.Accessibility.ACCESSIBLE.value, spawn=True, rear=True, habitat=True)
                allfishpotentialaccesshabitat = makeAccessClause(allfishpotentialaccesshabitat, fish, appconfig.Accessibility.POTENTIAL.value, spawn=True, rear=True, habitat=True)


                allfishspawn = makeHabitatClause(allfishspawn, fish, spawn=True)
                allfishrear = makeHabitatClause(allfishrear, fish, rear=True)
                allfishhabitat = makeHabitatClause(allfishhabitat, fish, spawn=True, rear=True)

                allfishaccess_query = f"""
                    UPDATE {dbTargetSchema}.{statTable}
                    SET 
                        accessible_spawn_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishaccessspawn}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,accessible_rear_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishaccessrear}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,accessible_habitat_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishaccesshabitat}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,accessible_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishaccess}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,potentially_accessible_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishpotentialaccess}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,total_spawn_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishspawn}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,total_rear_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishrear}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,total_habitat_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishhabitat}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                        ,potentially_accessible_habitat_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishpotentialaccesshabitat}) AND sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                    WHERE watershed_id = '{watershed_id}'
                    AND sec_wshed_name ILIKE '{sec_shed}';
                """

                query = f"""
                    UPDATE {dbTargetSchema}.{statTable} SET total_km =
                    (SELECT coalesce(sum(segment_length) FILTER (WHERE sec_name ILIKE '{sec_shed}'), 0) FROM {shed}.{wsStreamTable})
                    WHERE watershed_id = '{watershed_id}'
                    AND sec_wshed_name ILIKE '{sec_shed}';
                    
                    {col_query}
                    {fishaccess_query}
                    {fishhabitat_query}
                    {allfishaccess_query}
                """
                # print(query)
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    connection.commit()

            

def main():
    print('Computing Summary Statistics')
    createTable()
    runStats()
    print ("Computing Summary Statistics Complete")

if __name__ == "__main__":
    main()  