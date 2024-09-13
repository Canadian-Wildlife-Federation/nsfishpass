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
# This script creates the barrier_passability_view table
#
#

import appconfig

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = appconfig.config['BARRIER_PROCESSING']['passability_table']
species = appconfig.config[iniSection]['species']

def build_views(conn):
    # create view combining barrier and passability table
    # programmatically build columns, joins, and conditions based on species in species table

    global specCodes

    specCodes = [substring.strip() for substring in species.split(',')]

    cols = []
    joinString = ''
    conditionString = ''

    query = f""" DROP VIEW IF EXISTS {dbTargetSchema}.barrier_passability_view; """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


    for i in range(len(specCodes)):
        code = specCodes[i]
        col = f"""
        b.total_upstr_hab_{code},
        p{i}.passability_status AS passability_status_{code}
        """
        cols.append(col)
        joinString = joinString + f'JOIN {dbTargetSchema}.{dbPassabilityTable} p{i} ON b.id = p{i}.barrier_id\n'
        joinString = joinString + f'JOIN {dbTargetSchema}.fish_species f{i} ON f{i}.id = p{i}.species_id\n'
        if i == 0:
            conditionString = conditionString + f'f{i}.code = \'{code}\'\n'
        else:
            conditionString = conditionString + f'AND f{i}.code = \'{code}\'\n' 
    colString = ','.join(cols)

    query = f"""
        CREATE VIEW {dbTargetSchema}.barrier_passability_view AS 
        SELECT 
            b.id,
            b.cabd_id,
            b.modelled_id,
            b.update_id,
            b.original_point,
            b.snapped_point,
            b.name,
            b.type,
            b.owner,

            b.dam_use,

            b.fall_height_m,

            b.stream_name,
            b.strahler_order,
            b.wshed_name,
            b.secondary_wshed_name,
            b.transport_feature_name,

            b.critical_habitat,
            
            b.crossing_status,
            b.crossing_feature_type,
            b.crossing_type,
            b.crossing_subtype,
            
            b.culvert_number,
            b.structure_id,
            b.date_examined,
            b.road,
            b.culvert_type,
            b.culvert_condition,
            b.action_items, 
            b.passability_status_notes,
            {colString}
        FROM {dbTargetSchema}.{dbBarrierTable} b
        {joinString}
        WHERE {conditionString};
    """

    #print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def main():
    with appconfig.connectdb() as conn:
        conn.autocommit = False
        
        print("Building Views")
        build_views(conn)

    print("done")

if __name__ == "__main__":
    main()   