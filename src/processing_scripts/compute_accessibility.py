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
# For each segment computes the maximum downstream gradient then uses
# this and the barrier information to compute species accessibility
# for each fish species
#


import appconfig
from appconfig import dataSchema

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
updateTable = dbTargetSchema + ".habitat_access_updates"
species = appconfig.config[iniSection]['species']
    
def computeAccessibility(connection):
        
    query = f"""
        SELECT code, name
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()

        global species

        features = [substring.strip() for substring in species.split(',')]
        
        for feature in features:
            code = feature
            # name = feature[1]

            print("  processing " + feature)

            if code == 'as':
                # initial accessibility calculation
                query = f"""
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {code}_accessibility;
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN {code}_accessibility varchar;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                    SET {code}_accessibility = 
                    CASE 
                    WHEN (gradient_barrier_down_{code}_cnt = 0 and barrier_down_{code}_cnt = 0) THEN '{appconfig.Accessibility.ACCESSIBLE.value}'
                    WHEN (gradient_barrier_down_{code}_cnt = 0 and barrier_down_{code}_cnt > 0) THEN '{appconfig.Accessibility.POTENTIAL.value}'
                    ELSE '{appconfig.Accessibility.NOT.value}' END;
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
            elif code == 'ae':
                query = f"""
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {code}_accessibility;
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN {code}_accessibility varchar;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                    SET {code}_accessibility = 
                    CASE 
                    --WHEN strahler_order = 1 THEN '{appconfig.Accessibility.NOT.value}'
                    WHEN (gradient_barrier_down_{code}_cnt = 0 and barrier_down_{code}_cnt = 0) THEN '{appconfig.Accessibility.ACCESSIBLE.value}'
                    WHEN (gradient_barrier_down_{code}_cnt = 0 and barrier_down_{code}_cnt > 0) THEN '{appconfig.Accessibility.POTENTIAL.value}'
                    ELSE '{appconfig.Accessibility.NOT.value}' END;
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)

                
            # # process any updates to accessibility
            # query = f"""
            # UPDATE {dbTargetSchema}.{dbTargetStreamTable} a
            #     SET {code}_accessibility = 
            #     CASE
            #     WHEN b.{code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND barrier_down_{code}_cnt = 0 THEN '{appconfig.Accessibility.ACCESSIBLE.value}'
            #     WHEN b.{code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND barrier_down_{code}_cnt > 0 THEN '{appconfig.Accessibility.POTENTIAL.value}'
            #     WHEN b.{code}_accessibility = '{appconfig.Accessibility.NOT.value}' THEN '{appconfig.Accessibility.NOT.value}'
            #     ELSE a.{code}_accessibility END
            #     FROM {updateTable} b
            #     WHERE b.stream_id = a.id AND b.{code}_accessibility IS NOT NULL AND b.update_type = 'access';
            # """
            # with connection.cursor() as cursor2:
            #     cursor2.execute(query)
            
            # connection.commit()

def main():        
    #--- main program ---
            
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Gradient Accessibility Per Species")
        computeAccessibility(conn)
        
    print("done")

    
if __name__ == "__main__":
    main() 
