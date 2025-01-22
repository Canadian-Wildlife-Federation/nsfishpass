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
# This script creates the fish_species table containing accessibility and 
# habitat parameters for species of interest from a CSV specified by the user
#
# TO DO: fix problem with this script where it's the only one that doesn't
# rely on the watershed being specified - this seems to be a problem

import appconfig
import subprocess

# dataFile = appconfig.config['DATABASE']['fish_parameters']
sourceTable = appconfig.dataSchema + ".fish_species_raw"
# fishSpeciesTable = appconfig.config['DATABASE']['fish_species_table']

dataFile = appconfig.fish_parameters
fishSpeciesTable = appconfig.fishSpeciesTable

specCodes = appconfig.getSpecies()

def main():
    with appconfig.connectdb() as conn:

        query = f"""
            DROP TABLE IF EXISTS {sourceTable};
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # load data using ogr
        orgDb = "dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost +"' port='"+ appconfig.dbPort + "' user='" + appconfig.dbUser + "' password='" + appconfig.dbPassword + "'"
        pycmd = '"' + appconfig.ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" "' + dataFile + '"' + ' -nln "' + sourceTable + '" -oo AUTODETECT_TYPE=YES -oo EMPTY_STRING_AS_NULL=YES'
        print(pycmd)
        subprocess.run(pycmd)
        print("CSV loaded to table: " + sourceTable)

        query = f"""
            DROP TABLE IF EXISTS {appconfig.dataSchema}.{fishSpeciesTable};

            CREATE TABLE {appconfig.dataSchema}.{fishSpeciesTable}(
                code varchar(4) PRIMARY KEY,
                name varchar,
                mi_kmaw_name varchar,
                
                accessibility_gradient double precision not null,
                fall_height_threshold double precision not null,
                
                spawn_gradient_min numeric,
                spawn_gradient_max numeric,
                rear_gradient_min numeric,
                rear_gradient_max numeric,
                
                spawn_discharge_min numeric,
                spawn_discharge_max numeric,
                rear_discharge_min numeric,
                rear_discharge_max numeric,
                
                spawn_channel_confinement_min numeric,
                spawn_channel_confinement_max numeric,
                rear_channel_confinement_min numeric,
                rear_channel_confinement_max numeric
                );

                ALTER TABLE  {appconfig.dataSchema}.{fishSpeciesTable} OWNER TO cwf_analyst;
            """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        query = f"""
            INSERT INTO {appconfig.dataSchema}.{fishSpeciesTable}(
                code,
                name,
                mi_kmaw_name,

                accessibility_gradient,
                fall_height_threshold,

                spawn_gradient_min,
                spawn_gradient_max,
                rear_gradient_min,
                rear_gradient_max,
                
                spawn_discharge_min,
                spawn_discharge_max,
                rear_discharge_min,
                rear_discharge_max,
                
                spawn_channel_confinement_min,
                spawn_channel_confinement_max,
                rear_channel_confinement_min,
                rear_channel_confinement_max
            )
            SELECT
                a.code,
                a.name,
                a.mi_kmaw_name,

                a.accessibility_gradient,
                a.fall_height_threshold,

                a.spawn_gradient_min,
                a.spawn_gradient_max,
                a.rear_gradient_min,
                a.rear_gradient_max,
                
                a.spawn_discharge_min,
                a.spawn_discharge_max,
                a.rear_discharge_min,
                a.rear_discharge_max,
                
                a.spawn_channel_confinement_min,
                a.spawn_channel_confinement_max,
                a.rear_channel_confinement_min,
                a.rear_channel_confinement_max
            FROM {sourceTable} a;

            DROP TABLE {sourceTable};
            """
        
        # print(query)

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print (f"""Species parameters loaded to {appconfig.dataSchema}.{fishSpeciesTable}""")

if __name__ == "__main__":
    main()