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
# Work in progress
#
#

import appconfig
import subprocess
import pandas as pd



pd.options.display.max_columns = None
pd.options.display.max_rows = None

sourceTable = appconfig.dataSchema + ".barrier_assessment_raw"

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
# dbTargetTable = appconfig.config['BARRIER_PROCESSING']['barrier_assessments_table']

barrierAssessmentsFile = appconfig.barrierAssessmentsFile
# barrierAsessmentTable = appconfig.barrierAssessmnentTable

specCodes = appconfig.getSpecies()



def createView(conn):
    """
    Create barrier assessment table
    summary view
    """
    passability_cols  = ''

    for species in specCodes:
        species = species

        passability_cols = f"""
            {passability_cols}
            ,"additional information passability status _ {species}" as passability_status_{species}
        """

    query = f"""
        CREATE OR REPLACE VIEW {dbTargetSchema}_wcrp.barrier_assessments_vw  AS 
            SELECT 
                "site information barrier id (modelled)" as barrier_id,
                "site information crossing type" as crossing_type,
                "site information stream name" as stream_name,
                "site information road name" as road_name,
                "site information latitude (dd mm ss)" as latitude,
                "site information longitude (dd mm ss)" as longitude,
                "rapid assessment is there a visible outflow drop?" as is_there_visible_outflow_drop,
                "rapid assessment is the water depth less than 15cm anywheres in" as water_depth_lt_15_cm,
                "rapid assessment is the culvert backwatered only part of the wa" as is_culvert_backwatered,
                "rapid assessment is the stream width noticibly different above " as width_noticibly_different,
                "culvert information culvert material" as culvert_material,
                "culvert information culvert bottom" as culvert_bottom,
                "culvert information culvert shape" as culvert_shape,
                "additional information backwatered" as backwatered,
                "additional information length of culvert with embedment" as length_of_culvert_with_embedment,
                "stream characteristics stream width ratio" as stream_width_ratio,
                "upstream of culvert culvert length (m)" as culvert_length,
                "downstream of culvert culvert slope (%)" as culvert_slope,
                "downstream of culvert outflow drop (m)" as outflow_drop,
                "baffle information number of baffles" as num_baffles,
                "additional information notes" as notes,
                "additional information link" as link
                {passability_cols}
            FROM {sourceTable};

        ALTER TABLE {dbTargetSchema}_wcrp.barrier_assessments_vw OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def read_input_file():
    """
    Read excel spreadsheet and preprocess the data into a format that can be represented in SQL
    """
    dataFile = barrierAssessmentsFile

    if dataFile == 'None':
        return 'None'

    data = pd.read_excel(dataFile, header=[0, 1])

    # preprocess the csv into something that can be loaded into database
    data.columns = data.columns.map(' '.join)

    # print(data)

    data.to_csv('../barrier_asmts.csv', index=False)
    dataFile = '../barrier_asmts.csv'

    return dataFile



def main():

    dataFile = read_input_file()

    if dataFile != 'None':
    
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

            createView(conn)
            print("Created summary view")

        

        print('Done')
    else:
        print('No barrier assessment file. Skipping...')


if __name__ == "__main__":
    main()  