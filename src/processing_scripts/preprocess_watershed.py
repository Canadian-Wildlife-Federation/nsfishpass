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
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
# 
# DESCRIPTION
# 
# This script initializes and prepares stream network data for a specific watershed analysis. The logic of the query within the script is as follows:
# Key Logic Flow:
# 1.Converts user-specified watershed short names into database IDs by querying the chyf_aoi lookup table.
# 2.Sets up a new schema and creates a streams table with fields for identifiers, watershed codes, stream names, Strahler order (stream order), segment lengths, and geometry.
# 3.Stream Extraction (Core Query): 
# -	Uses ST_Intersects to find streams that overlap the target watershed.
# -	Clips stream geometries to watershed boundaries using ST_Intersection.
# -	Uses ST_Dump to convert any multi-part geometries into individual line segments.
# -	Generates new UUIDs for each segment while preserving the original source ID for linkage.
# -	DISTINCT ON (t1.id) ensures one record per source stream, avoiding duplicates from multiple watershed intersections.
# 4.Geometric Processing: 
# -	Calculates segment lengths in kilometers.
# -	Applies stream order weighting: 1st order streams get 25% weight, 2nd order get 75%, higher orders get 100%.
# -	Snaps geometries to a 0.01 unit grid.
# -	Removes any empty geometries created during intersection.
# -	Preserves original geometry in a separate column before snapping.
# 5.Placeholder Data: Temporarily populates channel confinement and discharge fields with random values (marked TODO for real data).
# 6.Secondary Watershed Attribution: If available, adds secondary watershed codes/names via spatial intersection; otherwise uses the config section name.
# 
#
import appconfig
import ast

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']

workingWatershedId = ast.literal_eval(appconfig.config[iniSection]['watershed_id'])
workingWatershedId = [x.upper() for x in workingWatershedId]

if len(workingWatershedId) == 1:
    workingWatershedId = f"('{workingWatershedId[0]}')"
else:
    workingWatershedId = tuple(workingWatershedId)

dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
# watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']
# secondaryWatershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['secondary_watershed_table']

watershedTable = appconfig.watershedTable
secondaryWatershedTable = appconfig.secondaryWatershedTable

publicSchema = "public"
aoi = "chyf_aoi"
aoiTable = publicSchema + "." + aoi

# stream order segment weighting
w1 = 0.25
w2 = 0.75

def main():

    with appconfig.connectdb() as conn:

        query = f"""
        SELECT id::varchar FROM {aoiTable} WHERE short_name IN {workingWatershedId};
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            ids = cursor.fetchall()

        aoi_ids = []
        
        for x in ids:
            aoi_ids.append(x[0])

        if len(aoi_ids) == 1:
            aoi_ids = f"('{aoi_ids[0]}')"
        else:
            aoi_ids = tuple(aoi_ids)
        
        query = f"""
            CREATE SCHEMA IF NOT EXISTS {dbTargetSchema};
        
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable};

            CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{dbTargetStreamTable}(
              {appconfig.dbIdField} uuid not null,
              source_id uuid not null,
              {appconfig.dbWatershedIdField} varchar not null,
              sec_code varchar,
              sec_name varchar,
              stream_name varchar,
              strahler_order integer,
              segment_length double precision,
              w_segment_length double precision,
              geometry geometry(LineString, {appconfig.dataSrid}),
              primary key ({appconfig.dbIdField})
            );
            
            CREATE INDEX {dbTargetSchema}_{dbTargetStreamTable}_geometry_idx ON {dbTargetSchema}.{dbTargetStreamTable} using gist(geometry);
            
            --ensure results are readable
            GRANT USAGE ON SCHEMA {dbTargetSchema} TO public;
            GRANT SELECT ON {dbTargetSchema}.{dbTargetStreamTable} to public;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {dbTargetSchema} GRANT SELECT ON TABLES TO public;

            INSERT INTO {dbTargetSchema}.{dbTargetStreamTable} 
                ({appconfig.dbIdField}, source_id, {appconfig.dbWatershedIdField}
                ,stream_name, strahler_order, geometry)
            SELECT DISTINCT ON (t1.id) gen_random_uuid(), t1.id, t1.aoi_id,
                t1.rivername1, t1.strahler_order,
                (ST_Dump((ST_Intersection(t1.geometry, t2.geometry)))).geom
            FROM {appconfig.dataSchema}.{appconfig.streamTable} t1
            JOIN {appconfig.dataSchema}.{appconfig.watershedTable} t2 ON ST_Intersects(t1.geometry, t2.geometry)
            WHERE aoi_id IN {aoi_ids};

            -------------------------
            --LONELY ISLAND DELETION
            DELETE FROM {dbTargetSchema}.{dbTargetStreamTable}
            WHERE source_id IN ('c21eefa8-0a48-4c7f-82e2-21c870c32ed6',
                                    '6bb292e2-461c-4ccb-8b1d-39531227b894',
                                    'de89cb0e-f191-4874-8492-d671c26b457b',
                                    '814065e4-7bd6-4e54-94c4-9b1b16f94a3b',
                                    'adf9555b-923d-40b7-a04d-ffdb7d5262a4',
                                    '7d8e0017-1187-40b5-9aff-f20dc5c3f90f',
                                    'ff0b60df-7308-45ec-b1b9-2daf3728fd68',
                                    'b8bbf406-4f60-4aab-b255-5ad005491783',
                                    'ab2f7a7c-279f-4f1e-b0f5-f81da91948c7',
                                    'c0441a60-e0e6-43fa-9835-ac9fa3a0aa38',
                                    '1e2eaed5-0f4f-433c-9f86-2fbdc5b2ea99',
                                    'd0beeb29-30bd-461d-b34b-d2a1c4958079',
                                    '880aba57-9e11-40ba-b9a9-54c1a1cf82dc',
                                    '240856ed-c892-4249-91df-5acaca5af2ec',
                                    'a56c51c4-0de7-4aaf-b02b-a5b3b140a3c2',
                                    'e72b4823-9897-4402-8fd6-9152e41fbbf7',
                                    'beb80465-f567-4b58-bac3-9ec5d6b6a206',
                                    '5ed1200c-8025-495b-8f9b-97b5f15a22c4',
                                    '1f2b5947-a115-4b03-9f71-71a23cea1f94',
                                    'c828c558-d184-4120-bc62-e68b1cafdaff',
                                    'ce9da3d3-6488-4fcd-a8ee-4fb78469307e',
                                    '4e666f0d-3d4c-4e83-b94c-5008a301b60d',
                                    '466b3c7a-756b-4bde-9ded-4acfa247c68d',
                                    'f8aa3c3c-7de4-462f-9458-a483c3463226',
                                    'f38c99cb-7c63-411c-8aa6-60d687b21c59',
                                    '31109372-6907-4713-b292-c8106ff2f11e',
                                    '39f6eeeb-7d07-43db-9203-68a8b55a95b4',
                                    '72d4dba9-c209-4039-b15f-dd4ea9ed1636',
                                    'ea6d9538-9ee6-42bb-b338-29c5012b2015',
                                    '2ade345f-785c-4c05-a61d-5cca2b6f7f5c',
                                    'de636deb-258f-4583-9899-54ff2371a622',
                                    'afba7feb-e470-4134-b846-d1caa338a29f',
                                    '8e62c085-578a-46ad-9138-016a89372766',
                                    'd60f5450-fb8f-47e1-8929-32a940f81f28',
                                    '0ae71823-2adc-493d-9cea-28e047e4025f',
                                    'f4b77bdf-2153-4691-93a8-12f5db1127e6',
                                    '3efdfffa-e9f4-4f49-bcee-4e87282c6ec7',
                                    '9968bde6-4986-490f-8b76-d0729ec7c626',
                                    'a3b0c1d1-9728-44a6-a854-8b69e76a7822',
                                    'f116f4f0-f0ec-460b-8cf0-96510f1ebe49',
                                    'b8d96741-df02-4948-ad7e-aeae13b79eaf',
                                    '2e105b3f-5500-4f6c-a45f-696cebd3e89c',
                                    '66879381-a1c0-4995-9dd2-833d41ff9225',
                                    'e99a69a4-134c-43d7-af1a-a39ab30cebd9',
                                    'c0932299-9104-4d4b-a761-3ba1db635ae7',
                                    'e15b5b45-8186-4b18-9def-39692db8a6b1',
                                    '0006839a-806d-4bbc-9bb6-35fc99077a8a',
                                    '4956c7af-63e6-4205-bda9-4636a79513d8',
                                    '5bf9594e-ea8c-4a50-a3e7-ca6d2df8506b',
                                    '3effb7f6-5f6c-454f-945d-bbde180bb20c',
                                    '8511e2ba-961a-4e0c-be94-d71f54131e82',
                                    '9784b96a-1967-4837-bef4-6d4f33507586',
                                    'f8aa3c3c-7de4-462f-9458-a483c3463226',
                                    '2f139975-9596-4fe1-a69a-70b541d74d92',
                                    '9e8aeecc-b199-45bd-8fd1-1fca636b17bc',
                                    'de89cb0e-f191-4874-8492-d671c26b457b',
                                    '74b36136-7379-4433-86b6-36c2f0ac14df',
                                    '0d03184d-b9bd-4ca0-9622-4995405463bf',
                                    '1628eca1-7414-418d-9221-ba2ecb8d457e',
                                    '075fdfd3-087e-4a3f-bb5e-aca3aa8e803e',
                                    'e72b4823-9897-4402-8fd6-9152e41fbbf7',
                                    '5a384732-5638-4b26-ae31-7b754a4a12b3',
                                    '7f600d35-2409-4e9a-8f12-63ffe50c41cd',
                                    '3f31add2-6717-4240-97cb-ac96e6006ba8',
                                    '04e14410-838f-498c-8e6e-277e9db4ed07',
                                    '68246431-1e6d-467a-b9fe-601790dc7a24',
                                    '5a384732-5638-4b26-ae31-7b754a4a12b3',
                                    'b2463096-1e41-41c5-b1f0-c6a42c617658',
                                    '2fe71b30-82ac-4f4b-a0ec-00f657e880d3',
                                    '653fd18c-dcee-488a-b01a-fa7a41264849',
                                    '0f102749-b015-4089-ab5e-eaca3ddb7ae8',
                                    '457d7996-3ae2-4176-b6c0-1ccabb48f2ee',
                                    '40a3e077-59e3-4038-ae4a-882c9b4b5b2c',
                                    '6e74776d-9194-4dc2-9783-9d7c964179d9',
                                    '5341c3c2-14aa-4a55-98ac-d235d0a88bb5',
                                    '0cd9c55b-1630-482f-8b3b-2fa93252e83b',
                                    '3227fb86-78ae-4c50-9fda-f9c9b25cd394',
                                    '2c440367-4143-4c97-a60a-b92eccd0438b',
                                    '9aa4d1ae-3e20-47ed-a9db-7030b1e08a92',
                                    '226cb4d7-34eb-4243-8be8-2f3adb7ad59a',
                                    '5a1dcbb6-333f-495d-9980-c60ecc4ea745',
                                    'a813b041-3d64-4237-b2e9-3af5fc79ad75',
                                    '05343c65-01cd-4876-b291-2b5c518b9618',
                                    'f8aa3c3c-7de4-462f-9458-a483c3463226',
                                    'ba1f70ed-7e5a-4c21-a6d3-3b630559efea',
                                    'b905309f-f27a-4884-9ea3-01ccd45a2a3e',
                                    '51da06a3-e264-45c4-b4fd-24a1c996025c',
                                    '3393557a-e42c-4702-937c-f8737c1fca16',
                                    'cfdc59c9-32de-45c1-94a8-d0d79ddf8531',
                                    'e72b4823-9897-4402-8fd6-9152e41fbbf7',
                                    '5d7ba68b-830c-48ce-869a-df5036179b5f',
                                    '1200d199-7bde-4286-9d65-f1f4bbbfdcb1',
                                    '46654416-cb0b-458d-af68-35e52eb1fb0c',
                                    '1f2da3d6-16cd-42f8-bf79-8a2d3cd6738e',
                                    '3af41218-c590-4603-b01d-e8e6fcf9a4f3',
                                    'ff3527a3-9993-4a71-8e15-205502917e74',
                                    '4976d8d0-d33a-4ea5-9bef-36f3c17cf4bb',
                                    'cacc77af-e8f0-4a23-8b3c-9a228065fdd8',
                                    'ac3f9d0b-5b67-4b1f-89d1-166f19313e17',
                                    '54ea4664-2031-424d-b852-a55c7393784a',
                                    '0cd9c55b-1630-482f-8b3b-2fa93252e83b',
                                    '31e98250-f1f5-4396-8731-72720b00ad69',
                                    'bdb635f6-c8d4-432d-995c-7b19f6665e09',
                                    '9519e424-4b9c-4740-9110-27efece22be6',
                                    '7e92f000-1e82-43ac-85be-d0e7bd7d5d79',
                                    'acc9b06a-6020-4254-a644-3a2d6c672db1',
                                    'f07cbbed-7d31-4110-ba33-cf8e4289690b',
                                    '5341c3c2-14aa-4a55-98ac-d235d0a88bb5',
                                    '8340e2d3-2109-42aa-9ca6-3e8cd9222f9b',
                                    'c37110a5-0a7f-4f12-870c-e5f0bacdbdf6',
                                    'ac334b9b-9ac7-4f2a-acca-cef1ce3ddfb8',
                                    '4dac9131-26fd-4a3a-b135-14797bc27189',
                                    '6bb292e2-461c-4ccb-8b1d-39531227b894',
                                    '95e9d71a-a141-4c15-ba7c-8ae9d98a9774',
                                    'cd76ddef-5275-4e62-92e8-9d87c14b64ba',
                                    '0e88af49-802a-4214-8fbe-c0a9a8ea925a',
                                    '6da2e8ec-70a4-4364-bdf6-6487e99a83d4',
                                    '3db9bf10-74a3-4087-b17e-810f5c8c8789',
                                    'e72b4823-9897-4402-8fd6-9152e41fbbf7',
                                    '1cc60156-965d-4045-8777-20b2bc1eb6b9',
                                    '621b34a3-8e32-4893-8e29-bc6c50483cdc',
                                    'c233ed21-7fbf-4745-97bc-e1316d8b2abc');
            -------------------------
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set segment_length = st_length2d(geometry) / 1000.0;
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set w_segment_length = (case strahler_order 
                                                                                    when 1 then segment_length * {w1}
                                                                                    when 2 then segment_length * {w2}
                                                                                    else segment_length
                                                                                    end);
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column geometry_original geometry(LineString, {appconfig.dataSrid});
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set geometry_original = geometry;
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set geometry = st_snaptogrid(geometry, 0.01);

            DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE ST_IsEmpty(geometry);
            -------------------------
            
            --TODO: remove this when values are provided
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {appconfig.streamTableChannelConfinementField} numeric;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {appconfig.streamTableDischargeField} numeric;
            
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {appconfig.streamTableChannelConfinementField} = floor(random() * 100);
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {appconfig.streamTableDischargeField} = floor(random() * 100);

            ALTER SCHEMA {dbTargetSchema} OWNER TO cwf_analyst;
            ALTER TABLE  {dbTargetSchema}.{dbTargetStreamTable} OWNER TO cwf_analyst;
       
        """
        # print(query)
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        if secondaryWatershedTable != 'None':
            query = f"""
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} s
            SET 
                sec_code = t3.SEC_CODE,
                sec_name = t3.SEC_NAME
            FROM {appconfig.dataSchema}.{secondaryWatershedTable} t3 
            WHERE ST_Intersects(s.geometry, t3.geometry)
            """
        else:
            query = f"""
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} s
            SET 
                sec_name = '{iniSection}'
            """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        
    print(f"""Initializing processing for watershed {workingWatershedId} complete.""")

if __name__ == "__main__":
    main()     