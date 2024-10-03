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
# This script loads fish aquatic invasive species data to the barriers table
#
import subprocess
import appconfig

iniSection = appconfig.args.args[0]
streamTable = appconfig.config['PROCESSING']['stream_table']
dbTargetSchema = appconfig.config[iniSection]['output_schema']

datatable = dbTargetSchema + ".aquatic_invasive_species"

def main():

    if iniSection != 'cmm':
        return

    with appconfig.connectdb() as conn:
        query = f"""
            ALTER TABLE {datatable} ADD COLUMN IF NOT EXISTS stream_id uuid;

            UPDATE {iniSection}.barriers SET ais_upstr = NULL, ais_downstr = NULL;

            -- Get stream ids nearest to each ais point
            WITH nearest AS (
                SELECT 
                    ais.id as ais_id,
                    s.id as stream_id,
                    s.geometry as stream_geom,
                    s.dist
                FROM {datatable} ais
                CROSS JOIN LATERAL (
                    SELECT s.id, s.geometry, s.geometry <-> ais.geom as dist
                    FROM {iniSection}.{streamTable} s
                    ORDER BY dist
                    LIMIT 1
                ) s
            )
            UPDATE {datatable} ais
            SET stream_id =
                (
                    SELECT n.stream_id
                    FROM nearest n
                    WHERE ais.id = n.ais_id
                )

        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # Assign upstream ais
        query = f"""
            -- upstream ais
            WITH ais_upstr AS (
                SELECT ais.id, ais.species, b_dnstr.barrier_id as b_dnstr
                FROM {datatable} ais
                CROSS JOIN LATERAL
                (
                    WITH RECURSIVE upstream(id, geometry) AS (
                        SELECT id, geometry FROM {iniSection}.{streamTable} WHERE id = ais.stream_id
                        UNION ALL
                        SELECT n.id, n.geometry
                        FROM {iniSection}.{streamTable} n, upstream w
                        WHERE ST_DWithin(ST_StartPoint(w.geometry),ST_EndPoint(n.geometry),0.01)
                        AND n.id IS NOT NULL
                    )
                    SELECT u.id as stream_id, b.id as barrier_id, b.barrier_cnt_downstr_as
                    FROM upstream u
                    INNER JOIN {iniSection}.barriers b
                        ON b.stream_id_down = u.id
                    WHERE b.barrier_cnt_downstr_as = (
                        SELECT MIN(b.barrier_cnt_downstr_as) 
                        FROM upstream u
                        INNER JOIN {iniSection}.barriers b
                            ON b.stream_id_down = u.id
                    )
                ) AS b_dnstr
            )
            UPDATE {iniSection}.barriers b
            SET ais_upstr = array_append(ais_upstr, au.species)
            FROM ais_upstr au
            WHERE au.b_dnstr = b.id;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # Assign downstream ais
        query = f"""
            WITH ais_dnstr AS (
                SELECT ais.id, ais.species, b_upstr.barrier_id as b_upstr
                FROM {datatable} ais
                CROSS JOIN LATERAL
                (
                    WITH RECURSIVE downstream(id, geometry) AS (
                        SELECT id, geometry FROM {iniSection}.{streamTable} WHERE id = ais.stream_id
                        UNION ALL
                        SELECT n.id, n.geometry
                        FROM {iniSection}.{streamTable} n, downstream w
                        WHERE ST_DWithin(ST_EndPoint(w.geometry),ST_StartPoint(n.geometry),0.01)
                        AND n.id IS NOT NULL
                    )
                    SELECT d.id as stream_id, b.id as barrier_id, b.barrier_cnt_upstr_as
                    FROM downstream d
                    INNER JOIN {iniSection}.barriers b
                        ON b.stream_id_up = d.id
                    WHERE b.barrier_cnt_upstr_as = (
                        SELECT MIN(b.barrier_cnt_upstr_as) 
                        FROM downstream d
                        INNER JOIN {iniSection}.barriers b
                            ON b.stream_id_up = d.id
                    )
                ) AS b_upstr
            )
            UPDATE {iniSection}.barriers b
            SET ais_downstr = array_append(ais_downstr, ad.species)
            FROM ais_dnstr ad
            WHERE ad.b_upstr = b.id;
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        

    print("Loading AIS data complete")

if __name__ == "__main__":
    main()