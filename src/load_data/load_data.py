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
# This script loads gdb files into postgis database, create by the create_db.py script
#
import subprocess
import appconfig
from psycopg2.extras import RealDictCursor

streamTable = appconfig.config['DATABASE']['stream_table']
roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']
secondaryWatershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['secondary_watershed_table']
tidalZones = appconfig.config['CREATE_LOAD_SCRIPT']['tidal_zones']

file = appconfig.config['CREATE_LOAD_SCRIPT']['raw_data']
watershedfile = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_data']
tidalZoneFile = appconfig.config['CREATE_LOAD_SCRIPT']['tidal_zone_data']
temptable = appconfig.dataSchema + ".temp"

sheds = appconfig.config['HABITAT_STATS']['watersheds'].split(",")

def loadWatersheds(conn):

    print("Loading watershed boundaries")
    layer = watershedTable
    datatable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    # query = f"""
    # DROP TABLE IF EXISTS {appconfig.dataSchema}.{watershedTable};
    # """

    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    # conn.commit()


    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + datatable + '" -nlt CONVERT_TO_LINEAR -lco GEOMETRY_NAME=geometry "' + watershedfile + '" ' + layer
    subprocess.run(pycmd)
    
    query = f"""
    ALTER TABLE {appconfig.dataSchema}.{watershedTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def loadSecondaryWatersheds(conn):
    print("Loading secondary watershed boundaries")
    # layers = ["cmm_halfway", "cmm_avon", "cmm_stcroix", "cmm_shore_direct_1", "cmm_shore_direct_2", "cmm_shore_direct_3"]
    #layers = ["cmm_halfway", "cmm_avon", "cmm_stcroix"]
    layers = [secondaryWatershedTable]

    if secondaryWatershedTable == 'None':
        return

    query = f"""
    DROP TABLE IF EXISTS {appconfig.dataSchema}.{secondaryWatershedTable};
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    datatable = appconfig.dataSchema + "." + secondaryWatershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    for layer in layers:
        pycmd = '"' + appconfig.ogr + '" -update -append -preserve_fid -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geom -nln "' + datatable + '" -nlt CONVERT_TO_LINEAR -lco GEOMETRY_NAME=geometry "' + watershedfile + '" ' + layer
        subprocess.run(pycmd)

    query = f"""
    CREATE INDEX {appconfig.dataSchema}_{watershedTable}_geometry on {appconfig.dataSchema}.{watershedTable} using gist(geometry);

    ALTER TABLE {appconfig.dataSchema}.{secondaryWatershedTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def loadTidalZones(conn):
    print("Loading tidal zones")
    layer = tidalZones

    if tidalZones == 'None':
        return 
    
    datatable = appconfig.dataSchema + "." + tidalZones
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + datatable + '" -nlt CONVERT_TO_LINEAR -lco GEOMETRY_NAME=geometry "' + tidalZoneFile + '" ' + layer
    subprocess.run(pycmd)

    query = f"""
    ALTER TABLE {appconfig.dataSchema}.{watershedTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def loadStreams(conn):

    print("Loading stream data")

    publicSchema = "public"

    flowpath = "chyf_flowpath"
    flowpathProperties = "chyf_flowpath_properties"
    flowpathNames = "chyf_names"
    aoi = "chyf_aoi"

    flowpathTable = publicSchema + "." + flowpath
    flowpathPropertiesTable = publicSchema + "." + flowpathProperties
    flowpathNamesTable = publicSchema + "." + flowpathNames
    aoiTable = publicSchema + "." + aoi

    aois = str(sheds)[1:-1].upper()
    query = f"""
    SELECT id::varchar FROM {aoiTable} WHERE short_name IN ({aois});
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    if len(rows) == 1:
        aoiTuple = f"('{rows[0]['id']}')"
    else:
        aoiTuple = tuple([row['id'] for row in rows])
    
    query = f"""
    DROP TABLE IF EXISTS {appconfig.dataSchema}.{streamTable};
    DROP TABLE IF EXISTS {appconfig.dataSchema}.{flowpathProperties};
    
    CREATE TABLE {appconfig.dataSchema}.{streamTable} as SELECT * FROM {flowpathTable} WHERE aoi_id IN {aoiTuple} AND ef_type != 2 AND rank = 1;
    CREATE TABLE {appconfig.dataSchema}.{flowpathProperties} as SELECT * FROM {flowpathPropertiesTable} WHERE aoi_id IN {aoiTuple};

    ALTER TABLE {appconfig.dataSchema}.{streamTable} ALTER COLUMN geometry TYPE geometry(LineString, {appconfig.dataSrid}) USING ST_Transform(geometry, {appconfig.dataSrid});
    
    CREATE INDEX {appconfig.dataSchema}_{streamTable}_geometry on {appconfig.dataSchema}.{streamTable} using gist(geometry); 
    CREATE INDEX {appconfig.dataSchema}_{streamTable}_id on {appconfig.dataSchema}.{streamTable} (id);
    CREATE INDEX {appconfig.dataSchema}_{flowpathProperties}_id on {appconfig.dataSchema}.{flowpathProperties} (id);

    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD PRIMARY KEY (id);
    
    ANALYZE {appconfig.dataSchema}.{flowpathProperties};
    ANALYZE {appconfig.dataSchema}.{streamTable};

    ALTER TABLE {appconfig.dataSchema}.{streamTable} OWNER TO cwf_analyst;
    ALTER TABLE {appconfig.dataSchema}.{flowpathProperties} OWNER TO cwf_analyst;
    """
    # print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    query = f"""
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN rivername1 varchar;
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN rivername2 varchar;
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN strahler_order integer;
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN watershed_name varchar;

    UPDATE {appconfig.dataSchema}.{streamTable} SET rivername1 = a.name_en FROM {flowpathNamesTable} a WHERE rivernameid1 IS NOT NULL AND rivernameid1 = a.name_id;
    UPDATE {appconfig.dataSchema}.{streamTable} SET rivername2 = a.name_en FROM {flowpathNamesTable} a WHERE rivernameid2 IS NOT NULL AND rivernameid2 = a.name_id;
    UPDATE {appconfig.dataSchema}.{streamTable} b SET strahler_order = a.strahler_order FROM {appconfig.dataSchema}.{flowpathProperties} a WHERE b.id = a.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    if secondaryWatershedTable != 'None':
        query = f'UPDATE {appconfig.dataSchema}.{streamTable} b SET watershed_name = a.sec_name FROM {appconfig.dataSchema}.{secondaryWatershedTable} a WHERE ST_INTERSECTS(b.geometry, a.geometry);'

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()


def loadRoads(conn):
    
    print("Loading road data")

    layer = "nsrn"
    datatable = appconfig.dataSchema + "." + roadTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + datatable + '" -lco GEOMETRY_NAME=geometry -lco FID=fid "' + file + '" ' + layer
    subprocess.run(pycmd)

    query = f"""
    --CREATE INDEX {appconfig.dataSchema}_{roadTable}_geometry on {appconfig.dataSchema}.{roadTable} using gist(geometry);

    ALTER TABLE {appconfig.dataSchema}.{roadTable} ADD COLUMN watershed_name varchar;

    ALTER TABLE {appconfig.dataSchema}.{roadTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    if secondaryWatershedTable != 'None':
        query = f'UPDATE {appconfig.dataSchema}.{roadTable} b SET watershed_name = a.sec_name FROM {appconfig.dataSchema}.{secondaryWatershedTable} a WHERE ST_INTERSECTS(b.geometry, a.geometry);'

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()


def main():

    # -- MAIN SCRIPT --  

    print("Connecting to database")

    conn = appconfig.connectdb()
    loadWatersheds(conn)
    loadSecondaryWatersheds(conn)
    loadTidalZones(conn)
    loadStreams(conn)
    loadRoads(conn)
    
    print("Loading NS dataset complete")

if __name__ == "__main__":
    main()