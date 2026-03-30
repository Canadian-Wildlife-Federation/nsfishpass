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
# This script uses dem data to assign elevation values to
# linear geometries use bilinear interpolation of dem values
#
import subprocess
import appconfig
import os
import numpy
import tifffile as tif
import shapely.wkb
import shapely.geometry
from math import floor
import json
import psycopg2.extras
from psycopg2.extras import RealDictCursor
import ast

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetTable = appconfig.config['PROCESSING']['stream_table']
workingWatershedId = ast.literal_eval(appconfig.config[iniSection]['watershed_id'])
workingWatershedId = [x.upper() for x in workingWatershedId]

if len(workingWatershedId) == 1:
    workingWatershedId = f"('{workingWatershedId[0]}')"
else:
    workingWatershedId = tuple(workingWatershedId)

dbTargetGeom = appconfig.config['ELEVATION_PROCESSING']['3dgeometry_field']
# demDir = appconfig.config['ELEVATION_PROCESSING']['dem_directory']
demDir = appconfig.demDir

demfiles = []

class DEMFile:
    def __init__(self, filename, xmin, ymin, xmax, ymax, xcellsize, ycellsize, xcnt, ycnt, srid, nodata):
        self.filename = filename
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax
        self.xcellsize = xcellsize
        self.ycellsize = ycellsize
        self.xcnt = xcnt
        self.ycnt = ycnt
        self.srid = srid
        self.nodata = nodata

def getWatershedIds(conn):
    
    publicSchema = "public"
    aoi = "chyf_aoi"
    aoiTable = publicSchema + "." + aoi

    aois = workingWatershedId

    # aois = str(sheds)[1:-1].upper()

    query = f"""
    SELECT id::varchar FROM {aoiTable} WHERE short_name IN {aois};
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    if len(rows) == 1:
        ids = [row['id'] for row in rows]
        ids = f"('{ids[0]}')"
    else:
        ids = tuple([row['id'] for row in rows])

    return ids
        
def prepareOutput(conn):
    
    #see comment below - st_force3d with custom z value
    #is only supported in newer postgis so I wrote my own
    #for now but this could be replaced if newer postgis used
    
    # force3dfunction = f"""{appconfig.dataSchema}.st_force3d"""
    
    query = f"""
    
    ALTER TABLE {dbTargetSchema}.{dbTargetTable} 
    ADD COLUMN IF NOT EXISTS {dbTargetGeom} geometry(LineStringZ, {appconfig.dataSrid});

    UPDATE {dbTargetSchema}.{dbTargetTable} 
    SET {dbTargetGeom} = St_Force3D({appconfig.dbGeomField}, {appconfig.NODATA}); 
    
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    

def indexDem():
    #read all files in dem
    #get bounds
    #build index of 
    print("indexing dem files")
    demfiles = []
    for demfile in os.listdir(demDir):
        if (demfile.endswith('.tif') or demfile.endswith('.tiff')):
            demfiles.append(getFileDetails(os.path.join(demDir,demfile)))
            
    return demfiles
  
def getFileDetails(demfile):
    print("    reading: " + demfile)
    
    
    out = subprocess.run("\"" + appconfig.gdalsrsinfo + "\" -e -o epsg " + "\"" + demfile + "\"", capture_output=True)
    srid = out.stdout.decode('utf-8').split(':')[1].strip()
    
    out = subprocess.run("\"" + appconfig.gdalinfo + "\" -json " + "\"" + demfile + "\"", capture_output=True)

    jsonout = out.stdout.decode('utf-8')
    
    metadata = json.loads(jsonout)
    
    xmin = metadata['cornerCoordinates']['lowerLeft'][0]
    ymin = metadata['cornerCoordinates']['lowerLeft'][1]
    
    xmax = metadata['cornerCoordinates']['upperRight'][0]
    ymax = metadata['cornerCoordinates']['upperRight'][1]
    
    xcnt = metadata['size'][0]
    ycnt = metadata['size'][1]
    
    xsize = (xmax - xmin) / xcnt
    ysize = (ymax - ymin) / ycnt 
    
    nodata = metadata['bands'][0]['noDataValue']

    #USE IF ERROR UNDEFINED SRID APPEARS (CHECK IF SRID:-1)
    #print(f'xmin: {xmin}\n ymin:{ymin}\n xmax:{xmax}\n ymax:{ymax}\n xsize:{xsize}\n ysize:{ysize}\n xcnt:{xcnt}\n ycnt:{ycnt}\n srid:{srid}\n nodata:{nodata}')

    return DEMFile(demfile, xmin, ymin, xmax,ymax, xsize, ysize, xcnt, ycnt, srid, nodata)
#-- BELOW CODE IS FOR WHEN SRID HANDLING IS NEEDED - COMMENTED OUT FOR NOW
#-- CODE BELOW IS FOR WHEN SRID = -1, THEREFORE UNDEFINED, AND THE COMMENTED OUT FUNCTIONS ASSIGN A DEFAULT SRID OR GUESS BASED ON THE DEM EXTENT
#-- COMMENT ABOVE FUNCTIONS WITH MATCHING NAMES
# def indexDem(default_srid=None, assign_crs=False):
#     """
#     Index DEM files with CRS handling.
    
#     Args:
#         default_srid: Default SRID to use for files with undefined CRS
#         assign_crs: If True, create new files with CRS assigned
    
#     Returns:
#         List of DEMFile objects
#     """
#     print("indexing dem files")
#     demfiles = []
    
#     for demfile in os.listdir(demDir):
#         if (demfile.endswith('.tif') or demfile.endswith('.tiff')):
#             filepath = os.path.join(demDir, demfile)
            
#             # Check if this file has undefined CRS and we want to assign one
#             if assign_crs and default_srid:
#                 out = subprocess.run("\"" + appconfig.gdalsrsinfo + "\" -e -o epsg " + "\"" + filepath + "\"", capture_output=True)
#                 srid_output = out.stdout.decode('utf-8').strip()
                
#                 if ':' in srid_output and srid_output.split(':')[1].strip() == '-1':
#                     # Create new file with CRS assigned
#                     filepath = assign_crs_to_file(filepath, default_srid)
            
#             demfiles.append(getFileDetails(filepath, default_srid))
            
#     return demfiles

# def getFileDetails(demfile, default_srid=None):
#     print("    reading: " + demfile)
    
#     # Try to get SRID from the file
#     out = subprocess.run("\"" + appconfig.gdalsrsinfo + "\" -e -o epsg " + "\"" + demfile + "\"", capture_output=True)
#     srid_output = out.stdout.decode('utf-8').strip()
    
#     # Handle cases where SRID is undefined or -1
#     if ':' in srid_output:
#         srid = srid_output.split(':')[1].strip()
#         if srid == '-1' or srid == '':
#             srid = handle_undefined_srid(demfile, default_srid)
#     else:
#         srid = handle_undefined_srid(demfile, default_srid)
    
#     out = subprocess.run("\"" + appconfig.gdalinfo + "\" -json " + "\"" + demfile + "\"", capture_output=True)
#     jsonout = out.stdout.decode('utf-8')
#     metadata = json.loads(jsonout)
    
#     xmin = metadata['cornerCoordinates']['lowerLeft'][0]
#     ymin = metadata['cornerCoordinates']['lowerLeft'][1]
#     xmax = metadata['cornerCoordinates']['upperRight'][0]
#     ymax = metadata['cornerCoordinates']['upperRight'][1]
    
#     xcnt = metadata['size'][0]
#     ycnt = metadata['size'][1]
    
#     xsize = (xmax - xmin) / xcnt
#     ysize = (ymax - ymin) / ycnt 
    
#     nodata = metadata['bands'][0]['noDataValue']

#     #print(f'xmin: {xmin}\n ymin:{ymin}\n xmax:{xmax}\n ymax:{ymax}\n xsize:{xsize}\n ysize:{ysize}\n xcnt:{xcnt}\n ycnt:{ycnt}\n srid:{srid}\n nodata:{nodata}')

#     return DEMFile(demfile, xmin, ymin, xmax, ymax, xsize, ysize, xcnt, ycnt, int(srid), nodata)

# # Handle undefined SRID by guessing or using default (to be commented out if not needed)
# def handle_undefined_srid(demfile, default_srid=None):
#     """
#     Handle DEM files with undefined SRID by guessing or using default.
    
#     Args:
#         demfile: Path to the DEM file
#         default_srid: Default SRID to use if provided
    
#     Returns:
#         SRID as string
#     """
#     print(f"    Warning: {os.path.basename(demfile)} has undefined CRS (SRID=-1)")
    
#     if default_srid:
#         print(f"    Using provided default SRID: {default_srid}")
#         return str(default_srid)
    
#     # Try to guess based on coordinate bounds
#     out = subprocess.run("\"" + appconfig.gdalinfo + "\" -json " + "\"" + demfile + "\"", capture_output=True)
#     jsonout = out.stdout.decode('utf-8')
#     metadata = json.loads(jsonout)
    
#     xmin = metadata['cornerCoordinates']['lowerLeft'][0]
#     ymin = metadata['cornerCoordinates']['lowerLeft'][1]
#     xmax = metadata['cornerCoordinates']['upperRight'][0]
#     ymax = metadata['cornerCoordinates']['upperRight'][1]
    
#     # Guess CRS based on coordinate ranges
#     guessed_srid = guess_srid_from_bounds(xmin, ymin, xmax, ymax)
#     #print(f"    Guessed SRID based on bounds: {guessed_srid}")
    
#     return str(guessed_srid)


# def guess_srid_from_bounds(xmin, ymin, xmax, ymax):
#     """
#     Guess the most likely SRID based on coordinate bounds.
    
#     Returns:
#         Integer SRID
#     """
#     # Check if coordinates are in typical lat/lon ranges
#     if (-180 <= xmin <= 180 and -180 <= xmax <= 180 and 
#         -90 <= ymin <= 90 and -90 <= ymax <= 90):
#         return 4326  # WGS84
    
#     # Check for Canadian coordinate systems (common for Canadian Wildlife Federation)
#     # NAD83 UTM zones for Canada
#     if (200000 <= xmin <= 800000 and 5000000 <= ymin <= 7000000):
#         # Likely UTM - guess zone based on longitude if we had it
#         # For now, return a common Canadian projection
#         return 3978  # NAD83 Canada Atlas Lambert
    
#     # Check for other UTM-like coordinates
#     elif (abs(xmin) > 100000 and abs(xmax) > 100000):
#         #print(f"    Coordinates appear projected but unknown system: ({xmin}, {ymin})")
#         return 3857  # Web Mercator as fallback
    
#     else:
#         #print(f"    Cannot determine CRS from bounds: ({xmin}, {ymin}, {xmax}, {ymax})")
#         return 4326  # WGS84 as ultimate fallback

# def assign_crs_to_file(input_file, srid, output_file=None):
#     """
#     Create a new DEM file with the specified CRS assigned.
    
#     Args:
#         input_file: Path to input DEM file
#         srid: SRID to assign
#         output_file: Output file path (optional, will add '_crs' suffix if not provided)
    
#     Returns:
#         Path to the new file with CRS assigned
#     """
#     if not output_file:
#         base, ext = os.path.splitext(input_file)
#         output_file = f"{base}_crs{ext}"
    
#     # Use gdal_translate to assign CRS
#     cmd = [
#         appconfig.gdal_translate if hasattr(appconfig, 'gdal_translate') else 'gdal_translate',
#         '-a_srs', f'EPSG:{srid}',
#         input_file,
#         output_file
#     ]
    
#     try:
#         result = subprocess.run(cmd, capture_output=True, text=True)
#         if result.returncode == 0:
#             print(f"    Created {output_file} with SRID {srid}")
#             return output_file
#         else:
#             print(f"    Error assigning CRS: {result.stderr}")
#             return input_file
#     except Exception as e:
#         print(f"    Error running gdal_translate: {e}")
#         return input_file
    
def processArea(demfile, connection, watershed_id, onlymissing = False):
    print("    processing: " + (demfile.filename))
    
    #get edges
    query = f"""
        SELECT srid 
        FROM public.geometry_columns
        WHERE
        f_table_schema = '{dbTargetSchema}' and 
        f_table_name = '{dbTargetTable}' and 
        f_geometry_column = '{appconfig.dbGeomField}'
    """
    srid = -9999
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        srid = cursor.fetchone()[0]
    
    if onlymissing: 
        #only load features with at least one missing elevation values  
        query = f"""
            WITH ids AS (
                SELECT distinct id FROM (
                    SELECT {appconfig.dbIdField}, st_z ((ST_DumpPoints({dbTargetGeom})).geom ) AS z 
                    FROM {dbTargetSchema}.{dbTargetTable}
                ) foo WHERE foo.z = {appconfig.NODATA}
            ),
            env AS (
                SELECT st_transform(
                  st_setsrid(
                    st_makebox2d(st_point({demfile.xmin}, {demfile.ymin}), st_point({demfile.xmax}, {demfile.ymax})), 
                      {demfile.srid}
                  ),{srid}
                ) as bbox
            )
            SELECT t.{appconfig.dbIdField} as id, st_transform(t.{dbTargetGeom}, {demfile.srid}) as geometry
            FROM {dbTargetSchema}.{dbTargetTable} t, env
            WHERE t.{dbTargetGeom} && env.bbox AND t.{appconfig.dbWatershedIdField} IN {watershed_id}
            AND t.{appconfig.dbIdField} in (select id from ids);
        """
    else:
        #load all features
        query = f"""
            WITH
            env AS (
                SELECT st_transform(
                  st_setsrid(
                    st_makebox2d(st_point({demfile.xmin}, {demfile.ymin}), st_point({demfile.xmax}, {demfile.ymax})), 
                      {demfile.srid}
                  ),{srid}
                ) as bbox
            )
            SELECT t.{appconfig.dbIdField} as id, st_transform(t.{dbTargetGeom}, {demfile.srid}) as geometry
            FROM {dbTargetSchema}.{dbTargetTable} t, env
            WHERE t.{dbTargetGeom} && env.bbox AND t.{appconfig.dbWatershedIdField} IN {watershed_id}
        """

    newvalues = [] 
    
    with connection.cursor() as cursor:
        
            cursor.execute(query)
            features = cursor.fetchall()
            if (len(features) == 0):
                return
            print("      reading dem")
            imarray = numpy.array(tif.imread(demfile.filename))
            
            print("      processing")
            for feature in features:
                geom = shapely.wkb.loads(feature[1] , hex=True)
                fid = feature[0]
                #print("processing: " + str(fid))
                ls = processGeometry(geom, demfile, imarray, onlymissing)
                newvalues.append(  (shapely.wkb.dumps(ls), fid) )
                
            imarray = None
            connection.commit()
    
    print("      saving results")
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetTable} 
        set {dbTargetGeom} = st_setsrid(st_geomfromwkb(%s),{srid})
        WHERE {appconfig.dbIdField} = %s
    """
    
    with connection.cursor() as cursor2:
        psycopg2.extras.execute_batch(cursor2, updatequery, newvalues)
            
    connection.commit()
    
    
    
def processGeometry(geom, demfile, demdata, onlymissing):
    
    newpnts = []
    for c in geom.coords:
        x = c[0]
        y = c[1]
        z = c[2]
        newpnts.append(processCoordinate(x, y, z, demfile, demdata, onlymissing))
            
    #make a geometry from newpnts
    ls = shapely.geometry.LineString(newpnts)
    return ls   


def processCoordinate(x, y, z, demfile, demdata, onlymissing):

    #find the dem cell and use that for now
    xindex = floor((x - demfile.xmin) / demfile.xcellsize)
    yindex = demfile.ycnt - floor((y - demfile.ymin) / abs(demfile.ycellsize)) - 1
         
    centerx = xindex * demfile.xcellsize + demfile.xmin + 0.5 * demfile.xcellsize
    centery = (demfile.ycnt - yindex -1) * abs(demfile.ycellsize) + demfile.ymin + 0.5 * abs(demfile.ycellsize)
            
    if ( x < centerx ):
        xindex2 = xindex - 1
    else:
        xindex2 = xindex + 1
                
    if ( y < centery ):
        yindex2 = yindex + 1
    else:
        yindex2 = yindex - 1
     
    
    if (onlymissing == False):
        #if out of range return no data for now - we will go back and deal with 
        #points that require multiple files later
        #often dem files will overlap a bit so this edge will
        #be processed by another area
        if (xindex < 0 or xindex >= demfile.xcnt or yindex < 0 or yindex >= demfile.ycnt  or
            xindex2 < 0 or xindex2 >= demfile.xcnt or yindex2 < 0 or yindex2 >= demfile.ycnt ): 
            return [x, y, z]
   
    
    x1 = xindex * demfile.xcellsize + demfile.xmin + 0.5 * demfile.xcellsize
    x2 = xindex2 * demfile.xcellsize + demfile.xmin + 0.5 * demfile.xcellsize
    y1 = (demfile.ycnt - yindex -1) * abs(demfile.ycellsize) + demfile.ymin + 0.5 * abs(demfile.ycellsize)
    y2 =  (demfile.ycnt - yindex2 -1) * abs(demfile.ycellsize) + demfile.ymin + 0.5 * abs(demfile.ycellsize)
    
    if (xindex >= 0 and yindex >= 0 and xindex < demfile.xcnt and yindex < demfile.ycnt):
        zx1y1 = demdata[yindex][xindex]
    else:
        zx1y1 = findElevation(x1, y1)
        
    if (xindex2 >= 0 and yindex >= 0 and xindex2 < demfile.xcnt and yindex < demfile.ycnt):
        zx2y1 = demdata[yindex][xindex2]
    else:
        zx2y1 = findElevation(x2, y1)
        
    if (xindex2 >= 0 and yindex2 >= 0 and xindex2 < demfile.xcnt and yindex2 < demfile.ycnt):
        zx2y2 = demdata[yindex2][xindex2]
    else:
        zx2y2 = findElevation(x2, y2)
        
    if (xindex >= 0 and yindex2 >= 0 and xindex < demfile.xcnt and yindex2 < demfile.ycnt):    
        zx1y2 = demdata[yindex2][xindex]
    else:
        zx1y2 = findElevation(x1, y2)
                
    if (zx1y1 == appconfig.NODATA or zx1y2 == appconfig.NODATA or zx2y2 == appconfig.NODATA or zx1y2 == appconfig.NODATA):
        #no data for this points
        return [x,y,z]
               
    if (zx1y1 == demfile.nodata or zx2y1 == demfile.nodata or
        zx2y1 == demfile.nodata or zx2y2 == demfile.nodata ):
        #not enough data to determine
        return [x, y, appconfig.NODATA]
        
    #bilinear interpolation of  elevation
    fxy1 = ((x2 - x) / (x2- x1))*zx1y1 + ((x - x1)/(x2 - x1))*zx2y1
    fxy2 = ((x2 - x) / (x2- x1))*zx1y2 + ((x - x1)/(x2 - x1))*zx2y2
    fxy = ((y2 - y) / (y2 - y1))*fxy1 + ((y - y1)/(y2 - y1))*fxy2
        
    return [x,y,fxy]


def findElevation(x, y):
    #search through all dem files for elevation at that point
    #determine by dropping coordinate into dem; should be centered if all dem's are the same
    #but if not won't worry about it for these purposes
    for demfile in demfiles:
        if (demfile.xmin <= x and demfile.xmax >= x and demfile.ymin <= y and demfile.ymax >= y ):
            #read file
            xindex = floor((x - demfile.xmin) / demfile.xcellsize)
            yindex = demfile.ycnt - floor((y - demfile.ymin) / abs(demfile.ycellsize)) - 1
            
            memmap_image = tif.memmap(demfile.filename)
            z = memmap_image[yindex][xindex]
            del memmap_image
            return z
    
    return appconfig.NODATA    

#--- main program ---
def main(demfiles):
    
    with appconfig.connectdb() as conn:
        
        prepareOutput(conn)
        
        watershed_id = getWatershedIds(conn)
    
        #demfiles = indexDem()
        
        #process each dem file
        print("Computing Elevations")
        for demfile in demfiles:
            processArea(demfile, conn, watershed_id)
    
        #search for any missing coordinates that may require 
        #multiple dem files to compute
        #if we have one giant dem file then ignore this
        if (len(demfiles) > 1):
            print ("  computing overlap areas")
            for demfile in demfiles:
                print(demfile.filename)
                processArea(demfile, conn, watershed_id, True)
                

    print("done")

##--MAIN PROGRAM FOR WHEN SRID HANDLING IS NEEDED
# def main(demfiles=None, default_srid=None, assign_crs=False):
#     """
#     Main function with CRS handling options.
    
#     Args:
#         demfiles: Pre-loaded DEM files (optional)
#         default_srid: Default SRID for undefined CRS files
#         assign_crs: Whether to create new files with CRS assigned
#     """
    
#     with appconfig.connectdb() as conn:
        
#         prepareOutput(conn)
        
#         watershed_id = getWatershedIds(conn)
    
#         # Index DEM files if not provided
#         if demfiles is None:
#             demfiles = indexDem(default_srid, assign_crs)
        
#         #process each dem file
#         print("Computing Elevations")
#         for demfile in demfiles:
#             processArea(demfile, conn, watershed_id)
    
#         #search for any missing coordinates that may require 
#         #multiple dem files to compute
#         #if we have one giant dem file then ignore this
#         if (len(demfiles) > 1):
#             print ("  computing overlap areas")
#             for demfile in demfiles:
#                 print(demfile.filename)
#                 processArea(demfile, conn, watershed_id, True)
                

#     print("done")

if __name__ == "__main__":
    main()     