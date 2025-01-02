
# Watershed Processing (Running the model)

Processing is completed by watershed id. Each watershed is processed into a separate schema in the database. The watershed configuration must be specified in the ini file and the configuration to be used provided to the script (see below).

Currently processing includes:
* Load fish species parameters - contains the species of interest and various modelling parameters. Before processing the watershed, these parameters should be reviewed and configured as necessary.
* Preprocessing step which loads all the streams from the raw datastore into the working schema
* Load barriers from the CABD barrier database
* Load and snap fish stocking and observation data to stream network
* Compute modelled crossings
* Load assessment data
* Compute mainstems
* Compute an elevation value for all stream segments
* Compute a smoothed elevation value for all stream segments
* Compute gradient for each stream vertex based on vertex elevation and elevation 100m upstream.
* Break stream segments at required locations
* Reassign raw elevation and smoothed elevation to broken stream segments
* Compute segment gradient based on start, end elevation and length
* Compute upstream/downstream statistics for stream network, including number of barriers, fish stocking species and fish survey species
* Compute accessibility models based on stream gradient and barriers
* Compute habitat models
* Compute upstream/downstream statistics for barriers
* Load barrier updates
* Load habitat accessibility updates
* Process habitat accessibility updates
* Load aquatic invasive species
* Rank barriers
* Create views

#  Individual Processing Scripts

These scripts are the individual processing scripts that are used for the watershed processing steps.

---
#### 0 - Configuring Fish Species Model Parameters

As a part of the loading scripts a fish species table is created which contains the fish species of interest for modelling and various modelling parameters. Before processing the watershed these parameters should be reviewed and configured as necessary.

Note: Currently there is no velocity or channel confinement data. These parameters are placeholders for when this data is added.  

**Script**  

load_parameters.py [dataFile] -c config.ini

---
#### 1 - Preprocessing

This script creates required database schemas and loads stream data for the watershed into a working table in this schema.

**Script**

preprocess_watershed.py -c config.ini [watershedid]

**Input Requirements**

* Raw stream network dataset loaded


**Output**

* A database schema named after the watershed id
* A streams table in this schema populated with all streams from the raw dataset

---
#### 2 - Loading Barriers
This script loads dam barriers from the CABD API where use_analysis = true.  

By default, the script uses the nhn_watershed_id from config.ini for the subject watershed(s) to retrieve features from the API. You can have multiple nhn_watershed_ids specified in the config file as long as they are formatted as a list, e.g., ["01dd000","01de000","01df000"].

**Process**
The Script first drops views created in barrier_passability_view.py since those views depend on tables created in this script.

The fish species table is loaded into the wcrp schema.

The barriers table is created which will hold dams and waterfalls pulled from the CABD along with modelled crossings which will be generated in a later script.

The waterfalls table is created which will only hold waterfalls. In the future, this will be the only place for waterfalls and they will be separated out of barriers but waterfalls are also loaded into barriers since all the calculations work out with the barriers table.

The passability table is created. This table stores the passability for each species for each barrier. This means that each barrier has a record in this table for each species which tracks the passability of that species for that barrier. For example, if there are two species of interest in the watershed, each barrier will have two records in the table.

The CABD API is queried to get dam data which is loaded into the barriers table.

The CABD API is queried to get waterfall data which is loaded into the barriers table and the waterfalls table.

Barriers in the barriers table are snapped to the stream network.
Waterfalls are snapped to the stream network in the waterfalls table.

Secondary watershed values are defined. By default, this is the same as the wcrp primary watershed name. For CMM, there are 3 secondary watersheds necessitating this part of the script.

Passability values are loaded into the passability table. In the CABD, passability is assigned with a string value 'BARRIER' or 'PASSABLE'. This is translated into 1 and 0 respectively. For waterfalls, the height thresholds are taken from the fish_species table and the passability is assigned by species based on these for each waterfall. 

Passability column is dropped from the barriers table since the info is now in the barrier_passability table.

**Script**

load_and_snap_barriers_cabd.py -c config.ini [watershedid]

**Input Requirements**

* Access to the CABD API
* Streams table populated from the preprocessing step 

**Output**

* A new barrier table populated with dam barriers from the CABD API
* Fish species table with information on each species of interest
* A waterfalls table populated with waterfalls from the CABD API
* A barrier_passability table populated with the passability of each barrier for each species (one entry for each barrier-species pair)
* The barrier table has two geometry fields - the raw field and a snapped field (the geometry snapped to the stream network). The maximum snapping distance is specified in the configuration file.

---
#### 3 - Load and snap fish observation data

NOT USED IN NSFISHPASS

Loads fish observation or habitat data provided and snaps it to the stream network. This data can be used by later scripts to override accessibility or habitat calculations. For instance, you can mark a stream segment as accessible because fish have been observed there, even if the accessibility model would have marked that segment as inaccessible.

**Script**

load_and_snap_fishobservations.py -c config.ini [watershedid]

**Input Requirements**

* fish observation or habitat data
* stream network

**Output**

* addition of one table: habitat_data


---
#### 4 - Compute Modelled Crossings
This script computes modelled crossings defined as locations where roads, railways, or trails cross stream networks (based on feature geometries). Due to precision and accuracy of the input datasets, not all crossings may not actually exist on the ground.   

The modelled_id field for modelled crossings is a stable id. The second and all subsequent runs of compute_modelled_crossings.py will create an archive table of previous modelled crossings, and assign the modelled_id for newly generated crossings to their previous values, based on a distance threshold of 10 m. If the modelled crossings table is ever dropped (without an archive created), then modelled_ids will be regenerated.    

The matchArchive function can be modified to keep other attributes stable as well, but most other attributes on crossings should be retrieved from assessment records and other comments in the barrier_updates table, which is loaded and mapped in a later script.

**Script**

compute_modelled_crossings.py -c config.ini [watershedid]

**Input Requirements**

* Streams table populated from the preprocessing step
* Road, railway, and trail data loaded from the load_data script

**Output**

* A new modelled crossings table with a reference to the stream edge the crossing crosses and information from the intersecting transportation network.
* Modelled crossings are added to the barriers table.


---
#### 5 - Load Barrier Updates
This script loads barrier updates for the watershed, joins it with modelled crossings based on the join_distance parameter in config.ini, loads the joined crossings to a crossings table, then loads these crossings to the barriers table.

**Script**

load_barrier_updates.py -c config.ini [watershedid]

**Input Requirements**

* Streams table
* Modelled crossings table
* Barriers table

**Output**

* A new barrier updates table which contains all barrier updates for the watershed
* Updated barriers table that now includes information from the barrier updates table
 
---
#### 6 - Mainstems

This script computes mainstems based on names of streams and longest upstream length.

**Script**

compute_mainstems.py -c config.ini [watershedid]

**Input Requirements**

* Streams table

**Output**

* A new field, mainstem_id, downstream_route_measure and upstream_route_measure, added to the input table. The measure fields are calculated in km.


---
#### 7 - Assign Raw Z Value

This script drapes a stream network over provided DEMs and computes a rawz value for each vertex in the stream network.

**Script**

assign_raw_z.py -c config.ini [watershedid]

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution. The scripts assume this data is in an equal length projection so the st_length2d(geometry) function returns the length in metres. This should be specified in the config file under [ELEVATION_PROCESSING] as the dem_directory variable.    
* Streams table populated from the preprocessing step

**Output**

* A geometry_raw3d field added to the stream table that represents the 3d geometry for the segment

---
#### 8 - Compute Smoothed Z Value

This script takes a set of stream edges with raw z values and smoothes them so that the streams are always flowing downhill.

**Script**

smooth_z.py -c config.ini [watershedid]

**Input Requirements**

* Streams table with id and geometry_raws3d fields (output from the raw z processing)

**Output**

* A new field, geometry_smoothed3d, added to the input table

---
#### 9 - Compute Vertex Gradients

For every stream vertex, this script takes the elevation at that point and the elevation along the mainstem at a point 100 m upstream and computes the gradient based on those two elevations.

**Script**

compute_vertex_gradient.py -c config.ini [watershedid]

**Input Requirements**

* Streams table with smoothed elevation values

**Output**

* A new table (vertex_gradients) with a single point for every vertex with a gradient calculated. This table includes both the vertex geometry, upstream geometry and elevation values at both those locations

---
#### 10 - Break Streams at Barriers

This script breaks the stream network at barriers and recomputes necessary attributes. 

For this script, a barrier is considered to be: a CABD barrier (dams), all stream crossings, and all gradient barriers (gradients greater than the minimum value specified in the accessibility_gradient field in the fish_species table).  
A list of gradient barriers can be found in the output break_points table (type = gradient_barrier). Streams are broken at all barriers regardless of passability status.

**Script**

break_streams_at_barriers.py -c config.ini [watershedid]

**Input Requirements**

* Streams table with smoothed elevation values

**Output**

* a break_points table that lists all the locations where the streams were broken
* updated streams table with mainstem route measures recomputed (in km this time)
* updated barriers table (stream_id is replaces with a stream_id_up and stream_id_down referencing the upstream and downstream edges linked to the point)

---

#### 11 - ReAssign Raw Z Value
Recompute z values again based on the raw data so any added vertices are computed based on the raw data and not interpolated points.

**Script**

assign_raw_z.py -c config.ini [watershedid]

---
#### 12 - ReCompute Smoothed Z Value
Recompute smoothed z values again based on the raw data so any added vertices are computed based on the raw data and not interpolated points.

**Script**

smooth_z.py -c config.ini [watershedid]

---
#### 13 - Compute Segment Gradient

Compute a segment gradient based on the smoothed elevation for the most upstream coordinate, most downstreamm coordinate, and the length of the stream segment

**Script**

compute_segment_gradient.py -c config.ini [watershedid]

**Input Requirements**

* streams table smoothed elevation values

**Output**

* addition of segment_gradient to streams table

---
#### 14 - Compute upstream and downstream barrier and fish species information.

This script computes a number of statistics for each stream segment:
* number of upstream and downstream barriers
* the identifiers of the upstream and downstream barriers
* number of upstream and downstream gradient barriers

Barriers include:
* CABD barriers (dams) where passability status != 1 (i.e., passable)
* Stream crossings where passability status = 1 (i.e., passable)
* Beaver activity where passability status = 1 (i.e., passable)

**Script**

compute_updown_barriers_fish.py -c config.ini [watershedid]

**Input Requirements**

* stream network
* barriers table

**Output**
* addition of statistic fields to stream network table

---
#### 15 - Compute accessibility models

Computes an accessibility value for each fish species for each stream segment based on:
* segment gradient
* maximum accessibility gradient (specified in the fish_species table)
* barrier location

Segments are classified as:
* CONNECTED NATURALLY ACCESSIBLE WATERBODIES - when there are no gradient barriers downstream and there are no impassable barriers downstream 
* DISCONNECTED NATURALLY ACCESSIBLE WATERBODIES - when there are no gradient barriers downstream but there are impassable barriers downstream
* NATURALLY INACCESSIBLE WATERBODIES - when there are gradient barriers downstream

**Script**

compute_accessibility.py -c config.ini [watershedid]

**Input Requirements**

* stream network
* barriers table

**Output**

* addition of an accessibility field to stream network table for each fish species 

---
#### 16 - Compute habitat models

Computes a true/false value for habitat for each species for each stream segment.

Habitat models are currently just based on accessibility - any stream segments that are 'ACCESSIBLE' or 'POTENTIALLY ACCESSIBLE' have been classified as habitat. These models can be refined when there are additional habitat parameters or supplementary habitat data available for the species of interest.

**Script**

assign_habitat.py -c config.ini [watershedid]


**Input Requirements**

* stream network
* fish species parameters

**Output**
* addition of habitat model fields to stream network table for each fish species


---
#### 17 - Compute barrier statistics 

Computes a collection of barrier statistics including:

Statistics by species:
* total potentially accessible upstream distance for each species  
* total upstream habitat for each species
* functional upstream habitat for each species

Functional upstream habitat is calculated by walking up the stream network, summing up length of stream segments where the habitat model = true, stopping at the first barrier encountered (upstream).

 **Script**

compute_barriers_upstream_values.py -c config.ini [watershedid]

**Input Requirements**
* stream network
* barriers

**Output**
* addition of statistic fields to the barriers table

---
#### 18 - Compute barrier DCI

Computes the Dendritic Connectivity Index (DCI) value for each barrier and each species.    

The overall DCI value for each species of interest (i.e., the DCI assuming all barriers remain the same passability) is calculated by discounting (or weighting) the length of each stream segment that contains habitat based on the cumulative passability of all downstream barriers. These discounted lengths are then divided by the total length of habitat for that species in the watershed to produce the DCI value.  

For each species, the DCI for each barrier is then calculated as the effect that barrier would have on the overall DCI value if the barrier was removed. To do this, the barrier's passability value is set to 1 (passable), the overall species DCI is calculated again, and the difference between the two DCI values is set as the DCI for that barrier.

 **Script**

compute_barrier_dci.py -c config.ini [watershedid]

**Input Requirements**
* stream network
* barriers

**Output**
* addition of statistic fields to the barriers table


# Algorithms 
## Draping Algorithm

To compute raw elevation, for each vertex:

1. drop the vertex on the DEM and determine which 4 cells are the nearest to the point. In the example below the four nearest cells to V are A, B, C & D.

2. compute a bilinear interpolated value at this point using the values from cells A, B, C, & D.

<pre>
A = (x1, y2, Az)
B = (x2, y2, Bz)
C = (x1, y1, Cz)
D = (x2, y1, Dz)
V = (x, y, Vz)
    
fxy1 = ((x2 - x) / (x2- x1))*Cz + ((x - x1)/(x2 - x1))*Dz
fxy2 = ((x2 - x) / (x2- x1))*Az + ((x - x1)/(x2 - x1))*Bz
Vz = ((y2 - y) / (y2 - y1))*fxy1 + ((y - y1)/(y2 - y1))*fxy2

+-------------+-------------+
|             |             |
|             |             |
|      A      |      B      |
|             |             |
|             |             |
+-------------+-------------+
|          V  |             |
|             |             |
|      C      |      D      |
|             |             |
|             |             |
+-------------+-------------+
</pre>    

Notes: we assume that the elevation values provided in the DEM represent the elevation at the center point of the cell    





## Smoothing Algorithm

The smoothing process ensures streams always flow down hill.


Notes:
*This algorithm does not contain any spike detection, so if there is an error in the DEM that causes a significant spike in the stream network this will significantly affect the results.
* Nodes and vertices with no elevation values (NODATA), are ignored in the computation of the min/max values.


1. Create a graph of the nodes in the stream network
2. Starting at the sink nodes and walking up the network computing a max_elevation value for each node. This value is the maximum of the node's raw elevation and the downstream node elevation values
3. Starting at the source nodes and walking down the network compute a min_elevation value for each node. This value is the minimum of the node's raw elevation values and the upstream node elevation values.
4. For each node assign a smoothed elevation of the the average of the max_elevation and min_elevation
5. For each edge in the network
  * clip all vertices elevations so they are no smaller or bigger than the z values at the end nodes
  * compute min/max elevations for each vertex then average the results to get smoothed value 

<pre>
 Node  Elevation   Min  Max   Smoothed
  A       12       12   12    12
  B       10       10   10    10
  C       6        6    7     6.5
  D       7        6    7     6.5      
  F       8        8    8     8      
  G       2        2    2     2
  
    A           B 
     \         /
      \       /
       \     /
        C---+
        |      F
        |     / 
        |    /
        D---+
        |
        |
        |
        F
        
</pre>        



## Mainstem Algorithm

Mainstems are computed by starting at the sink node and walking up the network. At any confluence the mainstem is push up the edge that:
1) has the same stream name as the current edge
2) if no edges have the same name then any named edge; if there are multiple named edges it picks the edge with the longest path to a headwater
3) if no named edges; then it  picks the edge with the longest path to a headwater.


---
# Configuration File

[OGR]  
ogr = location of ogr2ogr executable  
gdalinfo = location of gdalinfo executable  
gdalsrsinfo = location of gdalsrsinfo executable   
proj = *optional* location of proj library  
  
[DATABASE]  
host = database host  
port = database post  
name = database name

data_schema = name of main schema for holding raw stream data    
stream_table = names of streams table    
fish_parameters = location of csv file containing data for species of interest (data typically includes gradient etc.)
fish_species_table = name of fish species table    
working_srid = the srid of the stream data. These scripts use the function st_length to compute stream length so the raw data should be in a meters based projection (or reprojected before used). 

[CABD_DATABASE]  
buffer = this is the buffer distance to grab features - the units are in the working_srid so if its meters 200 is reasonable, if it's degrees something like 0.001 is reasonable  
snap_distance = distance (in working srid units) for snapping point features #to the stream network (fish observation data, barrier data etc)  
  
[CREATE_LOAD_SCRIPT]  
raw_data = spatial file containing raw road, trail, and stream data  
road_table = road table name  
trail_table = trail table name  
  
[PROCESSING]  
stream_table = stream table name 

[WATERSHEDID 1] -> there will be one section for each watershed with a unique section name  
watershed_id = watershed id to process. if multiple watersheds should be combined/processed as a single watershed, this can be a list.    
nhn_watershed_id = nhn watershed id(s) to process  
output_schema = output schema name  
fish_observation_data = zip file containing fish observation data (or None if such files does not exist)
habitat_access_updates = spatial file containing updates to habitat or accessibility      
watershed_table = table name containing watershed boundaries
barrier_updates = table name containing barrier updates

watershed_data = spatial file containing watershed boundaries. this can be the same as the raw_data file  
secondary_watershed_table = table containing the secondary watershed boundaries (or None if it does not exist)
tidal_zone_data = path to file containing data for tidal zones (or None)
tidal_zones = table name for tidal zones (or None)

dem_directory = path to directory containing dems for watershed

species = list of species of interest for the watershed

[WATERSHEDID 2] -> there will be one section for each watershed with a unique section name  
watershed_id = watershed id to process. if multiple watersheds should be combined/processed as a single watershed, this can be a list.    
nhn_watershed_id = nhn watershed id(s) to process  
output_schema = output schema name  
fish_observation_data = zip file containing fish observation data (or None if such files does not exist)
habitat_access_updates = spatial file containing updates to habitat or accessibility      
watershed_table = table name containing watershed boundaries
barrier_updates = table name containing barrier updates

watershed_data = spatial file containing watershed boundaries. this can be the same as the raw_data file  
secondary_watershed_table = table containing the secondary watershed boundaries (or None if it does not exist)
tidal_zone_data = path to file containing data for tidal zones (or None)
tidal_zones = table name for tidal zones (or None)

dem_directory = path to directory containing dems for watershed

species = list of species of interest for the watershed
  
[ELEVATION_PROCESSING]   
3dgeometry_field = field name (in streams table) for geometry that stores raw elevation data  
smoothedgeometry_field = field name (in streams table)  for geometry that stores smoothed elevation data  
  
[MAINSTEM_PROCESSING]  
mainstem_id = name of mainstem id field (in streams table)  
downstream_route_measure = name of downstream route measure field  
upstream_route_measure =name  upstream route measure field  
  
[GRADIENT_PROCESSING]  
vertex_gradient_table = table for storing vertex gradient values   
segment_gradient_field = name of segment gradient field (in streams table)  
max_downstream_gradient_field = name of field for storing the maximum downstream segment gradient (in streams table)  
  
[BARRIER_PROCESSING]  
barrier_table = table for storing barriers
gradient_barrier_table = table where gradient barriers are stored (type = gradient_barrier)    
barrier_updates = spatial file containing updates to barriers
  
[CROSSINGS]  
modelled_crossings_table = table for storing modelled crossings    
crossings_table = table for storing all crossings
join_distance = distance (in working srid units) for joining assessment data with modelled crossings


[HABITAT_STATS]  
stats_table = name of the table to be created in the [DATABASE].data_schema schema which will contain watershed statistics

watershed_data_schemas = the list of processing schemas to include in the stats table; the schemas must exist and data must be fully processed
watersheds = list of watershed ids