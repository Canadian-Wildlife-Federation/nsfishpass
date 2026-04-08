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
# Assumes stream network forms a tree where ever node has 0 or 1 out nodes
# Assume - data projection is m length projection or else need to modify how length is computed
# Requires stream name field, in this field a value of UNNAMED represents no-name
#
# In addition to computing vertex and segment gradient it also computes the
# maximum vertex gradient for the stream segment
# 
#DESCRIPTION
# 
# This script calculates the overall gradient for entire stream segments. It determines the average steepness of each complete stream segment by comparing its starting and ending elevations. The script operates in two main steps.
# 1.	Add a gradient column: Creates a new field in the stream table to store the calculated gradient values (if it doesn't already exist).
# 2.	Calculates segment gradient: For each stream segment, it: 
#       1.	Takes the elevation at the first point (downstream end)
#       2.	Takes the elevation at the last point (upstream end)
#       3.	Calculates the elevation difference
#       4.	Divides by the total length of the segment to get the gradient
# 
import appconfig

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']
dbSmoothedGeomField = appconfig.config['ELEVATION_PROCESSING']['smoothedgeometry_field']


def computeSegmentGradient(connection):

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {dbSegmentGradientField} double precision;
        
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
        SET {dbSegmentGradientField} = (ST_Z (ST_PointN ({dbSmoothedGeomField}, 1)) - ST_Z (ST_PointN ({dbSmoothedGeomField}, -1))) / ST_Length ({dbSmoothedGeomField})
    """
    #print (query)
    with connection.cursor() as cursor:
        cursor.execute(query)    
            
    connection.commit()



def main():
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Segment Gradient")
        
        print("  computing vertex gradients")
        computeSegmentGradient(conn)
        
        
    print("done")

if __name__ == "__main__":
    main() 
