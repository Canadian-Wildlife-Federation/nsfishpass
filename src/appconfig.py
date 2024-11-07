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
# This script parses the config file and connects to the database.
# Other scripts use the ConfigParser from this module to parse the config file
#

import configparser
import os
import psycopg2 as pg2
import psycopg2.extras
import enum 
import argparse
import getpass

NODATA = -999999

#users can optionally specify a configuration file
configfile = "config.ini"

parser = argparse.ArgumentParser(description='Process habitat modelling for watershed.')
parser.add_argument('-c', type=str, help='the configuration file', required=False)
parser.add_argument('args', type=str, nargs='*')
args = parser.parse_args()
if (args.c):
    configfile = args.c

iniSection = args.args[0]   

config = configparser.ConfigParser()
config.read(configfile)

# Environment variables
ogr = config['OGR']['ogr']
proj = config['OGR']['proj']; 
gdalinfo = config['OGR']['gdalinfo']
gdalsrsinfo = config['OGR']['gdalsrsinfo']

# Connection info
dbHost = config['DATABASE']['host']
dbPort = config['DATABASE']['port']
dbName = config['DATABASE']['name']
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

# Files to load raw data and info for wcrp set up
dataSchema = config['DATABASE']['data_schema']
streamTable = config['DATABASE']['stream_table']
streamTableDischargeField = "discharge"
streamTableChannelConfinementField = "channel_confinement"
fishSpeciesTable = config['DATABASE']['fish_species_table']

dataSrid = config['DATABASE']['working_srid']  

dbIdField = "id"
dbGeomField = "geometry"
dbWatershedIdField = "watershed_id"

watershedTable = config['CREATE_LOAD_SCRIPT']['watershed_table']
tidalZones = config['CREATE_LOAD_SCRIPT']['tidal_zones']
fish_parameters = config['DATABASE']['fish_parameters']

# WCRP specific configuration parameters
dbOutputSchema = config[iniSection]['output_schema']
dbBarrierTable = config['BARRIER_PROCESSING']['barrier_table']
dbPassabilityTable = config['BARRIER_PROCESSING']['passability_table']
species = config[iniSection]['species']

watershed_id = config[iniSection]['watershed_id']

class Accessibility(enum.Enum):
    ACCESSIBLE = 'CONNECTED NATURALLY ACCESSIBLE WATERBODIES'
    POTENTIAL = 'DISCONNECTED NATURALLY ACCESSIBLE WATERBODIES'
    NOT = 'NATURALLY INACCESSIBLE WATERBODIES'


print(f"""--- Configuration Settings Begin ---
Database: {dbHost}:{dbPort}:{dbName}:{dbUser}
OGR: {ogr}
SRID: {dataSrid}
Raw Data Schema: {dataSchema}
--- Configuration Settings End ---
""")   

#if you have multiple version of proj installed
#you might need to set this to match gdal one
#not always required
if (proj != ""):
    os.environ["PROJ_LIB"] = proj

psycopg2.extras.register_uuid()

def connectdb():
    return pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

def getSpecies():
    """
    Format the species in the config file into an array of strings
    :returns: an array containing the species of interest
    """
    return [substring.strip() for substring in species.split(',')]