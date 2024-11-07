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
# This script creates the barrier_passability_view table which shows all dams, waterfalls, and modelled crossings along
# with their passability
#
# This script also creates the natural_barriers_vw which shows all waterfalls and gradient barriers along with their passabilities
#
#

import appconfig

iniSection = appconfig.iniSection
dbTargetSchema = appconfig.dbOutputSchema
watershed_id = appconfig.watershed_id

dbBarrierTable = appconfig.dbBarrierTable
dbPassabilityTable = appconfig.dbPassabilityTable

def build_views(conn):
    # create view combining barrier and passability table
    # programmatically build columns, joins, and conditions based on species in species table

    if iniSection == 'cmm':
        wcrp = 'st_croix'
    else:
        wcrp = iniSection

    specCodes = appconfig.getSpecies()


    query = f""" 
        DROP VIEW IF EXISTS {dbTargetSchema}.barrier_passability_view CASCADE; 
        DROP VIEW IF EXISTS {dbTargetSchema}.natural_barriers_vw;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    cols = []
    nat_cols = []
    joinString = ''
    nat_joinstring = ''
    conditionString = ''

    ## This loop builds the condition with all joins for each species
    # This way, the passability columns for each species for each barrier will be in the 
    # view along with stats.
    # This also joins the ranking tables for each species so they can be viewed in one table
    for i in range(len(specCodes)):
        code = specCodes[i]
        col = f"""
        b.func_upstr_hab_{code},
        b.total_upstr_hab_{code},
        r{i}.w_func_upstr_hab_{code},
        r{i}.w_total_upstr_hab_{code},
        r{i}.group_id as group_id_{code},
		r{i}.num_barriers_group as num_barriers_group_{code},
		r{i}.downstr_group_ids as downstr_group_ids_{code},
		r{i}.total_hab_gain_group as total_hab_gain_group_{code},
		r{i}.w_total_hab_gain_group as w_total_hab_gain_group_{code},
		r{i}.avg_gain_per_barrier as avg_gain_per_barrier_{code},
		r{i}.w_avg_gain_per_barrier as w_avg_gain_per_barrier_{code},
		r{i}.rank_w_avg_gain_tiered as rank_w_avg_gain_tiered_{code},
		r{i}.rank_w_total_upstr_hab as rank_w_total_upstr_hab_{code},
		r{i}.rank_combined as rank_combined_{code}
        """

        pass_col = f"""
        p{i}.passability_status AS passability_status_{code}
        """

        cols.append(col)
        cols.append(pass_col)
        nat_cols.append(pass_col)

        pass_join = f'JOIN {dbTargetSchema}.{dbPassabilityTable} p{i} ON b.id = p{i}.barrier_id\n'
        rank_join = f'LEFT JOIN {dbTargetSchema}.ranked_barriers_{code}_{wcrp} r{i} ON b.id = r{i}.id\n'
        species_join = f'JOIN {dbTargetSchema}.fish_species f{i} ON f{i}.id = p{i}.species_id\n'

        joinString = joinString + pass_join + rank_join + species_join
        nat_joinstring = nat_joinstring + pass_join + species_join

        if i == 0:
            conditionString = conditionString + f'f{i}.code = \'{code}\'\n'
        else:
            conditionString = conditionString + f'AND f{i}.code = \'{code}\'\n' 
    colString = ','.join(cols)
    nat_colString = ','.join(nat_cols)

    query = f"""
        CREATE VIEW {dbTargetSchema}.barrier_passability_view AS 
        SELECT 
            b.id,
            b.cabd_id,
            b.modelled_id,
            b.update_id,
            b.original_point,
            b.snapped_point,
            b.name,
            b.type,
            b.owner,

            b.dam_use,

            b.fall_height_m,

            b.stream_name,
            b.strahler_order,
            b.wshed_name,
            b.secondary_wshed_name,
            b.transport_feature_name,

            b.critical_habitat,
            
            b.crossing_status,
            b.crossing_feature_type,
            b.crossing_type,
            b.crossing_subtype,
            
            b.culvert_number,
            b.structure_id,
            b.date_examined,
            b.road,
            b.culvert_type,
            b.culvert_condition,
            b.action_items, 
            b.passability_status_notes,
            {colString}
        FROM {dbTargetSchema}.{dbBarrierTable} b
        {joinString}
        WHERE {conditionString};
    """

    # print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    query = f"""
        CREATE VIEW {dbTargetSchema}.natural_barriers_vw AS
        with gradients as (
            select b.id, b.type, b.point
            from {dbTargetSchema}.break_points b
            where type = 'gradient_barrier'
        ), falls as (
            select w.id, w.type, w.snapped_point as point
            from {dbTargetSchema}.barriers w
            where w.type = 'waterfall'
        ), nat_barriers as (
            SELECT *
            FROM gradients
            UNION ALL
            SELECT *
            FROM falls
        )
        SELECT 
            b.*,
            {nat_colString}
        FROM nat_barriers b
        {nat_joinstring}
        WHERE {conditionString};
    """

    # print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    query = f"""
        select join_tracking_table_crossings_vw(%s, %s);
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (iniSection, specCodes))
    conn.commit()



def main():
    with appconfig.connectdb() as conn:
        conn.autocommit = False
        
        print("Building Views")
        build_views(conn)

    print("done")

if __name__ == "__main__":
    main()   