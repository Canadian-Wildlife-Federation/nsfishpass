# This script generates a query to automatically rank barriers in a watershed
# for Nova Scotia watersheds
# usage: py auto_rank_barriers.py <watershed> <species_code>


import sys
import getpass
import psycopg2 as pg2

# ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
ogr = "C:\\Program Files\\QGIS 3.22.1\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-prod.postgres.database.azure.com"
dbPort = "5432"
dbName = "nsfishpass"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

watershed = sys.argv[1] # avon, halfway, st_croix, or msa
species_code = sys.argv[2] # as or ae

watershed_name = ''
wcrp = ''

if watershed == 'avon':
    watershed_name = 'Avon R.'
    wcrp = 'cmm'
elif watershed == 'halfway':
    watershed_name = 'Halfway R.'
    wcrp = 'cmm'
elif watershed == 'st_croix':
    watershed_name = 'St. Croix R.'
    wcrp = 'cmm'
elif watershed == 'msa':
    watershed_name = watershed
    wcrp = 'msa'
elif watershed == 'cheticamp':
    watershed_name = watershed
    wcrp = 'cheticamp'
else:
    print('INVALID WATERSHED NAME')
    sys.exit()

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)


query = f"""
DROP TABLE IF EXISTS {wcrp}.ranked_barriers_{species_code}_{watershed};

WITH barrier_passability_{species_code} 
AS (
	SELECT bp.barrier_id, bp.passability_status
	FROM {wcrp}.barrier_passability bp
	WHERE bp.species_id = 
		(
			SELECT id 
			FROM {wcrp}.fish_species 
			WHERE code = '{species_code}'
		)
)
SELECT b.id
	,b.name
    ,b.type
    ,b.owner
    ,b.passability_status_notes
    ,b.dam_use
    ,b.stream_name
    ,b.strahler_order
    ,b.wshed_name
    ,b.secondary_wshed_name
    ,b.crossing_status
    ,b.crossing_feature_type
    ,b.culvert_number
    ,b.structure_id
    ,b.date_examined
    ,b.road
    ,b.culvert_type
    ,b.culvert_condition
    ,b.barrier_cnt_upstr_{species_code}
    ,b.barriers_upstr_{species_code}
    ,b.barrier_cnt_downstr_{species_code}
    ,b.barriers_downstr_{species_code}
    ,b.total_upstr_hab_all
    ,b.func_upstr_hab_all
    ,b.dci_{species_code}
    ,b.original_point
    ,b.snapped_point
    ,b.stream_id_up
    ,b.func_upstr_hab_{species_code} 
    ,b.total_upstr_hab_{species_code}
    ,b.w_func_upstr_hab_{species_code} * (1 - passability_status::double precision) as w_func_upstr_hab_{species_code}
    ,b.w_total_upstr_hab_{species_code} * (1 - passability_status::double precision) as w_total_upstr_hab_{species_code}
	,bp.passability_status INTO {wcrp}.ranked_barriers_{species_code}_{watershed}
FROM {wcrp}.barriers b
JOIN barrier_passability_{species_code} bp
	ON bp.barrier_id = b.id
WHERE b.secondary_wshed_name = '{watershed_name}'
	AND bp.passability_status != '1' 
	AND b.total_upstr_hab_{species_code} != 0
    AND b.type != 'waterfall'
ORDER BY dci_{species_code} DESC;

ALTER TABLE IF EXISTS {wcrp}.ranked_barriers_{species_code}_{watershed}
    ALTER COLUMN id SET NOT NULL;
ALTER TABLE IF EXISTS {wcrp}.ranked_barriers_{species_code}_{watershed}
    ADD COLUMN group_id numeric;
ALTER TABLE IF EXISTS {wcrp}.ranked_barriers_{species_code}_{watershed}
    ADD PRIMARY KEY (id);
"""

with conn.cursor() as cursor:
	cursor.execute(query)
conn.commit()
 
query = f"""
--TO FIX: some group_ids need to get combined - e.g., multiple branches of river
ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed} ADD COLUMN IF NOT EXISTS mainstem_id uuid;
UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed} SET mainstem_id = t.mainstem_id FROM {wcrp}.streams t WHERE t.id = stream_id_up;

CREATE INDEX ranked_barriers_{species_code}_{watershed}_idx_mainstem ON {wcrp}.ranked_barriers_{species_code}_{watershed} (mainstem_id);
CREATE INDEX ranked_barriers_{species_code}_{watershed}_idx_group_id ON {wcrp}.ranked_barriers_{species_code}_{watershed} (group_id);
CREATE INDEX ranked_barriers_{species_code}_{watershed}_idx_id ON {wcrp}.ranked_barriers_{species_code}_{watershed} (id);

WITH mainstems AS (
SELECT DISTINCT mainstem_id, row_number() OVER () AS group_id
FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
)

UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed} a SET group_id = m.group_id FROM mainstems m WHERE m.mainstem_id = a.mainstem_id;
"""

with conn.cursor() as cursor:
	cursor.execute(query)
conn.commit()
	
query = f"""
DO $$
DECLARE
	continue_loop BOOLEAN := TRUE;
	i INT := 1;
	grp_offset INT := (SELECT COUNT(*)*10 FROM {wcrp}.ranked_barriers_{species_code}_{watershed});
BEGIN
	WHILE continue_loop LOOP
		
		i := i + 1;
	
		perform id, group_id
		from {wcrp}.ranked_barriers_{species_code}_{watershed}
		where group_id < grp_offset
		LIMIT 1;

		IF NOT FOUND THEN
			continue_loop := FALSE;
		ELSE
			with avgVals as (
				select id, mainstem_id, group_id, barrier_cnt_upstr_{species_code}, func_upstr_hab_{species_code}
					,AVG(w_func_upstr_hab_{species_code}) OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_{species_code} DESC) as average
					,ROW_NUMBER() OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_{species_code} DESC) as row_num
				from {wcrp}.ranked_barriers_{species_code}_{watershed}
				where group_id < grp_offset
				order by group_id, barrier_cnt_upstr_{species_code} DESC
			),
			max_grp_gain as (
				select 
					group_id
					,max(average) as best_gain
				from avgVals
				group by group_id
				order by group_id
			),
			part as (
				select mx.*, av.row_num 
				from max_grp_gain mx
				join avgVals av on mx.best_gain = av.average
			),
			new_grps as (
				select av.id
					,CASE 
						WHEN av.row_num <= part.row_num THEN (av.group_id*grp_offset) + i
						ELSE av.group_id
					END as new_group_id
				from avgVals av
				join part on av.group_id = part.group_id
			)

			update {wcrp}.ranked_barriers_{species_code}_{watershed}
			set group_id = new_grps.new_group_id
			from new_grps
			where {wcrp}.ranked_barriers_{species_code}_{watershed}.id = new_grps.id
			AND new_grps.new_group_id > grp_offset;

		END IF;
	END LOOP;
END $$;

select id, mainstem_id, group_id, barrier_cnt_upstr_{species_code}, func_upstr_hab_{species_code}
	,AVG(w_func_upstr_hab_{species_code}) OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_{species_code} DESC) as average
	,ROW_NUMBER() OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_{species_code} DESC) as row_num
from {wcrp}.ranked_barriers_{species_code}_{watershed}
order by group_id, barrier_cnt_upstr_{species_code} DESC;


	
----------------- CALCULATE GROUP GAINS -------------------------	
	
alter table {wcrp}.ranked_barriers_{species_code}_{watershed} add column total_hab_gain_group numeric;
alter table {wcrp}.ranked_barriers_{species_code}_{watershed} add column w_total_hab_gain_group numeric;
alter table {wcrp}.ranked_barriers_{species_code}_{watershed} add column num_barriers_group integer;
alter table {wcrp}.ranked_barriers_{species_code}_{watershed} add column avg_gain_per_barrier numeric;
alter table {wcrp}.ranked_barriers_{species_code}_{watershed} add column w_avg_gain_per_barrier numeric;

with temp as (
	SELECT 
    	sum(w_func_upstr_hab_{species_code}) AS w_sum
        ,sum(func_upstr_hab_{species_code}) AS sum
        ,group_id
	from {wcrp}.ranked_barriers_{species_code}_{watershed}
	group by group_id
)

update {wcrp}.ranked_barriers_{species_code}_{watershed} a 
SET 
	total_hab_gain_group = t.sum
	,w_total_hab_gain_group = t.w_sum 
FROM temp t 
WHERE t.group_id = a.group_id;

update {wcrp}.ranked_barriers_{species_code}_{watershed} SET total_hab_gain_group = func_upstr_hab_{species_code} WHERE group_id IS NULL;
update {wcrp}.ranked_barriers_{species_code}_{watershed} SET w_total_hab_gain_group = w_func_upstr_hab_{species_code} WHERE group_id IS NULL;

with temp as (
	SELECT count(*) AS cnt, group_id
	from {wcrp}.ranked_barriers_{species_code}_{watershed}
	group by group_id
)


update {wcrp}.ranked_barriers_{species_code}_{watershed} a SET num_barriers_group = t.cnt FROM temp t WHERE t.group_id = a.group_id;
update {wcrp}.ranked_barriers_{species_code}_{watershed} SET num_barriers_group = 1 WHERE group_id IS NULL;

update {wcrp}.ranked_barriers_{species_code}_{watershed} SET avg_gain_per_barrier = total_hab_gain_group / num_barriers_group;
update {wcrp}.ranked_barriers_{species_code}_{watershed} SET w_avg_gain_per_barrier = w_total_hab_gain_group / num_barriers_group;

---------------GET DOWNSTREAM GROUP IDs----------------------------

ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed} ADD downstr_group_ids varchar[];

WITH downstr_barriers AS (
	SELECT rb.id, rb.group_id
		,UNNEST(barriers_downstr_{species_code}) AS barriers_downstr_{species_code}
	FROM {wcrp}.ranked_barriers_{species_code}_{watershed} rb
),
downstr_group AS (
	SELECT db_.id, db_.group_id as current_group, db_.barriers_downstr_{species_code}
		,rb.group_id
	FROM downstr_barriers AS db_
	JOIN {wcrp}.ranked_barriers_{species_code}_{watershed} rb
		ON rb.id = db_.barriers_downstr_{species_code}::uuid
	WHERE db_.group_id != rb.group_id
), 
dg_arrays AS (
	SELECT dg.id, ARRAY_AGG(DISTINCT dg.group_id)::varchar[] as downstr_group_ids
	FROM downstr_group dg
	GROUP BY dg.id
)
UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed}
SET downstr_group_ids = dg_arrays.downstr_group_ids
FROM dg_arrays
WHERE {wcrp}.ranked_barriers_{species_code}_{watershed}.id = dg_arrays.id;


----------------- ASSIGN RANK ID  -------------------------	

-- Rank based on first sorting the barriers into tiers by number of downstream barriers then by avg gain per barrier within those tiers (immediate gain)
ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed} 
ADD rank_w_avg_gain_tiered numeric;

WITH sorted AS (
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, w_total_hab_gain_group, w_avg_gain_per_barrier
		,passability_status
		,ROW_NUMBER() OVER(ORDER BY barrier_cnt_downstr_{species_code}, w_avg_gain_per_barrier DESC) as row_num
	FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
	WHERE w_avg_gain_per_barrier >= 0.5
	UNION ALL
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, w_total_hab_gain_group, w_avg_gain_per_barrier
		,passability_status
		,(SELECT MAX(row_num) FROM (
			SELECT ROW_NUMBER() OVER(ORDER BY barrier_cnt_downstr_{species_code}, w_avg_gain_per_barrier DESC) as row_num
			FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
			WHERE w_avg_gain_per_barrier >= 0.5
		) AS subquery) + ROW_NUMBER() OVER(ORDER BY barrier_cnt_downstr_{species_code}, w_avg_gain_per_barrier DESC) as row_num 
	FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
	WHERE w_avg_gain_per_barrier < 0.5
	
),
ranks AS (
	SELECT id
		,FIRST_VALUE(row_num) OVER(PARTITION BY group_id ORDER BY barrier_cnt_downstr_{species_code}) as ranks
		,FIRST_VALUE(barrier_cnt_downstr_{species_code}) OVER (PARTITION BY group_id ORDER BY barrier_cnt_downstr_{species_code}) as tier
	FROM sorted
	ORDER BY group_id, barrier_cnt_downstr_{species_code}, w_avg_gain_per_barrier DESC
)
UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed} 
SET rank_w_avg_gain_tiered = ranks.ranks
FROM ranks
WHERE {wcrp}.ranked_barriers_{species_code}_{watershed}.id = ranks.id;

-- Rank based on total habitat upstream (potential gain)
ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed} 
ADD rank_w_total_upstr_hab numeric;

WITH sorted AS (
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, w_total_upstr_hab_{species_code}, w_total_hab_gain_group, w_avg_gain_per_barrier
		,ROW_NUMBER() OVER(ORDER BY w_total_upstr_hab_{species_code} DESC) as row_num
	FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
),
ranks AS (
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, w_total_upstr_hab_{species_code}, w_total_hab_gain_group, w_avg_gain_per_barrier
		,FIRST_VALUE(row_num) OVER(PARTITION BY group_id ORDER BY row_num) as relative_rank
	FROM sorted
	ORDER BY group_id, barrier_cnt_downstr_{species_code}, w_avg_gain_per_barrier DESC
),
densify AS (
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, w_total_upstr_hab_{species_code}, w_total_hab_gain_group, w_avg_gain_per_barrier
		,DENSE_RANK() OVER(ORDER BY relative_rank) as ranks
	FROM ranks
)
UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed} 
SET rank_w_total_upstr_hab = densify.ranks
FROM densify
WHERE {wcrp}.ranked_barriers_{species_code}_{watershed}.id = densify.id;

-- Composite Rank of potential and immediate gain with upstream habitat cutoff
ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed} 
ADD rank_combined numeric;

WITH ranks AS (
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, total_upstr_hab_{species_code}, total_hab_gain_group, avg_gain_per_barrier
		,rank_w_avg_gain_tiered
		,rank_w_total_upstr_hab
		,DENSE_RANK() OVER(ORDER BY rank_w_avg_gain_tiered + rank_w_total_upstr_hab, group_id ASC) as rank_composite
	FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
	ORDER BY rank_composite ASC
)
UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed}
SET rank_combined = ranks.rank_composite
FROM ranks
WHERE {wcrp}.ranked_barriers_{species_code}_{watershed}.id = ranks.id;

-- Potential and immediate with weight by downstream barriers
ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed} 
ADD tier_combined varchar;

WITH ranks AS (
	SELECT id, group_id, barrier_cnt_upstr_{species_code}, barrier_cnt_downstr_{species_code}, w_total_upstr_hab_{species_code}, w_total_hab_gain_group, w_avg_gain_per_barrier
		,rank_w_avg_gain_tiered
		,rank_w_total_upstr_hab
		,rank_combined
		,DENSE_RANK() OVER(ORDER BY LEAST(rank_w_avg_gain_tiered, rank_w_total_upstr_hab), group_id ASC) as rank_composite
	FROM {wcrp}.ranked_barriers_{species_code}_{watershed}
)
UPDATE {wcrp}.ranked_barriers_{species_code}_{watershed}
SET tier_combined = case
			when r.rank_combined <= 10 then 'A'
			when r.rank_combined <= 20 then 'B'
			when r.rank_combined <= 30 then 'C'
			else 'D'
		end
FROM ranks r
WHERE {wcrp}.ranked_barriers_{species_code}_{watershed}.id = r.id;

ALTER TABLE {wcrp}.ranked_barriers_{species_code}_{watershed}
DROP COLUMN stream_id_up;

"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Done!")



