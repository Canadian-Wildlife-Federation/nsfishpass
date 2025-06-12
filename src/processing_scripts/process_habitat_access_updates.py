from psycopg2.extras import DictCursor
import appconfig

import sys

dataSchema = appconfig.config['DATABASE']['data_schema']
iniSection = appconfig.args.args[0]
streamTable = appconfig.config['DATABASE']['stream_table']
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
dbHabAccessUpdates = "habitat_access_updates"
dbIdField = "id"
dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']
species = appconfig.config[iniSection]['species']

def getPoints(conn):

    query = f"""
    SELECT * FROM {dbTargetSchema}.{dbHabAccessUpdates};
    """

    # use DictCursor so we can access by column name instead of index
    # helpful if we have data structure changes
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(query)
        points = [dict(row) for row in cursor.fetchall()]

    return points

def getUpstreamDownstream(conn):

    print("Getting upstream and downstream stream ids")

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbHabAccessUpdates} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{dbHabAccessUpdates} ADD COLUMN IF NOT EXISTS stream_id_up uuid;
        
        UPDATE {dbTargetSchema}.{dbHabAccessUpdates} SET stream_id_up = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbHabAccessUpdates} b
            WHERE a.geometry && st_buffer(b.snapped_point, 0.01) and
                st_intersects(st_endpoint(a.geometry), st_buffer(b.snapped_point, 0.01))
        )
        UPDATE {dbTargetSchema}.{dbHabAccessUpdates}
            SET stream_id_up = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbHabAccessUpdates}.id;
            
        ALTER TABLE {dbTargetSchema}.{dbHabAccessUpdates} ADD COLUMN IF NOT EXISTS stream_id_down uuid;

        UPDATE {dbTargetSchema}.{dbHabAccessUpdates} SET stream_id_down = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbHabAccessUpdates} b
            WHERE a.geometry && st_buffer(b.snapped_point, 0.01) and
                st_intersects(st_startpoint(a.geometry), st_buffer(b.snapped_point, 0.01))
        )
        UPDATE {dbTargetSchema}.{dbHabAccessUpdates}
            SET stream_id_down = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbHabAccessUpdates}.id;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    query = f"""
        DROP FUNCTION IF EXISTS public.downstream;
        CREATE OR REPLACE FUNCTION public.downstream(sid uuid, limit_id uuid DEFAULT NULL)
        RETURNS TABLE (stream_id uuid)
        LANGUAGE plpgsql
        AS $$
        BEGIN

        IF limit_id IS NOT NULL THEN
            RETURN QUERY
            WITH RECURSIVE walk_network(id, geometry) AS (
                SELECT id, geometry FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE id = $1
                UNION ALL
                SELECT n.id, n.geometry
                FROM {dbTargetSchema}.{dbTargetStreamTable} n, walk_network w
                WHERE ST_DWithin(ST_EndPoint(w.geometry),ST_StartPoint(n.geometry),0.01)
                and n.id != $2
            )
            SELECT id FROM walk_network;

        ELSE
            RETURN QUERY
            WITH RECURSIVE walk_network(id, geometry) AS (
                SELECT id, geometry FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE id = $1
                UNION ALL
                SELECT n.id, n.geometry
                FROM {dbTargetSchema}.{dbTargetStreamTable} n, walk_network w
                WHERE ST_DWithin(ST_EndPoint(w.geometry),ST_StartPoint(n.geometry),0.01)
                and n.id IS NOT NULL
            )
            SELECT id FROM walk_network;

        END IF;
        END; $$
        IMMUTABLE;

        DROP FUNCTION IF EXISTS public.upstream;
        CREATE OR REPLACE FUNCTION public.upstream(sid uuid, limit_id uuid DEFAULT NULL)
        RETURNS TABLE (stream_id uuid)
        LANGUAGE plpgsql
        AS $$
        BEGIN

        IF limit_id IS NOT NULL THEN
            RETURN QUERY
            WITH RECURSIVE walk_network(id, geometry) AS (
                SELECT id, geometry FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE id = $1
                UNION ALL
                SELECT n.id, n.geometry
                FROM {dbTargetSchema}.{dbTargetStreamTable} n, walk_network w
                WHERE ST_DWithin(ST_StartPoint(w.geometry),ST_EndPoint(n.geometry),0.01)
                and n.id != $2
            )
            SELECT id FROM walk_network;

        ELSE
            RETURN QUERY
            WITH RECURSIVE walk_network(id, geometry) AS (
                SELECT id, geometry FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE id = $1
                UNION ALL
                SELECT n.id, n.geometry
                FROM {dbTargetSchema}.{dbTargetStreamTable} n, walk_network w
                WHERE ST_DWithin(ST_StartPoint(w.geometry),ST_EndPoint(n.geometry),0.01)
                and n.id IS NOT NULL
            )
            SELECT id FROM walk_network;

        END IF;
        END; $$
        IMMUTABLE;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def processStreams(points, codes, conn):

    print("Processing updates to accessibility and habitat")

    for point in points:
        species = point['species']
        update_type = point['update_type']
        stream_id_up = point['stream_id_up']
        stream_id_down = point['stream_id_down']
        pair_id = point['pair_id']
        upstream = point['upstream']
        downstream = point['downstream']
        habitat_type = point['habitat_type']
        # comments = point['comments']

        if stream_id_up is None:
            continue

        for c in codes:
            code = c[0]

            if species == code:

                if (update_type == 'access' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET {code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'access' and pair_id is None and not upstream and not downstream):

                    query = f"""
                        --UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET {code}_accessibility = '{appconfig.Accessibility.NOT.value}'
                        --WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));

                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET {code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'access' and pair_id is None and upstream and not downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET {code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'access' and pair_id is None and downstream and not upstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET {code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'spawning' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_spawn_{code} = true
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'rearing' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_rear_{code} = true
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'general' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_{code} = true
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not spawning' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_spawn_{code} = false
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not rearing' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_rear_{code} = false
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not general' and pair_id and upstream):

                    query = f"""
                    SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
                    """

                    with conn.cursor(cursor_factory=DictCursor) as cursor:
                        cursor.execute(query)
                        result = [dict(row) for row in cursor.fetchall()]

                    for r in result:
                        pair_stream_id_down = r['stream_id_down']

                        query = f"""
                            UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_{code} = false, habitat_spawn_{code} = false, habitat_rear_{code} = false
                            WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                        """

                        with conn.cursor() as cursor:
                            cursor.execute(query)
                        conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'spawning' and pair_id is None and upstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_spawn_{code} = true
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'spawning' and pair_id is None and downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_spawn_{code} = true
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'rearing' and pair_id is None and upstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_rear_{code} = true
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'rearing' and pair_id is None and downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_rear_{code} = true
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'general' and pair_id is None and upstream):
                    
                    
                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_{code} = true
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'general' and pair_id is None and downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_{code} = true
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not spawning' and pair_id is None and upstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_spawn_{code} = false
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not spawning' and pair_id is None and downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_spawn_{code} = false
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not rearing' and pair_id is None and upstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_rear_{code} = false
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not rearing' and pair_id is None and downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_rear_{code} = false
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not general' and pair_id is None and upstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_{code} = false, habitat_spawn_{code} = false, habitat_rear_{code} = false
                        WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

                elif (update_type == 'habitat' and habitat_type == 'not general' and pair_id is None and downstream):

                    query = f"""
                        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET habitat_{code} = false, habitat_spawn_{code} = false, habitat_rear_{code} = false
                        WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
                    """

                    with conn.cursor() as cursor:
                        cursor.execute(query)
                    conn.commit()

            else:
                pass

def addComments(points, conn):

    print("Adding comments to streams")

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS "comments";
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS "comments_source";
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN "comments" varchar;
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN "comments_source" varchar;

        UPDATE {dbTargetSchema}.{dbTargetStreamTable} a
        SET
            "comments" = b.comments,
            comments_source = b.update_source
        FROM {dbTargetSchema}.{dbHabAccessUpdates} b
        WHERE b.stream_id_up = a.id
        AND b.update_type = 'comment'
        AND pair_id IS NULL;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    for point in points:
        stream_id_down = point['stream_id_down']
        pair_id = point['pair_id']
        upstream = point['upstream']
        update_type = point['update_type']
        comments = point['comments']

        if pair_id and upstream and update_type == 'comment':

            query = f"""
            SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbHabAccessUpdates} WHERE pair_id = '{pair_id}' AND downstream is true;
            """

            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                result = [dict(row) for row in cursor.fetchall()]

            for r in result:
                pair_stream_id_down = r['stream_id_down']

                query = f"""
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET "comments" = '{comments}'
                    WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{pair_stream_id_down}'));
                """

                with conn.cursor() as cursor:
                    cursor.execute(query)
                conn.commit()

        else:
            pass

def simplifyHabitatAccess(codes, conn):

    # query = f"""
    # SELECT code, name,
    # spawn_gradient_min::float, spawn_gradient_max::float
    # FROM {dataSchema}.{appconfig.fishSpeciesTable};
    # """

    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    #     codes = cursor.fetchall()

    for c in codes:
        code = c[0]
        name = c[1]
        mingradient = c[2]
        maxgradient = c[3]

        spawning = "habitat_spawn_" + code
        rearing = "habitat_rear_" + code

        colname = "habitat_" + code

        # query = f"""
        #     UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET {colname} = false WHERE {spawning} = false AND {rearing} = false;
        #     UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {colname} = false, {spawning} = false, {rearing} = false WHERE {code}_accessibility = '{appconfig.Accessibility.NOT.value}';
        # """
        # with conn.cursor() as cursor:
        #     cursor.execute(query)
        # conn.commit()

        query = f"""
            UPDATE {dbTargetSchema}.{dbTargetStreamTable}
            SET {code}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}' WHERE {code}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND barrier_down_{code}_cnt > 0;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

def main():

    if iniSection in ['cheticamp', 'stewiacke']:
        return

    with appconfig.connectdb() as conn:

        global specCodes
        global species

        specCodes = [substring.strip() for substring in species.split(',')]

        if len(specCodes) == 1:
            specCodes = f"('{specCodes[0]}')"
        else:
            specCodes = tuple(specCodes)

        query = f"""
        SELECT code, name,
        spawn_gradient_min::float, spawn_gradient_max::float
        FROM {dataSchema}.{appconfig.fishSpeciesTable}
        WHERE code IN {specCodes};
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            specCodes = cursor.fetchall()

        getUpstreamDownstream(conn)
        points = getPoints(conn)
        processStreams(points, specCodes, conn)
        # addComments(points, conn)
        simplifyHabitatAccess(specCodes, conn)

    print("Done!")

if __name__ == "__main__":
    main()