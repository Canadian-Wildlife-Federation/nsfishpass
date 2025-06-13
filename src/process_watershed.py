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
# This script runs all the steps to process and calculate connectivity for a watershed.
#
 
import appconfig
from datetime import datetime

from processing_scripts import (
    load_parameters,
    preprocess_watershed,
    load_and_snap_barriers_cabd,
    load_and_snap_fishobservation,
    load_habitat_access_updates,
    process_habitat_access_updates,
    compute_modelled_crossings,
    load_barrier_updates,
    compute_mainstems,
    assign_raw_z,
    smooth_z,
    compute_vertex_gradient,
    compute_segment_gradient,
    break_streams_at_barriers,
    compute_updown_barriers_fish,
    compute_accessibility,
    assign_habitat,
    compute_barriers_upstream_values,
    compute_barrier_dci,
    remove_isolated_flowpaths,
    load_ais,
    barrier_passability_view,
    rank_barriers,
    watershed_summary_stats,
    process_assessments,
)


def run_model(watershed_id):
    '''
    Runs the entire model workflow.
    '''
    print (f"Processing: {watershed_id}")

    load_parameters.main()
    preprocess_watershed.main()
    remove_isolated_flowpaths.main()
    load_and_snap_barriers_cabd.main()
    # load_and_snap_fishobservation.main()
    compute_modelled_crossings.main()
    load_barrier_updates.main()

    # process_assessments.main()

    compute_mainstems.main()

    dem_files = assign_raw_z.indexDem()

    assign_raw_z.main(dem_files)
    smooth_z.main()
    compute_vertex_gradient.main()
    load_habitat_access_updates.main()
    break_streams_at_barriers.main()
    print ("Recalculating elevations on broken streams: " + watershed_id)
    #re-assign elevations to broken streams
    assign_raw_z.main(dem_files)
    smooth_z.main()
    compute_segment_gradient.main()
    compute_updown_barriers_fish.main()
    compute_accessibility.main()
    assign_habitat.main()
    process_habitat_access_updates.main()
    compute_barriers_upstream_values.main()
    load_ais.main()
    compute_barrier_dci.main()
    rank_barriers.main()
    barrier_passability_view.main()
    watershed_summary_stats.main()

    print (f"Processing Complete: {watershed_id}")

if __name__ == "__main__":
    startTime = datetime.now()
    iniSection = appconfig.args.args[0]
    workingWatershedId = appconfig.config[iniSection]['watershed_id']
    run_model(workingWatershedId)
    print("Runtime: " + str((datetime.now() - startTime)))