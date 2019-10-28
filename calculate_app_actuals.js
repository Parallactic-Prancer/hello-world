async function run(traffic_centre, calendar_month) {
    if (traffic_centre && calendar_month) {
        // Was triggered by user: Only update the data for the TC in regards to the given month.
        // Cache all the needed data:
        // Each one of these objects has a key for each TC and the total as the value:
        const [_cacheTotVehiclesStopCheckedPerTC, _cacheTotDrunkDrivingPerTC, _cacheTotSpeedPerTC] = await cacheEventsAndOperation(calendar_month, traffic_centre);
    } else {
        // Was triggered via Schedule:
        const current_date = new Date();
        const current_month_number = (current_date.getMonth() + 1);
        const current_calendar_year = (current_month_number > 3) ? current_date.getFullYear() : (current_date.getFullYear() - 1);
        // Get current Year and Month DB Objects:
        const [calendar_year, calendar_month] = await Promise.all([DB.calendar_year.first('year = ?', current_calendar_year), DB.calendar_month.first('number = ?', current_month_number)]);
        if (calendar_year && calendar_month) {
            // Get Quarter and all the Traffic Centres:
            const [calendar_quarter, traffic_centres] = await Promise.all([calendar_month.calendar_quarter(), DB.traffic_centre.all().toArray()]);
            const [_cacheTotVehiclesStopCheckedPerTC, _cacheTotDrunkDrivingPerTC, _cacheTotSpeedPerTC] = await cacheEventsAndOperation(calendar_month, null);
            const [_cacheAPPActualsPerTC, _cacheQuartAPPTargPerTC, _cacheQuartAPPTargTot, _cacheAnnualAPPTargPerTC, _cacheAnnualAPPTargTot] = await cacheAllAPPTargetWorkflowObjets(calendar_year, calendar_quarter, calendar_month, null);

            // Let's Start actually updating the needed stuff:
            for (let traffic_centre of traffic_centres) {

            }
        }
    }

}

async function cacheEventsAndOperation(calendar_month, traffic_centre) {
    let month_start_date = new Date(`${calendar_month.year} ${calendar_month.number}`);
    let month_stop_date = new Date(month_start_date);
    month_stop_date.setMonth(month_stop_date.getMonth() + 1);
    // Set up queries for 'Vehicles Stopped & Checked', 'Drunk Driving Operations' and 'Speed Operations':
    let vsc_query_arr = getQueryStringAndArguments('vehicles_stopped_and_checked', traffic_centre, month_start_date, month_stop_date);
    let ddo_query_arr = getQueryStringAndArguments('drunk_driving_operation', traffic_centre, month_start_date, month_stop_date);
    let so_query_arr = getQueryStringAndArguments('speed_operation', traffic_centre, month_start_date, month_stop_date);
    // Get the needed Events and Operations for the Current Month:
    let [vsc_arr, ddo_arr, so_arr] = await Promise.all([
        DB.event.where.apply(DB.event, vsc_query_arr).toArray(),
        DB.operation.where.apply(DB.operation, ddo_query_arr).toArray(),
        DB.operation.where.apply(DB.operation, so_query_arr).toArray()
    ]);

    const tot_vehicles_sc_per_tc = {};
    vsc_arr.map(vehicle_stopped_checked_event => {
        if (vehicle_stopped_checked_event.traffic_centre_id in tot_vehicles_sc_per_tc) {
            tot_vehicles_sc_per_tc[vehicle_stopped_checked_event.traffic_centre_id] = ++tot_vehicles_sc_per_tc[vehicle_stopped_checked_event.traffic_centre_id];
        } else {
            tot_vehicles_sc_per_tc[vehicle_stopped_checked_event.traffic_centre_id] = 1;
        }
    });
    const tot_drunkdrive_op_per_tc = {};
    ddo_arr.map(drunk_driving_operation => {
        if (drunk_driving_operation.traffic_centre_id in tot_drunkdrive_op_per_tc) {
            tot_drunkdrive_op_per_tc[drunk_driving_operation.traffic_centre_id] = ++tot_drunkdrive_op_per_tc[drunk_driving_operation.traffic_centre_id];
        } else {
            tot_drunkdrive_op_per_tc[drunk_driving_operation.traffic_centre_id] = 1;
        }
    });
    const tot_speed_op_per_tc = {};
    so_arr.map(speed_operation => {
        if (speed_operation.traffic_centre_id in tot_speed_op_per_tc) {
            tot_speed_op_per_tc[speed_operation.traffic_centre_id] = ++tot_speed_op_per_tc[speed_operation.traffic_centre_id];
        } else {
            tot_speed_op_per_tc[speed_operation.traffic_centre_id] = 1;
        }
    });

    return [tot_vehicles_sc_per_tc, tot_drunkdrive_op_per_tc, tot_speed_op_per_tc];
}

function getQueryStringAndArguments(query_type, traffic_centre, month_start_date, month_stop_date) {
    let query_string, query_arguments;
    switch (query_type) {
        case 'vehicles_stopped_and_checked':
            query_string = '(event_type = ? or event_type = ?) and status = ? and completed_at >= ? and completed_at < ?';
            query_arguments = [
                0, // Roadside Vehicle Inspection
                5, // Manual Stats
                1, // Completed
                month_start_date,
                month_stop_date
            ];

        case 'drunk_driving_operation':
            query_string = 'roadblock_type = ? and (status = ? or status = ? or status = ?) and actual_operation_start_time >= ? and actual_operation_start_time < ?';
            query_arguments = [
                'drunk_driving',
                'completed_and_pending',
                'completed_and_approved',
                'closed',
                month_start_date,
                month_stop_date
            ];

        case 'speed_operation':
            query_string = 'general_type = ? and (status = ? or status = ? or status = ?) and actual_operation_start_time >= ? and actual_operation_start_time < ?';
            query_arguments = [
                'speed_operation',
                'completed_and_pending',
                'completed_and_approved',
                'closed',
                month_start_date,
                month_stop_date
            ];
        default:
            break;
    }

    if (query_string) {
        if (traffic_centre) {
            query_string += ' and traffic_centre = ?';
            query_arguments.push(traffic_centre);
        }
        return ([query_string].concat(query_arguments));
    }
    return null;
}

async function cacheAllAPPTargetWorkflowObjets(calendar_year, calendar_quarter, calendar_month, traffic_centre) {
    const _appActualsPerTC = await getAPPActuals(calendar_month, traffic_centre);
    const [_quarterAPPTargetsPerTC, _quarterAPPTargetTotals] = await Promise.all([getQuarterlyAPPTargetsPerTC(calendar_quarter, traffic_centre), getQuarterlyTotalsPerAPPTarget(calendar_quarter)]);
    const [_annualAPPTargetsPerTC, _annualAPPTargetTotals] = await Promise.all([getAnnualAPPTargetsPerTC(calendar_year, traffic_centre), getAnnualTotalsPerAPPTarget(calendar_year)]);

    return [_appActualsPerTC, _quarterAPPTargetsPerTC, _quarterAPPTargetTotals, _annualAPPTargetsPerTC, _annualAPPTargetTotals];
}

/*
getAPPActuals:
    Returns an Object with the Following:
        keys - Traffic Centres (IDs)
        values - Array of APP Actual Objects for the Traffic Centre on the given calendar_month
    NOTE: 
        1. If traffic centre is given as an argument it will only get all app actuals for that TC. 
        2. If no TC is given as an argument then it will return app actuals for all TCs of the given calendar_month
        * THERE MUST ALWAYS BE A calendar_month OBJECT PASSED TO THIS FUNCTION

*/
async function getAPPActuals(calendar_month, traffic_centre) {
    let query_string = 'calendar_month = ?';
    let arguments_arr = [calendar_month];
    if (traffic_centre) {
        query_string += ' and traffic_centre = ?';
        arguments_arr.push(traffic_centre);
    }
    let final_query_arr = [query_string].concat(arguments_arr);
    let all_app_actuals = await DB.app_actual.where.apply(DB.app_actual, final_query_arr).toArray();
    // Cache the Queried APP Actual Objects as an array per TC:
    const app_actuals_per_tc_cache = {};
    all_app_actuals.map(app_actual => {
        if (app_actual.traffic_centre_id in app_actuals_per_tc_cache) {
            app_actuals_per_tc_cache[app_actual.traffic_centre_id].push(app_actual);
        } else {
            app_actuals_per_tc_cache[app_actual.traffic_centre_id] = [app_actual];
        }
    });
    return app_actuals_per_tc_cache;
}

/*
getQuarterlyAPPTargets:
    Returns an Object with the Following:
        keys - Traffic Centres (IDs)
        values - Another Object with APP Target ID as a key and the quarterly_app_target_for_traffic_centre as the value
    NOTE: 
        1. If traffic centre is given as an argument it will only get all quarterly_app_target_for_traffic_centres for that TC. 
        2. If no TC is given as an argument then it will return quarterly_app_target_for_traffic_centres for all TCs of the given calendar_quarter
        * THERE MUST ALWAYS BE A calendar_quarter OBJECT PASSED TO THIS FUNCTION

*/
async function getQuarterlyAPPTargetsPerTC(calendar_quarter, traffic_centre) {
    let query_string = 'calendar_quarter = ?';
    let arguments_arr = [calendar_quarter];
    if (traffic_centre) {
        query_string += ' and traffic_centre = ?';
        arguments_arr.push(traffic_centre);
    }
    let final_query_arr = [query_string].concat(arguments_arr);
    let quarterly_app_target_for_traffic_centres = await DB.quarterly_app_target_for_traffic_centre.where.apply(DB.quarterly_app_target_for_traffic_centre, final_query_arr).toArray();
    // Cache the Objects:
    let cache_object = {};
    quarterly_app_target_for_traffic_centres.map(quart_at_for_tc => {
        if (quart_at_for_tc.traffic_centre_id in cache_object) {
            cache_object[quart_at_for_tc.traffic_centre_id][quart_at_for_tc.app_target_id] = quart_at_for_tc;
        } else {
            cache_object[quart_at_for_tc.traffic_centre_id] = {};
            cache_object[quart_at_for_tc.traffic_centre_id][quart_at_for_tc.app_target_id] = quart_at_for_tc;
        }
    });
    return cache_object;
}

/*
getQuarterlyTotalsPerAPPTarget:
    Returns an Object with the Following:
        keys - APP Target (IDs)
        values - quarterly_app_target_total object for the given calendar_quarter object.
*/
async function getQuarterlyTotalsPerAPPTarget(calendar_quarter) {
    const quarterly_app_target_totals = await DB.quarterly_app_target_total.where('archived != ? and calendar_quarter = ?', true, calendar_quarter).toArray();
    const cache_object = {};
    quarterly_app_target_totals.map(quarterly_app_target_total => {
        cache_object[quarterly_app_target_total.app_target_id] = quarterly_app_target_total;
    });
    return cache_object;
}

async function getAnnualAPPTargetsPerTC(calendar_year, traffic_centre) {
    let query_string = 'calendar_year = ?';
    let arguments_arr = [calendar_year];
    if (traffic_centre) {
        query_string += ' and traffic_centre = ?';
        arguments_arr.push(traffic_centre);
    }
    let final_query_arr = [query_string].concat(arguments_arr);
    let annual_app_target_for_traffic_centres = await DB.annual_app_target_for_traffic_centre.where.apply(DB.annual_app_target_for_traffic_centre, final_query_arr).toArray();
    // Cache the Objects:
    let cache_object = {};
    annual_app_target_for_traffic_centres.map(annual_app_target_for_traffic_centre => {
        if (annual_app_target_for_traffic_centre.traffic_centre_id in cache_object) {
            cache_object[annual_app_target_for_traffic_centre.traffic_centre_id][annual_app_target_for_traffic_centre.app_target_id] = annual_app_target_for_traffic_centre;
        } else {
            cache_object[annual_app_target_for_traffic_centre.traffic_centre_id] = {};
            cache_object[annual_app_target_for_traffic_centre.traffic_centre_id][annual_app_target_for_traffic_centre.app_target_id] = annual_app_target_for_traffic_centre;
        }
    });
    return cache_object;
}

async function getAnnualTotalsPerAPPTarget(calendar_year) {
    const annual_app_target_totals = await DB.annual_app_target_total.where('archived != ? and calendar_year = ?', true, calendar_year).toArray();
    const cache_object = {};
    annual_app_target_totals.map(annual_app_target_total => {
        cache_object[annual_app_target_total.app_target_id] = annual_app_target_total;
    });
    return cache_object;
}

module.exports = { run: run };
