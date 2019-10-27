async function run(traffic_centre) {
    const current_date = new Date();
    // Note to self, if its scheduled the the current month, if its triggerd from the app, send the month object...
    const current_month_number = (current_date.getMonth() + 1);
    const current_calendar_year = (current_month_number > 3) ? current_date.getFullYear() : (current_date.getFullYear() - 1);
    const [calendar_year, calendar_month] = await Promise.all([DB.calendar_year.first('year = ?', current_calendar_year), DB.calendar_month.first('number = ?', current_month_number)]);
    if (calendar_year && calendar_month) {
        const calendar_quarter = await calendar_month.calendar_quarter();
        // Cache all the needed data:
        // Each one of these objects has a key for each TC and the total number as the value:
        const [tot_vehicles_sc_per_tc, tot_drunkdrive_op_per_tc, tot_speed_op_per_tc] = await cacheEventsAndOperation(calendar_month, traffic_centre);
        // 1.) TC has sent a request to calculate all APP Targets for the Month: (1 TC)
        // 2.) Scehduled task that must update all APP Targets for the month: (all TCs)
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

module.exports = { run: run };