async function run(calendar_month_id) {
    const calendar_month = await DB.calendar_month.first(calendar_month_id);
    if (calendar_month) {
        const traffic_centre = await DB.traffic_centre.first('0ded5f8e-9027-11e5-a4b8-001e67260782');
        console.log(`Using the Month: ${calendar_month.name}`);
        const [_cacheTotVehiclesStopCheckedPerTC, _cacheTotDrunkDrivingPerTC, _cacheTotSpeedPerTC] = await cacheEventsAndOperation(calendar_month, traffic_centre);
    }

    console.log(`Nothing Broke... 🎉 `);
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
    console.log(`⏲ Querying the DB for Events and operations...`);
    console.time(`query`);
    let [vsc_arr, ddo_arr, so_arr] = await Promise.all([
        DB.event.where.apply(DB.event, vsc_query_arr).toArray(),
        DB.operation.where.apply(DB.operation, ddo_query_arr).toArray(),
        DB.operation.where.apply(DB.operation, so_query_arr).toArray()
    ]);
    console.log(`Done with Query...`);
    console.timeEnd(`query`);

    console.log(`🚋  Counting Events Veh S&C...`);
    console.time(`vehicle`);
    const tot_vehicles_sc_per_tc = {};
    vsc_arr.map(vehicle_stopped_checked_event => {
        if (vehicle_stopped_checked_event.traffic_centre_id in tot_vehicles_sc_per_tc) {
            tot_vehicles_sc_per_tc[vehicle_stopped_checked_event.traffic_centre_id] = ++tot_vehicles_sc_per_tc[vehicle_stopped_checked_event.traffic_centre_id];
        } else {
            tot_vehicles_sc_per_tc[vehicle_stopped_checked_event.traffic_centre_id] = 1;
        }
    });
    console.log(`Done.`)
    console.timeEnd(`vehicle`);

    console.log(`🍺  Counting drunk driving:`);
    console.time(`drunk`);
    const tot_drunkdrive_op_per_tc = {};
    ddo_arr.map(drunk_driving_operation => {
        if (drunk_driving_operation.traffic_centre_id in tot_drunkdrive_op_per_tc) {
            tot_drunkdrive_op_per_tc[drunk_driving_operation.traffic_centre_id] = ++tot_drunkdrive_op_per_tc[drunk_driving_operation.traffic_centre_id];
        } else {
            tot_drunkdrive_op_per_tc[drunk_driving_operation.traffic_centre_id] = 1;
        }
    });
    console.log(`Done.`)
    console.timeEnd(`drunk`);

    console.log(`🚤  Counting Speed Operations:`);
    console.time(`speed`);
    const tot_speed_op_per_tc = {};
    so_arr.map(speed_operation => {
        if (speed_operation.traffic_centre_id in tot_speed_op_per_tc) {
            tot_speed_op_per_tc[speed_operation.traffic_centre_id] = ++tot_speed_op_per_tc[speed_operation.traffic_centre_id];
        } else {
            tot_speed_op_per_tc[speed_operation.traffic_centre_id] = 1;
        }
    });
    console.log(`Done.`)
    console.timeEnd(`speed`);

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
            break;
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
            break;
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
            break;
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
