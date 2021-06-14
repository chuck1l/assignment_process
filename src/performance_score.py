import pandas as pd
import numpy as np
from config import database_connect


def create_pp_score(con, data):

    df = data.copy()
    tp_code_tuple = tuple(df.provider_code.unique())

    # SQL query for the provider performance score logic
    sql_pps = f'''
    SELECT
    flr.provider_code
    , CASE WHEN flr.otp_willcall_den = FALSE
            AND flr.otp_discharge_den = FALSE
            AND flr.otp_urgent_den = FALSE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id = 'A'
            AND flr.do_time_actual IS NOT NULL
            AND flr.do_scheduled_time IS NOT NULL
            AND DATEDIFF('minute', flr.do_scheduled_time, flr.do_time_actual) <= 15
        THEN 1.000
        ELSE 0
    END AS a_leg_do_num
    , CASE WHEN flr.otp_willcall_den = FALSE
            AND flr.otp_discharge_den = FALSE
            AND flr.otp_urgent_den = FALSE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id = 'A'
            AND flr.do_time_actual IS NOT NULL
            AND flr.do_scheduled_time IS NOT NULL
        THEN 1.000
        ELSE 0
    END AS a_leg_do_den
    , CASE WHEN flr.otp_willcall_den = FALSE
            AND flr.otp_discharge_den = FALSE
            AND flr.otp_urgent_den = FALSE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id = 'A'
            AND flr.do_time_actual IS NOT NULL
            AND flr.do_scheduled_time IS NOT NULL
            AND DATEDIFF('minute', flr.pu_scheduled_time, flr.pu_time_actual) <= 15
        THEN 1.000
        ELSE 0
    END AS a_leg_pu_sched_num
    , CASE WHEN flr.otp_willcall_den = FALSE
            AND flr.otp_discharge_den = FALSE
            AND flr.otp_urgent_den = FALSE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id = 'A'
            AND flr.do_time_actual IS NOT NULL
            AND flr.do_scheduled_time IS NOT NULL
        THEN 1.000
        ELSE 0
    END AS a_leg_pu_sched_den
    , CASE WHEN flr.otp_willcall_den = FALSE
            AND flr.otp_discharge_den = FALSE
            AND flr.otp_urgent_den = FALSE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id = 'B'
            AND flr.do_time_actual IS NOT NULL
            AND flr.do_scheduled_time IS NOT NULL
            AND DATEDIFF('minute', flr.pu_scheduled_time, flr.pu_time_actual) <= 15
        THEN 1.000
        ELSE 0
    END AS b_leg_pu_sched_num
    , CASE WHEN flr.otp_willcall_den = FALSE
            AND flr.otp_discharge_den = FALSE
            AND flr.otp_urgent_den = FALSE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id = 'B'
            AND flr.do_time_actual IS NOT NULL
            AND flr.do_scheduled_time IS NOT NULL
        THEN 1.000
        ELSE 0
    END AS b_leg_pu_sched_den
    , CASE WHEN flr.otp_willcall_den = TRUE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id IN ('A', 'B')
            AND flr.pu_time_actual IS NOT NULL
            AND DATEDIFF('minute', flr.willcall_recvd, flr.pu_time_actual) <= 60
        THEN 1.000
        ELSE 0
    END AS will_call_pu_num
    , CASE WHEN flr.otp_willcall_den = TRUE
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id IN ('A', 'B')
            AND flr.pu_time_actual IS NOT NULL
        THEN 1.000
        ELSE 0
    END AS will_call_pu_den
    , CASE WHEN (flr.otp_discharge_den = TRUE OR flr.otp_urgent_den = TRUE)
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id IN ('A', 'B')
            AND flr.pu_time_actual IS NOT NULL
            AND flr.pu_scheduled_time IS NOT NULL
            AND DATEDIFF('minute', flr.pu_scheduled_time, flr.pu_time_actual) <= 60
        THEN 1.000
        ELSE 0
    END AS discharge_pu_num
    , CASE WHEN (flr.otp_discharge_den = TRUE OR flr.otp_urgent_den = TRUE)
            AND flr.los_group_desc IN ('Ambulatory', 'Wheelchair', 'Stretcher')
            AND flr.ride_status = 'Verified-Paid'
            AND flr.ride_id IN ('A', 'B')
            AND flr.pu_time_actual IS NOT NULL
            AND flr.pu_scheduled_time IS NOT NULL
        THEN 1.000
        ELSE 0
    END AS discharge_pu_den
    , CASE WHEN a_leg_pu_sched_den = 1
            OR will_call_pu_den = 1
            OR discharge_pu_den = 1
        THEN 1.000
        ELSE 0
    END AS a_leg_pu_den
    , CASE WHEN b_leg_pu_sched_den = 1
            OR will_call_pu_den = 1
            OR discharge_pu_den = 1
        THEN 1.000
        ELSE 0
    END AS b_leg_pu_den
    , CASE WHEN a_leg_pu_den = 1
            AND ((a_leg_pu_sched_den + will_call_pu_den + discharge_pu_den) = 
            (a_leg_pu_sched_num + will_call_pu_num + discharge_pu_num))
        THEN 1.000
        ELSE 0
    END AS a_leg_pu_num
    , CASE WHEN b_leg_pu_den = 1
            AND ((b_leg_pu_sched_den + will_call_pu_den + discharge_pu_den) = 
            (b_leg_pu_sched_num + will_call_pu_num + discharge_pu_num))
        THEN 1.000
        ELSE 0
    END AS b_leg_pu_num
    , CASE WHEN flr.cancelation_reason IN
            ('Transportation Provider No Show / Late', 'No Provider Willing To Transport'
            , 'No Vehicle Or Transportation Available', 'No Car Seat'
            , 'No Vehicle Available - No Driver Available'
            , 'No Provider Willing to Transport ? Member/Family Abusive'
            , 'No Vehicle Available - Call Volume/Time Constraints'
            , 'No Vehicle Available - Inclement Weather', 'Transportation Provider No Show'
            , 'No Vehicle Available - Breakdown/Maintenance'
            , 'No Vehicle Available - Outside Service Hours')
            AND flr.recovered_vs_missed_vs_complete != 'Recovered'
        THEN 1.00
        ELSE 0.00
    END AS tp_missed_trip
    , CASE WHEN flc.complaint_against_entity IN
            ('Transportation Provider', 'Transportation Provider Employee', 'Vehicle')
            AND flc.complaint_type_code = 2
        THEN 1.00
        ELSE 0.00
    END AS tp_late_complaint
    , CASE WHEN flc.complaint_against_entity IN
            ('Transportation Provider', 'Transportation Provider Employee', 'Vehicle')
        THEN 1.00
        ELSE 0.00
    END AS tp_complaint
    FROM dw.fact_lcad_ride flr
    LEFT JOIN dw.fact_lcad_complaints flc ON flr.ride_key = flc.ride_key
    WHERE
    flr.date_ride BETWEEN dateadd('Month', -5, current_date) AND dateadd('day', -66, current_date)
    AND flr.provider_status = 1
    AND flr.provider_type_name IN 
        ('Dedicated', 'Non-Dedicated', 'Shooter', 'Sole Source', 'Taxi-Non-contracted', 'Volunteer')
    AND flr.provider_code IN {tp_code_tuple};'''
    
    performance_scores = pd.read_sql_query(sql_pps, con)

    performance_scores = performance_scores.groupby('provider_code', as_index=False).apply(
        lambda x: pd.Series({
            'a_leg_do_otp' : np.sum(x.a_leg_do_num)/np.sum(x.a_leg_do_den),
            'a_leg_pu_otp': np.sum(x.a_leg_pu_num)/np.sum(x.a_leg_pu_den),
            'b_leg_pu_otp': np.sum(x.b_leg_pu_num)/np.sum(x.b_leg_pu_den),
            'tp_complaint_percent': np.average(x.tp_complaint),
            'tp_late_complaint_percent': np.average(x.tp_late_complaint),
            'tp_missed_trip_percent': np.average(x.tp_missed_trip)
        })
    )

    # Creating the a leg pick up score from specified conditions
    conditions_apu = [
        performance_scores['a_leg_pu_otp'] >= 0.90,
        performance_scores['a_leg_pu_otp'] >= 0.87,
        performance_scores['a_leg_pu_otp'] >= 0.84,
        performance_scores['a_leg_pu_otp'] >= 0.80,
        performance_scores['a_leg_pu_otp'] >= 0.65,
        performance_scores['a_leg_pu_otp'] >= 0.50,
        performance_scores['a_leg_pu_otp'] < 0.50,
    ]
    values_apu = [10, 8.3, 6.7, 5, 3.3, 1.7, 0]

    performance_scores['a_pu_score'] = np.select(conditions_apu, values_apu)
    # Creating the a leg drop off score from specified conditions
    conditions_ado = [
        performance_scores['a_leg_do_otp'] >= 0.90,
        performance_scores['a_leg_do_otp'] >= 0.87,
        performance_scores['a_leg_do_otp'] >= 0.84,
        performance_scores['a_leg_do_otp'] >= 0.80,
        performance_scores['a_leg_do_otp'] >= 0.65,
        performance_scores['a_leg_do_otp'] >= 0.50,
        performance_scores['a_leg_do_otp'] < 0.50,
    ]
    values_ado = [20, 16.7, 13.3, 10, 6.7, 3.3, 0]

    performance_scores['a_do_score'] = np.select(conditions_ado, values_ado)
    # Creating the b leg pick up score from specified conditions
    conditions_bpu = [
        performance_scores['b_leg_pu_otp'] >= 0.90,
        performance_scores['b_leg_pu_otp'] >= 0.87,
        performance_scores['b_leg_pu_otp'] >= 0.84,
        performance_scores['b_leg_pu_otp'] >= 0.80,
        performance_scores['b_leg_pu_otp'] >= 0.65,
        performance_scores['b_leg_pu_otp'] >= 0.50,
        performance_scores['b_leg_pu_otp'] < 0.50,
    ]
    values_bpu = [20, 16.7, 13.3, 10, 6.7, 3.3, 0]

    performance_scores['b_pu_score'] = np.select(conditions_bpu, values_bpu)
    # Creating the tp complaint score from specified conditions
    conditions_tcs = [
        performance_scores['tp_complaint_percent'] <= 0.0035,
        performance_scores['tp_complaint_percent'] <= 0.005,
        performance_scores['tp_complaint_percent'] <= 0.0085,
        performance_scores['tp_complaint_percent'] <= 0.0105,
        performance_scores['tp_complaint_percent'] <= 0.0125,
        performance_scores['tp_complaint_percent'] <= 0.0175,
        performance_scores['tp_complaint_percent'] > 0.0175,
    ]
    values_tcs = [10, 8.3, 6.7, 5, 3.3, 1.7, 0]

    performance_scores['tp_complaint_score'] = np.select(conditions_tcs, values_tcs)
    # Creating the tp late score from specified conditions
    conditions_tls = [
        performance_scores['tp_late_complaint_percent'] <= 0.0005,
        performance_scores['tp_late_complaint_percent'] <= 0.001,
        performance_scores['tp_late_complaint_percent'] <= 0.0025,
        performance_scores['tp_late_complaint_percent'] <= 0.003,
        performance_scores['tp_late_complaint_percent'] <= 0.004,
        performance_scores['tp_late_complaint_percent'] <= 0.0055,
        performance_scores['tp_late_complaint_percent'] > 0.0055,
    ]
    values_tls = [15, 12.5, 10, 7.5, 5, 2.5, 0]

    performance_scores['tp_late_score'] = np.select(conditions_tls, values_tls)
    # Creating the tp missed trip score from specified conditions
    conditions_tmts = [
        performance_scores['tp_missed_trip_percent'] <= 0.002,
        performance_scores['tp_missed_trip_percent'] <= 0.0035,
        performance_scores['tp_missed_trip_percent'] <= 0.0065,
        performance_scores['tp_missed_trip_percent'] <= 0.008,
        performance_scores['tp_missed_trip_percent'] <= 0.0115,
        performance_scores['tp_missed_trip_percent'] <= 0.0185,
        performance_scores['tp_missed_trip_percent'] > 0.0185,
    ]
    values_tmts = [30, 25, 20, 15, 10, 5, 0]

    performance_scores['tp_missed_trip_score'] = np.select(conditions_tmts, values_tmts)

    performance_scores['performance_score'] = performance_scores[[
        'a_pu_score',
        'a_do_score',
        'b_pu_score',
        'tp_complaint_score',
        'tp_late_score',
        'tp_missed_trip_score'
    ]].sum(axis=1)

    performance_scores = performance_scores[['provider_code', 'performance_score']]

    output = df.merge(performance_scores, how='left', on='provider_code')

    return output


if __name__ == '__main__':
    d = {'provider_code': [505607, 508703, 510613, 508703, 505607, 508703], 'value': [1, 2, 3, 4, 5, 6]}
    test_data = pd.DataFrame(data=d)
    con = database_connect()
    test_result = create_pp_score(con, test_data)
    print(test_result)