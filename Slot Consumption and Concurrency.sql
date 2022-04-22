
## Analyze BigQuery Slot Consumption and  Concurrency for a Point in Time ##

DECLARE _RANGE_START_TS_LOCAL timestamp;
DECLARE _RANGE_END_TS_LOCAL timestamp;
DECLARE _RANGE_INTERVAL_SECONDS int64;
DECLARE _UTC_OFFSET INT64;
DECLARE _RANGE_START_TS_UTC timestamp;
DECLARE _RANGE_END_TS_UTC timestamp;
DECLARE _TIMEZONE STRING;

SET _TIMEZONE               =   "US/Eastern";
SET _RANGE_START_TS_LOCAL   =   '2021-04-14 13:52:00.00000';
SET _RANGE_END_TS_LOCAL     =   '2021-04-14 13:58:00.00000';
SET _RANGE_INTERVAL_SECONDS =   1;
SET _UTC_OFFSET             =   	(
SELECT	DATETIME_DIFF(DATETIME(_RANGE_START_TS_LOCAL
,	_TIMEZONE),DATETIME(_RANGE_START_TS_LOCAL),HOUR)
);
SET _RANGE_START_TS_UTC     =   TimeStamp_SUB(_Range_Start_TS_LOCAL, Interval _UTC_OFFSET Hour);
SET _RANGE_END_TS_UTC       =   TimeStamp_SUB(_Range_End_TS_LOCAL, Interval _UTC_OFFSET Hour);

SELECT  Cast(TimeStamp_Add(key.Key_TS, Interval _UTC_OFFSET Hour) as DateTime) as Period_TS_Local
,       query_info.reservation_id
,       query_info.project_id
,       query_info.user_email
,       query_info.job_id
,       IFNULL(query_info.Total_Slot_Sec,0) as Total_Slot_Sec   
,       IFNULL(query_info.Query_Count,0) as Query_Count
FROM
(
SELECT
   Cast(TimeStamp_Trunc(Point_In_Time, SECOND) as TimeStamp) as Key_TS
FROM
   UNNEST(GENERATE_TIMESTAMP_ARRAY(_RANGE_START_TS_UTC, _RANGE_END_TS_UTC, INTERVAL _RANGE_INTERVAL_SECONDS second)) Point_In_Time
) key
LEFT OUTER JOIN
(
Select
   reservation_id
,   project_id
,   user_email
,   job_id
,   cast(TIMESTAMP_TRUNC(period_start, SECOND) as timestamp) as Period_TS
,   sum(period_slot_ms)/1000 as Total_Slot_Sec
,   count(*) as Query_Count
From
   `region-us.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT` j
Where 1=1
and job_creation_time between timestamp_sub(_RANGE_START_TS_UTC, interval 6 hour) and _RANGE_END_TS_UTC
and period_start between _RANGE_START_TS_UTC and _RANGE_END_TS_UTC
and job_type = 'QUERY'
and statement_type <> 'SCRIPT'
and period_slot_ms > 0
and MOD((Timestamp_DIFF(cast(TIMESTAMP_TRUNC(period_start, SECOND) as timestamp), TIMESTAMP_TRUNC(_RANGE_START_TS_UTC, SECOND),SECOND)), _RANGE_INTERVAL_SECONDS) = 0
group by 1,2,3,4,5
) query_info
ON  key.Key_TS = query_info.Period_TS;