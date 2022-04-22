## Query Throughput and % Busy for a Time Period   ##

DECLARE _RANGE_START_TS_LOCAL timestamp;
DECLARE _RANGE_END_TS_LOCAL timestamp;
DECLARE _RANGE_INTERVAL_MINUTES int64;
DECLARE _UTC_OFFSET INT64;
DECLARE _SLOTS_ALLOCATED INT64;
DECLARE _SLOTS_SECONDS_ALLOCATED_PER_INTERVAL INT64;
DECLARE _RANGE_START_TS_UTC timestamp;
DECLARE _RANGE_END_TS_UTC timestamp;
DECLARE _TIMEZONE STRING;
 
SET _TIMEZONE               =   "US/Eastern";
SET _RANGE_START_TS_LOCAL   =   '2021-04-14 13:03:00.00000';
SET _RANGE_END_TS_LOCAL     =   '2021-04-14 14:03:00.00000';
SET _RANGE_INTERVAL_MINUTES =   1;
SET _SLOTS_ALLOCATED        =   2000;
SET _SLOTS_SECONDS_ALLOCATED_PER_INTERVAL =   _SLOTS_ALLOCATED * _RANGE_INTERVAL_MINUTES * 60;
SET _UTC_OFFSET             =   (
                               SELECT  DATETIME_DIFF(DATETIME(_RANGE_START_TS_LOCAL
                               ,   _TIMEZONE),DATETIME(_RANGE_START_TS_LOCAL),HOUR)
                               );
SET _RANGE_START_TS_UTC     =   TimeStamp_SUB(_Range_Start_TS_LOCAL, Interval _UTC_OFFSET Hour);
SET _RANGE_END_TS_UTC       =   TimeStamp_SUB(_Range_End_TS_LOCAL, Interval _UTC_OFFSET Hour);
 
SELECT  Cast(TimeStamp_Add(key.Key_TS, Interval _UTC_OFFSET Hour) as DateTime) as Period_TS_Local
,       query_info.reservation_id
,       query_info.project_id
,       query_info.user_email
,       IFNULL(query_info.Total_Slot_Sec, 0) as Total_Slot_Sec 
,       Round((IFNULL(query_info.Total_Slot_Sec, 0) / _SLOTS_SECONDS_ALLOCATED_PER_INTERVAL), 4) as Pct_Slot_Usage
,       Round(IFNULL(query_info.Total_Slot_Sec, 0) / (_RANGE_INTERVAL_MINUTES * 60), 4) as Avg_Interval_Slot_Seconds
,       IFNULL(query_info.Query_Count, 0) as Query_Count  
FROM
(
SELECT  Cast(TimeStamp_Trunc(Point_In_Time, SECOND) as TimeStamp) as Key_TS
FROM    UNNEST(GENERATE_TIMESTAMP_ARRAY(_RANGE_START_TS_UTC, _RANGE_END_TS_UTC, INTERVAL _RANGE_INTERVAL_MINUTES MINUTE)) Point_In_Time
) key
LEFT OUTER JOIN
(
Select  reservation_id
,   project_id
,   user_email
,   TIMESTAMP_ADD((TIMESTAMP_SECONDS(_RANGE_INTERVAL_MINUTES *60 * DIV(UNIX_SECONDS(period_start), _RANGE_INTERVAL_MINUTES*60))), INTERVAL (Mod(extract(minute from _RANGE_START_TS_UTC),_RANGE_INTERVAL_MINUTES)) MINUTE) as Period_TS
,   Count(Distinct job_id) as Query_Count
,   sum(period_slot_ms)/1000 as Total_Slot_Sec
FROM `region-us.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT`
Where 1=1
and job_creation_time between timestamp_sub(_RANGE_START_TS_UTC, interval 6 hour) and _RANGE_END_TS_UTC
and period_start between _RANGE_START_TS_UTC and _RANGE_END_TS_UTC
and job_type = 'QUERY'
and statement_type <> 'SCRIPT'
and period_slot_ms > 0
group by 1,2,3,4
) query_info
ON  query_info.Period_TS = key.Key_TS;