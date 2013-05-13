/*
 * A database for diagramming workflow
 */

create or replace language plpgsql;

DROP SCHEMA IF EXISTS act2 CASCADE;
CREATE SCHEMA act2; 

/*
 * Represents facility
 */
CREATE TABLE act2.facility (
	fid 		SERIAL PRIMARY KEY,
	code		char(8),
	shortName	text,
	longName	text,
	grossSF		int,
	yearBuilt	int
);

/*
 * Represents device
 */
CREATE TABLE act2.device (
	did 		SERIAL PRIMARY KEY,
	ddescription	varchar(64),
	fid		int references act2.facility (fid) on delete no action
);

/*
 * Represents device records with referential integrity problems
 */
CREATE TABLE act2.devicetemp (
	did 		SERIAL PRIMARY KEY,
	ddescription	varchar(64),
	fid		int 
);


 /* 
  * We create a domain to represent intervals
  */
 create domain act2.unit_d char(1)
	check (value in (
		'A',  /* 15 minutes */
		'B',	/* 1h */ 
		'C',	/* 4hr */ 
		'D' 	/* 24h4 */
)); 


/*
 * Represents work_log or time series data of electricity readings
 */
CREATE TABLE act2.work_log (
	id 		SERIAL PRIMARY KEY,
	did		int references act2.device (did) on delete no action,
	timelogged	timestamp,
	e_reading	numeric,
	e		real,
	delta_unit	act2.unit_d DEFAULT 'A',
	year		int,
	moy		int,
	woy		int,
	dow		int
);


/*
 * Represents work_log or time series data of electricity readings
 */
CREATE TABLE act2.work_log_temp (
	id 		SERIAL PRIMARY KEY,
	did		int,
	timelogged	timestamp,
	e_reading	numeric,
	e		real,
	delta_unit	act2.unit_d DEFAULT 'A',
	year		int,
	moy		int,
	woy		int,
	dow		int
);

COPY act2.facility (fid, code, shortName, longName, grossSF, yearBuilt)
	FROM '/var/tmp/facility.txt' WITH DELIMITER '	';

COPY act2.devicetemp (did, fid, ddescription)
	FROM '/var/tmp/device.txt' WITH DELIMITER '	';
/*
COPY act2.work_log_temp (id, did, timelogged, e_reading, e, delta_unit, year, moy, woy, dow)
	FROM '/var/tmp/worklog.txt' WITH DELIMITER '	';
*/

COPY act2.work_log_temp (id, did, timelogged, e_reading, e, delta_unit, year, moy, woy, dow)
	FROM '/var/tmp/work_log_full.txt' WITH DELIMITER '	';


/*
COPY act2.facility (fid, code, shortName, longName, grossSF, yearBuilt)
	FROM '/nfs/bronfs/uwfs/dw00/d41/ktyunho/act2/facility.txt' WITH DELIMITER '	';


COPY act2.devicetemp (did, fid, ddescription)
	FROM '/nfs/bronfs/uwfs/dw00/d41/ktyunho/act2/device.txt' WITH DELIMITER '	';

COPY act2.work_log_temp (id, did, timelogged, e_reading, e, delta_unit, year, moy, woy, dow)
	FROM '/nfs/bronfs/uwfs/dw00/d41/ktyunho/act2/worklog.txt' WITH DELIMITER '	';
*/

	
/*
 * Adds data into act2.device from the temporary table devicetemp and removes legitimate records from the temptable
 */ 
insert into act2.device (did, ddescription, fid)
select * from act2.devicetemp dt where dt.fid in (select f.fid from act2.facility f);

delete from act2.devicetemp dt
	where dt.fid in (select f.fid from act2.facility f);


/*
 * Adds data into act2.work_log from the temporary table work_log_temp and removes legitimate records from the temp table
 */ 
insert into act2.work_log (id, did, timelogged, e_reading, e, delta_unit, year, moy, woy, dow)
select * from act2.work_log_temp wlt where wlt.did in (select d.did from act2.device d);

delete from act2.work_log_temp wlt
	where wlt.did in (select d.did from act2.device d);


-------------------------------------------------------------------------------------------


CREATE TABLE act2.log (
	id 		SERIAL PRIMARY KEY,
	name		varchar(64),
	starttime	timestamp,
	endtime		timestamp,
	query		text
);


CREATE OR REPLACE FUNCTION act2.logquery (
	p_query text,
	p_name varchar(64),
	p_testnumber int
)
RETURNS VOID AS $PROC$	
DECLARE 
	st timestamp;
	ed timestamp;
	testnumber int;
	testcount int;
BEGIN
	testcount:=0;

	LOOP

		st:= (select date_trunc('milliseconds', clock_timestamp()) as "start");
		EXECUTE (p_query);
		ed:= (select date_trunc('milliseconds', clock_timestamp()) as "end");

		INSERT INTO act2.log (name, starttime, endtime, query) values (p_name, st, ed, p_query);

		testcount:= testcount+1;
		
		EXIT WHEN testcount > p_testnumber-1;
	END LOOP;
END;
$PROC$ LANGUAGE plpgsql; 


-------------------------------------------------------------------------------------------


select act2.logquery(
		format('select count(e), sum(e) from act2.work_log where did=143 and timelogged between ''2007-10-01 08:30:00-07'' and ''2007-10-01 09:30:00-07''')
		, 'query1-original'
		, '10'
		);


select l.endtime-l.starttime, l.* from act2.log l;

select * from act2.log l

select avg(l.endtime-l.starttime), l.name
	from act2.log l
	group by l.name;

explain analyze select count(e), sum(e) from act2.work_log where did=143 and timelogged between '2007-10-01 08:30:00-07' and '2007-10-01 09:30:00-07'

/*
DELETE FROM act2.log;
DROP TABLE act2.log;
*/