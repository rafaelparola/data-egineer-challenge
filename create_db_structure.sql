create table if not exists D_REGION 
(d_region_id integer primary key autoincrement
 , region text
 , execution_datetime text);

create table if not exists D_DATASOURCE 
(d_datasource_id integer primary key autoincrement
 , datasource text
 , execution_datetime text);
 
 create table if not exists D_DATES 
(d_date_id integer primary key autoincrement
 , datetime text
 , execution_datetime text);

create table if not exists D_COORD 
(d_coord_id integer primary key autoincrement
 , d_region_id integer
 , origin_coord_lat real
 , origin_coord_lon real
 , dst_coord_lat real
 , dst_coord_lon real
 , execution_datetime text
	, FOREIGN KEY (d_region_id)
       REFERENCES D_REGION (d_region_id) );

create table if not exists F_TRIPS 
(id integer primary key autoincrement
 , d_coord_id integer not null
 , d_datasource_id integer not null
 , d_date_id integer not null
 , count_grouped integer
 , execution_datetime text,
FOREIGN KEY (d_coord_id)
       REFERENCES D_COORD (d_coord_id) ,
FOREIGN KEY (d_datasource_id)
       REFERENCES D_DATASOURCE (d_datasource_id),
FOREIGN KEY (d_date_id)
       REFERENCES D_DATES (d_date_id));
