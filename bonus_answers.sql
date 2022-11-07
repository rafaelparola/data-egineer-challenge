-- BONUS ANSWERS
--1)From the two most commonly appearing regions, which is the latest datasource?

select ds.datasource from F_TRIPS f
join D_DATASOURCE ds on f.d_datasource_id = ds.d_datasource_id
join D_DATES dd on f.d_date_id = dd.d_date_id
join D_COORD dc on f.d_coord_id = dc.d_coord_id
where dc.d_region_id in (
select  dr.d_region_id as d_region_id
from F_TRIPS f
join D_COORD dc on f.d_coord_id = dc.d_coord_id
join D_REGION dr on dc.d_region_id = dr.d_region_id
group by dr.region
order by sum(count_grouped) desc 
limit 2
)order by datetime desc
limit 1

--2)What regions has the "cheap_mobile" datasource appeared in?

select distinct dr.region from F_TRIPS f
join D_COORD dc on f.d_coord_id = dc.d_coord_id
join D_REGION dr on dc.d_region_id = dr.d_region_id
join D_DATASOURCE ds on f.d_datasource_id = ds.d_datasource_id
where ds.datasource = 'cheap_mobile';

