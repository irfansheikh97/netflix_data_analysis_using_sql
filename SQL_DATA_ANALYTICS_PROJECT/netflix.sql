--removing duplicates
select * from netflix_raw_data
where concat(upper(title),type)  in (
select concat(upper(title),type) 
from netflix_raw_data
group by upper(title) ,type
having COUNT(*)>1
)
order by title

-- Finding Duplicates:
-- The first part (lines 2-9) identifies duplicate title-type combinations. It does this by:
-- Concatenating the uppercase versions of title and type columns (lines 3-4).
-- Grouping the data by the concatenated string (line 6).
-- Using the HAVING COUNT(*) > 1 clause to filter for groups with more than one record (line 7). This indicates duplicate titles within a specific type.

DELETE FROM netflix_raw_data
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY UPPER(title) || UPPER(type) ORDER BY show_id) AS rn
        FROM netflix_raw_data
    ) AS ranked_titles
    WHERE ranked_titles.rn > 1
    AND netflix_raw_data.show_id = ranked_titles.show_id
);
-- EXISTS Subquery: The DELETE statement now uses an EXISTS subquery instead of directly referencing the ranked_titles CTE in the WHERE clause.
-- Matching show_id: The subquery joins the netflix_data table with the ranked_titles CTE based on the show_id column. This ensures that only rows from the original table that correspond to duplicate title-type combinations (identified by rn > 1 in the subquery) are deleted.


--new table for listed_in, director, country,cast
SELECT 
    show_id,
    TRIM(value) AS director
into netflix_directors
FROM 
    netflix_raw_data,
    UNNEST(string_to_array(director, ',')) AS value;
	
SELECT 
    show_id,
    TRIM(value) AS country
into netflix_country
FROM 
    netflix_raw_data,
    UNNEST(string_to_array(country, ',')) AS value;
	
SELECT 
    show_id,
    TRIM(value) AS genre
into netflix_genre
FROM 
    netflix_raw_data,
    UNNEST(string_to_array(listed_in, ',')) AS value;
	
-- Use UNNEST and string_to_array to split columns that contain multiple values separated by a delimiter.

--populate missing values in country,duration columns
select director,country
from  netflix_country nc
inner join netflix_directors nd on nc.show_id=nd.show_id
group by director,country
order by director

INSERT into netflix_country
SELECT nr.show_id, m.country
FROM netflix_raw_data nr
LEFT JOIN (
  SELECT nd.director, nc.country
  FROM netflix_directors nd
  INNER JOIN netflix_country nc ON nd.show_id = nc.show_id
) AS m ON nr.director = m.director
WHERE nr.country IS NULL;

-- orignal query
with cte as 
(SELECT *, ROW_NUMBER() OVER (PARTITION BY UPPER(title) || UPPER(type) ORDER BY show_id) AS rn 
from netflix_raw_data
)
select show_id, type, title, cast(date_added as date) as date_added, release_year, rating,
case when duration is null then rating else duration end as duration, description
into netflix
from cte

--netflix data analysis

-- 1  for each director count the no of movies and tv shows created by them in separate columns 
-- for directors who have created tv shows and movies both 

select nd.director 
,COUNT(distinct case when n.type='Movie' then n.show_id end) as no_of_movies
,COUNT(distinct case when n.type='TV Show' then n.show_id end) as no_of_tvshow
from netflix n
inner join netflix_directors nd on n.show_id=nd.show_id
group by nd.director
having COUNT(distinct n.type)>1

-- 2 which country has highest number of comedy movies
select distinct(nc.country), count(ng.genre = 'Comedies') from netflix_country as nc
inner join netflix_genre as ng on nc.show_id = ng.show_id
group by nc.country, ng.genre

select  nc.country , COUNT(distinct ng.show_id ) as no_of_movies
from netflix_genre ng
inner join netflix_country nc on ng.show_id=nc.show_id
inner join netflix n on ng.show_id=nc.show_id
where ng.genre='Comedies' and n.type='Movie'
group by  nc.country
order by no_of_movies desc
LIMIT 1;

--3 for each year (as per date added to netflix), which director has maximum number of movies released

with cte as (
select nd.director, EXTRACT(YEAR from date_added) as date_year,count(n.show_id) as no_of_movies
from netflix n
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie'
group by nd.director,EXTRACT(YEAR from date_added)
)
, cte2 as (
select *
, ROW_NUMBER() over(partition by date_year order by no_of_movies desc, director) as rn
from cte
)
select * from cte2 where rn=1


--4 what is average duration of movies in each genre
select ng.genre , round(avg(cast(REPLACE(duration,' min','') AS int))) as avg_duration
from netflix n
inner join netflix_genre ng on n.show_id=ng.show_id
where type='Movie'
group by ng.genre

--5  find the list of directors who have created horror and comedy movies both.
-- display director names along with number of comedy and horror movies directed by them 
select nd.director
, count(distinct case when ng.genre='Comedies' then n.show_id end) as no_of_comedy 
, count(distinct case when ng.genre='Horror Movies' then n.show_id end) as no_of_horror
from netflix n
inner join netflix_genre ng on n.show_id=ng.show_id
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie' and ng.genre in ('Comedies','Horror Movies')
group by nd.director
having COUNT(distinct ng.genre)=2;