with temp as (select g.CountryName, 
					u.UserId, 
					sum(1-cast(IsCorrect as int)) as Wrong, 
					max(sum(1-cast(IsCorrect as int))) over(partition by g.CountryName) as Max_Wrong
			  from [Answers] a 
			  join [User] u on u.UserId = a.UserId
			  join [Geography] g on u.GeoId = g.GeoId
			  group by g.CountryName, u.UserId)
select CountryName, UserId, Wrong
from temp
where Wrong = Max_Wrong
order by CountryName asc