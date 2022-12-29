with temp as (select g.Continents, sum(cast(a.IsCorrect as int)) as continental_correct, count(u.UserId) as continental_user_count
			  from Answers a 
			  join [User] u on u.UserId = a.UserId
			  join [Geography] g on g.GeoId = u.GeoId
			  group by g.Continents),

temp2 as (select u.UserId, 
				 g.Continents, 
				 (sum(cast(a.IsCorrect as int)) / (cast(t.continental_correct as float) / t.continental_user_count)) as ratio,
				 max((sum(cast(a.IsCorrect as int)) / (cast(t.continental_correct as float) / t.continental_user_count))) over(partition by g.Continents) as max_ratio_cont
		  from Answers a 
		  join [User] u on u.UserId = a.UserId
		  join [Geography] g on g.GeoId = u.GeoId
		  join temp t on t.Continents = g.Continents
		  group by u.UserId, g.Continents, t.continental_correct, t.continental_user_count)

select t.UserId, t.Continents, t.ratio
from temp2 t
where t.ratio = t.max_ratio_cont