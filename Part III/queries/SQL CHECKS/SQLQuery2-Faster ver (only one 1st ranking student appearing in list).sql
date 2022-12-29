with temp as (select UserId, SubjectId, sum(cast(IsCorrect as int)) as n_correct, max(sum(cast(IsCorrect as int))) over(partition by SubjectId) as max_correct
from Answers
group by UserId, SubjectId),
temp2 as (select SubjectId, UserId, n_correct, row_number() over(partition by SubjectId order by max_correct desc) as rnk
from temp
where n_correct = max_correct)
select SubjectId, UserId, n_correct, rnk
from temp2
where rnk = 1 and n_correct > 0
order by n_correct desc