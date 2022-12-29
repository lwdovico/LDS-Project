with temp as (select SubjectId, UserId, sum(cast(IsCorrect as int)) as n_correct, max(sum(cast(IsCorrect as int))) over(partition by SubjectId) as max_correct
from Answers
group by UserId, SubjectId)
select SubjectId, UserId, n_correct
from temp
where n_correct = max_correct and n_correct > 0
order by n_correct desc