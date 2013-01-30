/*
Script to locate patient OUR#s that could apply to the same patient based
on PHI.
*/
with bday as ( select  p.id as id, dt.value as birthday
from datetime as dt
join attribute as a on dt.attribute_id = a.id
join object as o on dt.entity_id = o.value
join context as c on o.entity_id = c.entity_id and external = 'patient'
join patient as p on c.key = p.id
where a.name = 'birth_date'
),
 l as ( select  p.id as id, btrim(lower(s.value), '"') as name
from string as s
join attribute as a on s.attribute_id = a.id
join object as o on s.entity_id = o.value
join context as c on o.entity_id = c.entity_id and external = 'patient'
join patient as p on c.key = p.id
where a.name = 'last_name'
),
 m as ( select  p.id as id, substr(lower(s.value), 1, 1) as name
from string as s
join attribute as a on s.attribute_id = a.id
join object as o on s.entity_id = o.value
join context as c on o.entity_id = c.entity_id and external = 'patient'
join patient as p on c.key = p.id
where a.name = 'middle_initial'
),
 f as ( select  p.id as id, lower(s.value) as name
from string as s
join attribute as a on s.attribute_id = a.id
join object as o on s.entity_id = o.value
join context as c on o.entity_id = c.entity_id and external = 'patient'
join patient as p on c.key = p.id
where a.name = 'first_name'
),
 fi as ( select  p.id as id, substr(lower(s.value), 1, 1) as name
from string as s
join attribute as a on s.attribute_id = a.id
join object as o on s.entity_id = o.value
join context as c on o.entity_id = c.entity_id and external = 'patient'
join patient as p on c.key = p.id
where a.name = 'first_name'
),
results as (
select count(distinct patient.our) as count, bday.birthday as bday, array_agg(distinct f.name) as firstnames, fi.name as first_initial, l.name as lastname, array_agg(distinct patient.our) as our_numbers
from patient
join bday on bday.id = patient.id
left join l on l.id = patient.id
left join f on f.id = patient.id
left join fi on fi.id = patient.id
--left join m on m.id = patient.id
group by bday.birthday, l.name, fi.name --, m.name
order by l.name desc
)
select  our_numbers from results where count > 1