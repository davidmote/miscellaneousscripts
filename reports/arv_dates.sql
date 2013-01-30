/*
Sql query to collate the drug data from the bbl database and clean it so that
start_date is actually the start date, stop date is actually the stop date,
using the drug dosage status as a key to determine what the dates actually mean
*/

with AEH as (
select
    r.aehid as aeh_number
    ,CAST(e.ehvisz as DATE) as visit_date
    ,CASE
    WHEN e.eharvss = 2
    THEN '99999998'
    ELSE
    e.ehmed1i
    END as drug
    ,CASE
    WHEN e.eharvdd in (1,5,6) and e.eharvbz != '-4'
    THEN CAST(e.eharvbz as DATE)
    WHEN e.eharvsz !='-4' and e.eharvbz !='-4' and CAST(e.eharvsz as DATE) > CAST(e.eharvbz as DATE)
    THEN CAST(e.eharvbz as DATE)
    WHEN
    ((e.eharvdd in (4, 7))
        or (e.eharvsz !='-4' and e.eharvbz !='-4' and CAST(e.eharvsz as DATE) = CAST(e.eharvbz as DATE))
    ) and
    (
    SELECT count(*)
        from aeh_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd in (1, 5, 6)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    ) > 0
    THEN (
    SELECT CAST(ae.eharvbz as DATE)
        from aeh_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd in (1, 5, 6)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    )
    WHEN
    ((e.eharvdd in (4, 7))
        or (e.eharvsz !='-4' and e.eharvbz !='-4' and CAST(e.eharvsz as DATE) = CAST(e.eharvbz as DATE))
    ) and
    (
    SELECT count(*)
        from aeh_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd not in (4,7)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    ) > 0
    THEN (
    SELECT CAST(ae.eharvbz as DATE)
        from aeh_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd not in (4,7)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    )

    WHEN e.eharvbz !='-4' and e.eharvdd in (-4, 8) and e.eharvt = 1
    THEN CAST(e.eharvbz as DATE)
    ELSE NULL
        END as start_date
    ,CASE
    WHEN e.eharvsz != '-4'
    THEN CAST(e.eharvsz as DATE)
    WHEN e.eharvdd in (4,7) and e.eharvbz != '-4'
    THEN CAST(e.eharvbz as DATE)
    ELSE
    NULL
    END as stop_date
    ,CASE
     when e.ehpsm > 11
     then l.eharv1t
     else NULL
     END as adverse_event
     ,CASE
    WHEN e.eharvtg = 1
    then 'mild'
    when e.eharvtg = 2
    then 'moderate'
    WHEN e.eharvtg = 3
    then 'severe'
    when e.eharvtg = 4
    then 'life threatening'
    end as adverse_event_grade
from aeh_eharv01 e
join aeh_roster r on r.rid = e.rid
left join aeh_eharvl1 l on l.eharv1i = e.ehpsm
where e.ehmed1i != '-4'
and r.aehid != 'TEST'
and (e.entry, e.verify) = (4,4)
and not (e.eharvbz ='-4' and e.eharvsz = '-4')
and e.eharvdd not in (2, 3)
),
 AEHPART as (
select
    r.aehid as aeh_number
    ,CAST(e.ehvisz as DATE) as visit_date
    ,CASE
    WHEN e.eharvss = 2
    THEN '99999998'
    ELSE
    e.ehmed1i
    END as drug
    ,CASE
    WHEN e.eharvdd in (1,5,6) and e.eharvbz != '-4'
    THEN CAST(e.eharvbz as DATE)
    WHEN e.eharvsz !='-4' and e.eharvbz !='-4' and CAST(e.eharvsz as DATE) > CAST(e.eharvbz as DATE)
    THEN CAST(e.eharvbz as DATE)
    WHEN
    ((e.eharvdd in (4, 7))
        or (e.eharvsz !='-4' and e.eharvbz !='-4' and CAST(e.eharvsz as DATE) = CAST(e.eharvbz as DATE))
    ) and
    (
    SELECT count(*)
        from aehpart_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd in (1, 5, 6)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    ) > 0
    THEN (
    SELECT CAST(ae.eharvbz as DATE)
        from aehpart_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd in (1, 5, 6)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    )
    WHEN
    ((e.eharvdd in (4, 7))
        or (e.eharvsz !='-4' and e.eharvbz !='-4' and CAST(e.eharvsz as DATE) = CAST(e.eharvbz as DATE))
    ) and
    (
    SELECT count(*)
        from aehpart_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd not in (4,7)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    ) > 0
    THEN (
    SELECT CAST(ae.eharvbz as DATE)
        from aehpart_eharv01 ae
        where ae.eharvbz != '-4'
        and e.rid = ae.rid
        and e.ehmed1i = ae.ehmed1i
        and ae.eharvdd not in (4,7)
        and (ae.entry, ae.verify) = (4,4)
        and CAST(ae.eharvbz as DATE) < CAST(e.ehvisz as DATE)
        group by ae.rid, ae.ehmed1i, ae.ehvisz, ae.eharvbz
        order by CAST(ae.ehvisz as DATE) desc
        limit 1
    )

    WHEN e.eharvbz !='-4' and e.eharvdd in (-4, 8) and e.eharvt = 1
    THEN CAST(e.eharvbz as DATE)
    ELSE NULL
        END as start_date
    ,CASE
    WHEN e.eharvsz != '-4'
    THEN CAST(e.eharvsz as DATE)
    WHEN e.eharvdd in (4,7) and e.eharvbz != '-4'
    THEN CAST(e.eharvbz as DATE)
    ELSE
    NULL
    END as stop_date
    ,CASE
     when e.ehpsm > 11
     then l.eharv1t
     else NULL
     END as adverse_event
     ,CASE
    WHEN e.eharvtg = 1
    then 'mild'
    when e.eharvtg = 2
    then 'moderate'
    WHEN e.eharvtg = 3
    then 'severe'
    when e.eharvtg = 4
    then 'life threatening'
    end as adverse_event_grade
from aehpart_eharv01 e
join aehpart_roster r on r.rid = e.rid
left join aeh_eharvl1 l on l.eharv1i = e.ehpsm
where e.ehmed1i != '-4'
and r.aehid != 'TEST'
and (e.entry, e.verify) = (4,4)
and not (e.eharvbz ='-4' and e.eharvsz = '-4')
and e.eharvdd not in (2, 3)
),

all_drugs as (
select * from AEH
union all
select * from AEHPART
)

select * from all_drugs
group by aeh_number, visit_date, start_date, stop_date, drug, adverse_event, adverse_event_grade
order by aeh_number, visit_date