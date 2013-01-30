/*
Query used to collate and display drug data from the bbl aeh_eharv01 table.
based on a faulty assumption that begin_date actually meant beginning date.
This, essentially, is junk, because the data wasn't recorded in this fashion.
*/

select * from aeh_datadic
order by tblname;

 select fldname,tblname, text, code, notes from aeh_datadic
where tblname = 'EHARV01'
and text is not NULL;

select * from aehpart_eharv01 e
join aehpart_roster r on r.id = e.rid;

select r.aehid as aeh_number,
       e.ehvisz as visit_date,
       --e.arvtxstat,
       CASE
	WHEN e.arvtxstat = 1
	    THEN 'prep'
	WHEN e.arvtxstat = 2
	    THEN 'pep'
	END as reason,
       CASE
	WHEN e.eharvbz = '-4'
	    THEN (SELECT ae.eharvbz
		    from aeh_eharv01 as ae
		    where e.rid = ae.rid
		    and e.ehmed1i = ae.ehmed1i
		    and ae.ehmed1i != '-4'
		    and ae.eharvbz != '-4'
		    and eharvdd = 1
		    order by ae.ehvisz asc
		    limit 1
		    )
	ELSE e.eharvbz
	END as start_date,
       CASE
       WHEN e.eharvsz != '-4'
            THEN e.eharvsz
	WHEN e.eharvsz = '-4' and e.eharvdd = 7
	    THEN e.ehvisz

	else NULL
	END as stop_date,
       e.eharvt,
       e.ehscarv,
       e.eharvbz,
       e.eharvsz,
       e.eharvdd,
       e.ehmed1i as drug
       --array_agg(e.ehmed1i),
       --array_agg(e.EHARVMS)
from aeh_eharv01 e
join aeh_roster r on r.id = e.rid
where (e.entry, e.verify) = (4,4)
and e.ehmed1i != '-4'
--and e.arvtxstat not in (1,2)
--and e.eharvdd not in (4,7)
--and (e.eharvt=1 or e.ehscarv =1 or e.eharvon = 1)
--and e.eharvss not in (2,3)
--and (e.eharvbz = '-4' or CAST(e.eharvbz as DATE) < CAST(e.ehvisz as DATE))
--and (e.eharvsz = '-4' or CAST(e.eharvsz as DATE) > CAST(e.ehvisz as DATE))
and r.aehid != 'TEST'
--group by r.aehid, e.ehvisz, e.eharvbz, e.eharvsz
order by r.aehid, CAST(e.ehvisz as DATE)
;


select
    r.aehid as aeh_number
,CAST(e.ehvisz as DATE) as visit_date
       ,CASE
    WHEN e.arvtxstat = 1
        THEN 'prep'
    WHEN e.arvtxstat = 2
        THEN 'pep'
    WHEN e.arvtxstat = 3
        THEN 'initial_ongoing'
    WHEN e.arvtxstat = 4
        THEN 'continuing'
    END as treatment_intent
    ,CASE
    WHEN e.eharvbz = '-4'
    THEN NULL
/*      THEN (SELECT ae.eharvbz
            from aeh_eharv01 as ae
            where e.rid = ae.rid
            and e.ehmed1i = ae.ehmed1i
            and ae.ehmed1i != '-4'
            and ae.eharvbz != '-4'
            and eharvdd = 1
            order by ae.ehvisz asc
            limit 1
            )
*/  ELSE CAST(e.eharvbz as DATE)
    END as start_date,
       CASE
       WHEN e.eharvsz != '-4'
            THEN CAST(e.eharvsz as DATE)
/*  WHEN e.eharvsz = '-4' and e.eharvdd = 7
        THEN e.ehvisz
*/

    else NULL
    END as stop_date
    ,e.ehmed1i as drug
    ,CASE
    WHEN e.eharvdd = 1
        then 'initial dose'
    WHEN e.eharvdd = 2
        then 'reduced dose'
    WHEN e.eharvdd = 3
        then 'increased dose'
    WHEN e.eharvdd = 4
        then 'temporarily off'
    WHEN e.eharvdd = 5
        then 'resuming at previous dose'
    WHEN e.eharvdd = 6
        then 'resuming at lower dose'
    WHEN e.eharvdd = 7
        then 'permanently off'
    WHEN e.eharvdd = 8
        then 'continuing'
    END as dose_status
    ,CASE
    WHEN e.eharvss = 1
        then 'active'
    when e.eharvss = 2
        then 'placebo or blinded'
    when e.eharvss = 3
        then 'discontinued'
    end as drug_status
    ,CASE
    when e.eharvt = 1
        then 'yes'
    when e.eharvt = 0
        then 'no'
    end as currently_on_medication
    ,case
    when e.ehscarv = 1
        then 'yes'
    when e.ehscarv = 0
        then 'no'
    end as receive_arv_since_last_visit
    ,case
    when e.eharvon = 1
        then 'yes'
    when e.eharvon = 0
        then 'no'
    end as meds_ongoing
    ,case
    when e.eharvmr!= '-4'
        then e.eharvmr
    end as notes
    ,case
    when e.eharvpr = 1
        then 'yes'
    when e.eharvpr = 0
        then 'no'
    end as previously_reported
    ,case
     when e.eharvmg != -4
     then e.eharvmg
     end as daily_dose
from aeh_eharv01 e
join aeh_roster r on r.id = e.rid
where (e.entry, e.verify) = (4, 4)
--and e.ehmed1i != '-4'
and r.aehid != 'TEST'
union all
select
    r.aehid as aeh_number
,CAST(e.ehvisz as DATE) as visit_date
       ,CASE
    WHEN e.arvtxstat = 1
        THEN 'prep'
    WHEN e.arvtxstat = 2
        THEN 'pep'
    WHEN e.arvtxstat = 3
        THEN 'initial_ongoing'
    WHEN e.arvtxstat = 4
        THEN 'continuing'
    END as treatment_intent
    ,CASE
    WHEN e.eharvbz = '-4'
    THEN NULL
/*      THEN (SELECT ae.eharvbz
            from aeh_eharv01 as ae
            where e.rid = ae.rid
            and e.ehmed1i = ae.ehmed1i
            and ae.ehmed1i != '-4'
            and ae.eharvbz != '-4'
            and eharvdd = 1
            order by ae.ehvisz asc
            limit 1
            )
*/  ELSE CAST(e.eharvbz as DATE)
    END as start_date,
       CASE
       WHEN e.eharvsz != '-4'
            THEN CAST(e.eharvsz as DATE)
/*  WHEN e.eharvsz = '-4' and e.eharvdd = 7
        THEN e.ehvisz
*/

    else NULL
    END as stop_date
    ,e.ehmed1i as drug
    ,CASE
    WHEN e.eharvdd = 1
        then 'initial dose'
    WHEN e.eharvdd = 2
        then 'reduced dose'
    WHEN e.eharvdd = 3
        then 'increased dose'
    WHEN e.eharvdd = 4
        then 'temporarily off'
    WHEN e.eharvdd = 5
        then 'resuming at previous dose'
    WHEN e.eharvdd = 6
        then 'resuming at lower dose'
    WHEN e.eharvdd = 7
        then 'permanently off'
    WHEN e.eharvdd = 8
        then 'continuing'
    END as dose_status
    ,CASE
    WHEN e.eharvss = 1
        then 'active'
    when e.eharvss = 2
        then 'placebo or blinded'
    when e.eharvss = 3
        then 'discontinued'
    end as drug_status
    ,CASE
    when e.eharvt = 1
        then 'yes'
    when e.eharvt = 0
        then 'no'
    end as currently_on_medication
    ,case
    when e.ehscarv = 1
        then 'yes'
    when e.ehscarv = 0
        then 'no'
    end as receive_arv_since_last_visit
    ,case
    when e.eharvon = 1
        then 'yes'
    when e.eharvon = 0
        then 'no'
    end as meds_ongoing
    ,case
    when e.eharvmr!= '-4'
        then e.eharvmr
    end as notes
    ,case
    when e.eharvpr = 1
        then 'yes'
    when e.eharvpr = 0
        then 'no'
    end as previously_reported
    ,case
     when e.eharvmg != -4
     then e.eharvmg
     end as daily_dose
from aehpart_eharv01 e
join aehpart_roster r on r.id = e.rid
where (e.entry, e.verify) = (4, 4)
--and e.ehmed1i != '-4'
and r.aehid != 'TEST'
group by aeh_number,
    visit_date,
    treatment_intent,
    start_date,
    stop_date,
    drug,
    dose_status,
    drug_status,
    currently_on_medication,
    receive_arv_since_last_visit,
    meds_ongoing, notes,
    previously_reported,
    daily_dose
order by aeh_number, visit_date, drug;



