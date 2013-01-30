/*
Sql query to locate drugs in the old OCCAMS.drug table in a report like fashion
*/
select  du.id,
    p.id,
    p.our,
    dt.code,
    array_to_string(array_agg(distinct(dn.value)), ' / '),
    du.start_date,
    du.stop_date,
    case when du.notes ilike '%PREP%'
    then 'prep'
    when du.notes ilike '%PEP%'
    then 'pep'
    when du.notes ilike '%Post exposure prophylaxis%'
    then 'pep'
    end as reason,
    dt.code = '99999998' as is_blinded,
    du.notes,
    count(v.visit_date),
    array_agg(distinct(v.visit_date)),
    array_agg(v.id),
    CAST(du.create_date as DATE)
from drug_usage du
join patient p on p.id = du.patient_id
join drug_name dn on du.drug_type_id = dn.drug_type_id
join drug_type dt on du.drug_type_id = dt.id
left join visit v on v.patient_id = p.id and (
			( CAST(v.visit_date as DATE) >= CAST(du.start_date as DATE)
			  and (
				CAST(du.stop_date as DATE) >= CAST(v.visit_date as DATE)
				or du.stop_date is NULL
				)
			)
			 or CAST(du.create_date as DATE) = CAST(v.create_date as DATE)
			)

group by du.id, p.id, p.our, dt.code, du.stop_date, du.start_date, du.notes, du.create_date
order by p.id, CAST(start_date as DATE), code
