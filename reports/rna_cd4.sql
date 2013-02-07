/*
Sql query to build a custom report, by patient and collect date, that displays
Viral load, CD4/CD8 data, ARV data, and aliquot in the system.
*/
with viral_load as (
    select p.id as pid, p.our as our, p.legacy_number as aeh_number, e.collect_date, sq.value as quantifier_code, dr.value as result, st.value as test_kit_type
    from entity as e
    join schema as s on e.schema_id = s.id
    join decimal as dr on e.id = dr.entity_id and dr.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'ViralLoad'
        and a.name = 'result'
        )
    join string as sq on e.id = sq.entity_id and sq.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'ViralLoad'
        and a.name = 'quantifier_code'
        )
    join string as st on e.id = st.entity_id and st.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'ViralLoad'
        and a.name = 'test_kit_type'
        )
    join context as c on e.id = c.entity_id and external = 'patient'
    join patient as p on c.key = p.id
    where s.name = 'ViralLoad'
),

lymph as (
    select p.id as pid, p.our as our, p.legacy_number as aeh_number, e.collect_date, d4p.value as cd4_percent, d4a.value as cd4_absolute, d8p.value as cd8_percent, d8a.value as cd8_absolute, r.value as ratio
    from entity as e
    join schema as s on e.schema_id = s.id
    join decimal as d4p on e.id = d4p.entity_id and d4p.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'LymphocyteSubsets'
        and a.name = 'cd4_percent'
        )
    join decimal as d4a on e.id = d4a.entity_id and d4a.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'LymphocyteSubsets'
        and a.name = 'cd4_absolute'
        )
    join decimal as d8p on e.id = d8p.entity_id and d8p.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'LymphocyteSubsets'
        and a.name = 'cd8_percent'
        )
    join decimal as d8a on e.id = d8a.entity_id and d8a.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'LymphocyteSubsets'
        and a.name = 'cd8_absolute'
        )
    join decimal as r on e.id = r.entity_id and r.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'LymphocyteSubsets'
        and a.name = 'ratio'
        )
    join context as c on e.id = c.entity_id and external = 'patient'
    join patient as p on c.key = p.id
    where s.name = 'LymphocyteSubsets'
),

drugs as (
    select p.id as pid
    ,p.our as our
    ,p.legacy_number as aeh_number
    ,cast(dstart.value as date) as start_date
    , MAX(cast(dstop.value as date)) as stop_date
    ,CASE
  WHEN
   dcode.value = '08180006'
  THEN 'AzdU'
  WHEN
   dcode.value = '08180007'
  THEN 'ddI'
  WHEN
   dcode.value = '08180013'
  THEN 'NVP'
  WHEN
   dcode.value = '08180018'
  THEN 'Atevirdine mesylate U-87201E'
  WHEN
  dcode.value = '08180020'
  THEN 'ddC / HIVID'
  WHEN
   dcode.value = '08180021'
  THEN 'AZT / ZDV'
  WHEN
   dcode.value = '08180024'
  THEN 'd4T'
  WHEN
   dcode.value = '08180025'
  THEN 'Alovudine'
  WHEN
   dcode.value = '08180026'
  THEN '3TC'
  WHEN
   dcode.value = '08180030'
  THEN 'SQV'
  WHEN
   dcode.value = '08180031'
  THEN 'DLV'
  WHEN
   dcode.value = '08180032'
  THEN 'CD4/RST4'
  WHEN
   dcode.value = '08180043'
  THEN 'IDV'
  WHEN
   dcode.value = '08180048'
  THEN 'Loviride / Lotrene'
  WHEN
   dcode.value = '08180406'
  THEN 'ADV'
  WHEN
   dcode.value = '08180407'
  THEN 'ABC'
  WHEN
   dcode.value = '08180411'
  THEN 'Fluorouridine'
  WHEN
   dcode.value = '08180412'
  THEN 'Combivir'
  WHEN
   dcode.value = '08180414'
  THEN 'DAPD'
  WHEN
   dcode.value = '08180415'
  THEN 'FTC'
  WHEN
   dcode.value = '08180418'
  THEN '(3TC/ZDV) / ABC'
  WHEN
   dcode.value = '08180420'
  THEN 'Epzicom'
  WHEN
   dcode.value = '08180421'
  THEN 'TDF/FTC'
  WHEN
   dcode.value = '08180422'
  THEN 'EFV/FTC/TDF'
  WHEN
   dcode.value = '08180804'
  THEN 'EFV'
  WHEN
   dcode.value = '08180809'
  THEN 'ETR'
  WHEN
   dcode.value = '08180811'
  THEN 'Rilpivirine / TMC278'
  WHEN
   dcode.value = ' 08180813'
  THEN 'Elvitegravir / GS9137'
  WHEN
   dcode.value = '08180814'
  THEN 'RAL'
  WHEN
   dcode.value = '08180825'
  THEN 'Complera'
  WHEN
   dcode.value = '08181203'
  THEN 'RTV'
  WHEN
   dcode.value = '08181204'
  THEN 'NFV'
  WHEN
   dcode.value = '08181205'
  THEN 'APV'
  WHEN
   dcode.value = '08181208'
  THEN 'LPV / RTV'
  WHEN
   dcode.value = '08181209'
  THEN 'FTV'
  WHEN
   dcode.value = '08181210'
  THEN 'Tipranivir'
  WHEN
   dcode.value = '08181214'
  THEN 'ATV'
  WHEN
   dcode.value = '08181218'
  THEN 'Lexiva / GW433908 / Fosamprenavir'
  WHEN
   dcode.value = '08181220'
  THEN 'DRV'
  WHEN
   dcode.value = '08182002'
  THEN 'TDF'
  WHEN
   dcode.value = '08182403'
  THEN 'MVC'
  WHEN
   dcode.value = '08188804'
  THEN 'ENF'
  WHEN
   dcode.value = '10040005'
  THEN 'Hydroxyurea / Hydrea'
  WHEN
   dcode.value = '10920013'
  THEN 'Interleukin-2 / IL-2'
  WHEN
   dcode.value = '80120055'
  THEN 'Remune cyclosporin A'
  WHEN
   dcode.value = '99999998'
  THEN 'Blinded Study Drug'
  WHEN
   dcode.value = '99999999'
  THEN 'Drug Code Pending'
  ELSE 'Unknown Drug'
  END as drug_code  --,  d4a.value as cd4_absolute, d8p.value as cd8_percent, d8a.value as cd8_absolute, r.value as ratio
    from entity as e
    join schema as s on e.schema_id = s.id
    join datetime as dstart on e.id = dstart.entity_id and dstart.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'ARVMeds'
        and a.name = 'start_date'
        )
    join datetime as dstop on e.id = dstop.entity_id and dstop.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'ARVMeds'
        and a.name = 'stop_date'
        )

    join string as dcode on e.id = dcode.entity_id and dcode.attribute_id in (
        select a.id
        from attribute as a
        join schema as s on a.schema_id = s.id
        where s.name = 'ARVMeds'
        and a.name = 'drug'
        )
    join context as c on e.id = c.entity_id and external = 'patient'
    join patient as p on c.key = p.id
    where s.name = 'ARVMeds'
    group by p.id, p.our, p.legacy_number, dstart.value, dcode.value
    order by p.id
),


aliquot as (
select p.id as pid,
    p.our as our
    ,p.legacy_number as aeh_number
    ,s.collect_date as collect_date

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'pbmc'
        ) AS pbmc_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'plasma'
        ) AS plasma_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'csf'
        ) AS csf_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'csfpellet'
        ) AS csfpellet_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'gscells'
        ) AS gscells_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'gsplasma'
        ) AS gsplasma_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'urine'
        ) AS urine_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'serum'
        ) AS serum_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'swab'
        ) AS swab_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'ti_gut'
        ) AS ti_gut_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'rs_gut'
        ) AS rs_gut_count

    ,(SELECT COUNT(*)
       from aliquot as a
         join specimen as sp on a.specimen_id = sp.id
         join aliquotstate as aqs on aqs.id = a.state_id
         join aliquottype as at on at.id = a.aliquot_type_id
           WHERE sp.patient_id = p.id and s.collect_date = sp.collect_date
       and aqs.name = 'checked-in'
       and at.name = 'lymphoid'
        ) AS lymph_count
    from specimen as s
    join patient as p on p.id = s.patient_id
    group by p.id, p.our, p.legacy_number, s.collect_date
),

cd4_rna as (
select coalesce(vl.pid, l.pid) as pid
    ,coalesce(vl.our, l.our) as our
    ,coalesce(vl.aeh_number, l.aeh_number) as aeh_number
    ,coalesce(vl.collect_date, l.collect_date, NULL) as collect_date
    ,vl.quantifier_code
    ,vl.result
    ,vl.test_kit_type
       ,l.cd4_percent
       ,l.cd4_absolute
       ,l.cd8_percent
       ,l.cd8_absolute
       ,l.ratio
from lymph as l
full join viral_load as vl on vl.pid = l.pid and vl.collect_date = l.collect_date
where coalesce(vl.collect_date, l.collect_date, NULL)  is not NULL
),

cd4_rna_drug as (
select
    cr.pid as pid
    ,cr.our as our
    ,cr.aeh_number as aeh_number
    ,cr.collect_date as collect_date
    ,cr.quantifier_code as quantifier_code
    ,cr.result as rna_result
    ,cr.test_kit_type as rna_test_kit_type
       ,cr.cd4_percent as cd4_percent
       ,cr.cd4_absolute as cd4_absolute
       ,cr.cd8_percent as cd8_percent
       ,cr.cd8_absolute as cd8_absolute
       ,cr.ratio as cd4_cd8_ratio
       ,array_to_string(array_agg(distinct d.drug_code), ' / ') as drug_code

from cd4_rna as cr
left join drugs as d on cr.pid = d.pid and cr.collect_date >= d.start_date and (d.stop_date is NULL or cr.collect_date <= d.stop_date)
--where coalesce(cr.collect_date, d.collect_date, NULL) is not NULL
group by cr.pid, cr.our, cr.aeh_number, cr.collect_date, quantifier_code, rna_result, rna_test_kit_type, cd4_percent, cd4_absolute, cd8_percent, cd8_absolute, cd4_cd8_ratio
order by cr.our, cr.collect_date
)

select
    coalesce(cr.our, a.our) as our
    ,coalesce(cr.aeh_number, a.aeh_number) as aeh_number
    ,coalesce(cr.collect_date, a.collect_date) as collect_date
    ,cr.quantifier_code as quantifier_code
    ,cr.rna_result as rna_result
    ,cr.rna_test_kit_type as rna_test_kit_type
       ,cr.cd4_percent as cd4_percent
       ,cr.cd4_absolute as cd4_absolute
       ,cr.cd8_percent as cd8_percent
       ,cr.cd8_absolute as cd8_absolute
       ,cr.cd4_cd8_ratio as cd4_cd8_ratio
  ,cr.drug_code as drug_code
       ,a.pbmc_count
       ,a.plasma_count
       ,a.csf_count
       ,a.csfpellet_count
       ,a.gscells_count
       ,a.gsplasma_count
       ,a.urine_count
       ,a.serum_count
       ,a.swab_count
       ,a.ti_gut_count
       ,a.rs_gut_count
       ,a.lymph_count

from cd4_rna_drug as cr
full join aliquot as a on cr.pid = a.pid and cr.collect_date = a.collect_date
where coalesce(cr.collect_date, a.collect_date, NULL) is not NULL
order by our, collect_date


