select
s.block_number as BLOCKID,
a.title as ARM,
--st.title as STUDY
sr.value as SITE,
s.reference_number as RANDID,
p.our as PATIENT

from entity e
join string sr on sr.entity_id = e.id
join context c on c.entity_id = e.id
join stratum s on c.key = s.id and c.external = 'stratum'
left join patient p on s.patient_id = p.id
join study st on s.study_id = st.id
join arm a on s.arm_id = a.id
