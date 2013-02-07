#!/Users/davidmote/Environment/mojojojo/bin/zopepy

from avrc.aeh import FiaSession
from avrc.aeh import PhiSession
from avrc.aeh import model
from occams.datastore import model as datastore
from sqlalchemy import func

def main():
    dupes = PhiSession.execute("""
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
where length(l.name) > 1
group by bday.birthday, l.name, fi.name --, m.name
order by count desc
)
select  our_numbers from results where count > 1""")


#     multi_tester_query = FiaSession.execute(
# """
#     select p.our,
#     count(s.name) as study_count,
#     (select count(st.name)
#         from enrollment en
#         join study st on st.id = en.study_id
#         join patient pa on pa.id = en.patient_id
#         where st.name in ('early-test', 'lead-the-way')
#         and p.our = pa.our
#     ) as early_test_count,
#     (select en.consent_date
#         from enrollment en
#         join study st on st.id = en.study_id
#         join patient pa on pa.id = en.patient_id
#         where st.name in ('early-test', 'lead-the-way')
#         and p.our = pa.our
#         order by en.consent_date
#         limit 1
#     ) as first_early_test,
#     (select en.consent_date
#         from enrollment en
#         join study st on st.id = en.study_id
#         join patient pa on pa.id = en.patient_id
#         where st.name in ('early-test', 'lead-the-way')
#         and p.our = pa.our
#         order by en.consent_date desc
#         limit 1
#     ) as last_early_test,
#     (select en.consent_date
#         from enrollment en
#         join study st on st.id = en.study_id
#         join patient pa on pa.id = en.patient_id
#         where st.name in ('020-positive')
#         and p.our = pa.our
#         order by en.consent_date desc
#         limit 1
#     ) as "020-positive",
#     (select en.consent_date
#         from enrollment en
#         join study st on st.id = en.study_id
#         join patient pa on pa.id = en.patient_id
#         where st.name in ('020-negative')
#         and p.our = pa.our
#         order by en.consent_date
#         limit 1
#     ) as "020-negative",
#     array_agg(s.name) as studies --, e.consent_date
#     from patient p
#     join enrollment e on p.id = e.patient_id
#     join study s on s.id = e.study_id
#     where s.name in ('early-test', 'lead-the-way','020-positive', '020-negative')
#     group by p.our""")

    patient_et = (
        FiaSession.query(model.Patient.our.label('our'),
                 func.min(model.Enrollment.consent_date).label('first_et'),
                 func.max(model.Enrollment.consent_date).label('last_et'),
                 func.count(model.Enrollment.id).label('et_count'))
        .join(model.Enrollment.patient)
        .join(model.Enrollment.study)
        .filter(model.Study.name.in_(['early-test', 'lead-the-way', 'cvct-testing-together']))
        .group_by(model.Patient.our)
        ).subquery('patient_et')

    patient_020 =  (
        FiaSession.query(model.Patient.our.label('our'), func.max(model.Enrollment.consent_date).label('positive_020'))
        .join(model.Enrollment.patient)
        .join(model.Enrollment.study)
        .filter(model.Study.name.in_(['020-positive']))
        .group_by(model.Patient.our)
        ).subquery('patient_020')

    negative_020 =  (
        FiaSession.query(model.Patient.our.label('our'), func.max(model.Enrollment.consent_date).label('negative_020'))
        .join(model.Enrollment.patient)
        .join(model.Enrollment.study)
        .filter(model.Study.name.in_(['020-negative']))
        .group_by(model.Patient.our)
        ).subquery('negative_020')

    all_testers = (
                    FiaSession.query(model.Patient.our.label('our'),
                                     patient_et.c.first_et.label('first_et'),
                                      patient_et.c.last_et.label('last_et'),
                                      patient_et.c.et_count.label('et_count'),
                                      patient_020.c.positive_020.label('positive'),
                                      negative_020.c.negative_020.label('negative'))
                    .outerjoin(patient_et, patient_et.c.our == model.Patient.our)
                    .outerjoin(patient_020, patient_020.c.our == model.Patient.our)
                    .outerjoin(negative_020, negative_020.c.our == model.Patient.our)
                    .subquery('all_testers'))


    outlist = []

    covered = []

    for entrylist in dupes:
        entrylist = entrylist[0]
        if len(entrylist) > 1:
            covered.extend(entrylist)
            first_et, last_et, et_count, positive, negative = (
                FiaSession.query(
                    func.min(all_testers.c.first_et),
                    func.max(all_testers.c.last_et),
                    func.sum(all_testers.c.et_count),
                    func.max(all_testers.c.positive),
                    func.min(all_testers.c.negative))
                .filter(all_testers.c.our.in_(entrylist))
            ).one()
            primary_our = (
                FiaSession.query(all_testers.c.our).filter(all_testers.c.our.in_(entrylist))
                .order_by(all_testers.c.positive.desc().nullslast(),
                    all_testers.c.negative.asc().nullslast(),
                     all_testers.c.first_et.asc().nullslast())
            ).first()[0]
            entrylist.remove(primary_our)
            elist = ''
            for e in entrylist:
                elist = elist + '; %s' % e
            outlist.append([
            primary_our,
            first_et and first_et.isoformat() or '',
            last_et and last_et.isoformat() or '',
            str(et_count),
            positive and positive.isoformat() or '',
            negative and negative.isoformat() or '',
            elist
            ])

    remaining =(
                FiaSession.query(all_testers)
                .filter(~all_testers.c.our.in_(covered))
                .filter(all_testers.c.first_et != None)
                .filter(all_testers.c.last_et != None)
                .filter((all_testers.c.positive != None) |
                        (all_testers.c.negative != None)
                        )
                 )
    for entry in iter(remaining):
        first_et, last_et, et_count, positive, negative = (
            FiaSession.query(
                func.min(all_testers.c.first_et),
                func.max(all_testers.c.last_et),
                func.sum(all_testers.c.et_count),
                func.max(all_testers.c.positive),
                func.max(all_testers.c.negative))
            .filter(all_testers.c.our.in_([entry.our]))
        ).one()
        outlist.append([
        entry.our,
        first_et and first_et.isoformat() or '',
        last_et and last_et.isoformat() or '',
        str(et_count),
        positive and positive.isoformat() or '',
        negative and negative.isoformat() or '',
        ''
        ])
        covered.append(entry.our)

    headers = ["OUR", "First ET Test Date", "Most Recent ET Test Date", "Early Tests Completed", "020 Positive Enrollment","020 Negative Enrollment",  "Alternate OUR Numbers"]
    print "\t".join(headers)
    for row in outlist:
        print "\t".join(row)
    import pdb; pdb.set_trace( )




if __name__ == '__main__':
    main()


