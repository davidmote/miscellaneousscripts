#!/Users/davidmote/Environment/mojojojo/bin/zopepy

from avrc.aeh import FiaSession
from avrc.aeh import PhiSession
from avrc.aeh import model
from occams.datastore import model as datastore
from sqlalchemy import func


def main():
# PhiSession.query(model.Entity).join(model.Entity.schema).filter(model.Schema.name == 'IBio')

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
group by bday.birthday, l.name, fi.name --, m.name
order by count desc
)
select  our_numbers from results where count > 1""")
    multi_testers = FiaSession.execute(
"""
    select p.our,
    count(s.name) as study_count,
    (select count(st.name)
        from enrollment en
        join study st on st.id = en.study_id
        join patient pa on pa.id = en.patient_id
        where st.name in ('early-test', 'lead-the-way')
        and p.our = pa.our
    ) as early_test_count,
    (select en.consent_date
        from enrollment en
        join study st on st.id = en.study_id
        join patient pa on pa.id = en.patient_id
        where st.name in ('early-test', 'lead-the-way')
        and p.our = pa.our
        order by en.consent_date
        limit 1
    ) as first_early_test,
    (select en.consent_date
        from enrollment en
        join study st on st.id = en.study_id
        join patient pa on pa.id = en.patient_id
        where st.name in ('early-test', 'lead-the-way')
        and p.our = pa.our
        order by en.consent_date desc
        limit 1
    ) as last_early_test,
    (select en.consent_date
        from enrollment en
        join study st on st.id = en.study_id
        join patient pa on pa.id = en.patient_id
        where st.name in ('020-positive')
        and p.our = pa.our
        order by en.consent_date desc
        limit 1
    ) as "020-positive",
    (select en.consent_date
        from enrollment en
        join study st on st.id = en.study_id
        join patient pa on pa.id = en.patient_id
        where st.name in ('020-negative')
        and p.our = pa.our
        order by en.consent_date
        limit 1
    ) as "020-negative",
    array_agg(s.name) as studies --, e.consent_date
    from patient p
    join enrollment e on p.id = e.patient_id
    join study s on s.id = e.study_id
    where s.name in ('early-test', 'lead-the-way','020-positive', '020-negative')
    group by p.our""")

    patient_et = (
        FiaSession.query(model.Patient.our.label('our'),
                         func.min(model.Enrollment.consent_date).label('first_et'),
                         func.max(model.Enrollment.consent_date).label('last_et'))
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


    multi_testers = (
                    FiaSession.query(patient_et.c.our.label('our'),
                                     patient_et.c.first_et.label('first_et'),
                                      patient_et.c.last_et.label('last_et'),
                                      patient_020.c.positive_020.label('positive'))
                    .join(patient_020, patient_020.c.our == patient_et.c.our)
                    ).subquery('multi_testers')


    outlist = []
    covered = []
    et_patients = []
    for entrylist in dupes:
        entrylist = entrylist[0]
        if len(entrylist) > 1:
            primary_our = FiaSession.query(multi_testers).filter(multi_testers.c.our.in_(entrylist))
            if primary_our.count():
                primary_our = primary_our.one()
                patient_et_consents = (
                                FiaSession.query(
                                    model.Patient.our,
                                    model.Enrollment.consent_date.label('consent_date'))
                    .join(model.Enrollment.patient)
                    .join(model.Enrollment.study)
                    .filter(model.Study.name.in_(['early-test', 'lead-the-way', 'cvct-testing-together']))
                    .filter(model.Patient.our.in_(entrylist))
                    ).subquery('patient_et_consents')
                min_max = FiaSession.query(func.min(patient_et_consents.c.consent_date).label('min_date'), func.max(patient_et_consents.c.consent_date).label('max_date')).one()
                entrylist.remove(primary_our.our)
                et_patients.extend(entrylist)
                elist = ''
                for e in entrylist:
                    elist = elist + '; %s' % e
                outlist.append([primary_our.our,
                                        min_max.min_date < primary_our.first_et and min_max.min_date.isoformat() or primary_our.first_et.isoformat(),
                                        min_max.max_date >  primary_our.first_et and min_max.min_date.isoformat() or primary_our.first_et.isoformat(),
                                        primary_our.positive.isoformat(),
                                        elist])
                covered.extend(entrylist)
            else:
                # no match :/

                patient_et_consents = (
                            FiaSession.query(
                                model.Patient.our.label('our'),
                                model.Enrollment.consent_date.label('consent_date'))
                .join(model.Enrollment.patient)
                .join(model.Enrollment.study)
                .filter(model.Study.name.in_(['early-test', 'lead-the-way', 'cvct-testing-together']))
                .filter(model.Patient.our.in_(entrylist))
                ).subquery('patient_et_consents')

                patient_020_consents = (
                            FiaSession.query(
                                model.Patient.our.label('our'),
                                model.Enrollment.consent_date.label('consent_date'))
                .join(model.Enrollment.patient)
                .join(model.Enrollment.study)
                .filter(model.Study.name.in_(['020-positive']))
                .filter(model.Patient.our.in_(entrylist))
                ).subquery('patient_020_consents')
                if FiaSession.query(patient_et_consents).count() and FiaSession.query(patient_020_consents).count():
                    min_max = FiaSession.query(func.min(patient_et_consents.c.consent_date).label('min_date'), func.max(patient_et_consents.c.consent_date).label('max_date')).one()
                    max_020 = FiaSession.query(func.max(patient_020_consents.c.consent_date).label('positive_date')).one()
                    primary_our = FiaSession.query(patient_020_consents.c.our.label('our')).first()
                    et_patients.extend(entrylist)

                    entrylist.remove(primary_our.our)
                    elist = ''
                    for e in entrylist:
                        elist = elist + '; %s' % e
                    outlist.append([primary_our.our,
                                   min_max.min_date.isoformat(),
                                   min_max.max_date.isoformat(),
                                   max_020.positive_date.isoformat(),
                                   elist])
                    covered.extend(entrylist)
    remaining =(
                FiaSession.query(
                     multi_testers)
                     .filter(~multi_testers.c.our.in_(covered))
                     )
    for entry in iter(remaining):
        et_patients.append(entry.our)
        outlist.append([
                       entry.our,
                       entry.first_et.isoformat(),
                       entry.last_et.isoformat(),
                       entry.positive.isoformat(),
                       ''
                       ] )
    print outlist
    et_numbers = (
                  FiaSession.query(model.Patient.our.label('our'), model.Study.name, model.Enrollment.consent_date, model.Enrollment.reference_number)
                  .select_from(model.Patient)
                  .join(model.Patient.enrollments)
                  .join(model.Enrollment.study)
                  .filter(model.Patient.our.in_(et_patients))
                 .filter(model.Study.name.in_(['early-test', 'lead-the-way', 'cvct-testing-together']))
                  )

    # for entry in multi_testers:
    #     patientlist.append(dict(entry))


    # positive_with_negative = 0
    # positive_dupes = 0
    # all_negative = 0
    # partner_positive = 0
    # partner_negative = 0
    # part_mergable=[]
    # pos_mergable = []
    # neg_mergable = []

            # # query = (
            # #     FiaSession.query(model.Patient)
            # #     .join(model.Patient.enrollments)
            # #     .filter(model.Patient.our.in_(entrylist))
            # # )
            # positive = query.filter(model.Enrollment.study.has(model.Study.code == '020')).filter(model.Enrollment.study.has(model.Study.short_title != '020N'))
            # negative = query.filter(model.Enrollment.study.has(model.Study.short_title == '020N'))
            # partner = query.filter(model.Enrollment.study.has(model.Study.short_title == '027'))
            # earlytest = query.filter(model.Enrollment.study.has(model.Study.short_title == 'ET'))

            # if positive.count() and (earlytest.count() or negative.count()):
            #     positive_with_negative +=1
            # elif partner.count() and positive.count():
            #     partner_positive +=1
            # elif partner.count():
            #     partner_negative +=1
            # elif negative.count():
            #     all_negative +=1
            # elif positive.count():
            #     positive_dupes +=1
            # if positive.count():
            #     pos_mergable.append(entrylist)
            # elif partner.count():
            #     part_mergable.append(entrylist)
            # elif negative.count():
            #     neg_mergable.append(entrylist)

            # positive = query.filter(model.Patient.enrollments.any(model.Enrollment.study_id == 3)).filter(~model.Patient.enrollments.any(model.Enrollment.study_id.in_([1, 8])))
            # et = query.filter(~model.Patient.enrollments.any(model.Enrollment.study_id == 3)).filter(model.Patient.enrollments.any(model.Enrollment.study_id.in_([1, 8])))
            # if positive.count() and et.count():
            #     pos_mergable.append(entrylist)

    # print "positive with negative %d" %positive_with_negative
    # print "positive_dupes %d" %positive_dupes
    # print "partner_positive %d" %partner_positive
    # print "partner_negative %d" %partner_negative
    # print "all_negative %d" %all_negative
    for entry in iter(et_numbers): print (entry.our, entry.name, entry.consent_date.isoformat(), entry.reference_number)
    import pdb; pdb.set_trace( )
    print "foo"

    # PhiSession.query(model.Patient.our)
    # .join(datastore.Context,
    #     (datastore.Context.key == model.Patient.id)
    #     & (datastore.Context.external == 'patient')
    #     )
    # .join(datastore.ValueObject,
    #     datastore.ValueObject.entity_id == datastore.Context.entity_id
    #     )
    # .join(datastore.Entity, datastore.Entity.id == datastore.ValueObject._value
    #     )





if __name__ == '__main__':
    main()
