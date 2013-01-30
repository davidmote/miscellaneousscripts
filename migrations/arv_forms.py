#!/Users/davidmote/Environment/mojojojo/bin/zopepy
from avrc.aeh import model
from occams.drug import model as drugmodel
from occams.datastore import model as datastore
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from datetime import date, datetime
import sys

# Script for migrating ARV data from both the old OCCAMS.drug product and
# pre-existing BBL data to a new form, created by this script, into the
# production database.

def main():
    usage = """arv_forms.py FIACONNECT BBLCONNECT"""
    Session = configureSession(sys.argv[1])
    BBLSession = configureSession(sys.argv[2])
    try:
        medSchema = (
            Session.query(model.Schema)
            .filter_by(name="ARVMeds")
            .filter_by(publish_date=date(1996, 1,1))
            .one()
        )
    except sa.orm.exc.NoResultFound:
        medSchema = buildMergedSchema(Session, BBLSession)

    fillModern(Session, medSchema)
    fillLegacy(Session, BBLSession, medSchema)
    Session.commit()

def configureSession(connect):
    """Set up Session for data manipulation and create new tables as side
       effect."""
    new_engine = create_engine(connect)
    Session = scoped_session(
        sessionmaker(
            class_=datastore.DataStoreSession,
            user=(lambda : "bitcore@ucsd.edu"), # can be called by a library
            bind=new_engine
            )
        )
    return Session

def buildMergedSchema(Session, BBLSession):

    newSchema = model.Schema(
            name = "ARVMeds",
            title = "ARV Meds",
            state="published",
            publish_date=date(1996, 1,1)
            )
    order = 1
    drug = model.Attribute(
        schema=newSchema,
        name='drug',
        title='Drug',
        description="",
        type='string',
        order=order,
        is_collection=False,
        is_required=False,
        )
    drugs = mergeDrugs(Session, BBLSession)
    for i, (drug_key, drug_value) in enumerate(sorted(drugs.items(), key=lambda t: t[0])):
        newChoice = model.Choice(
            name = drug_key,
            title = unicode(drug_value),
            order = i,
            value = unicode(drug_key)
            )
        drug.choices.append(newChoice)
    order += 1
    start_date = model.Attribute(
        schema=newSchema,
        name='start_date',
        title='Start Date',
        description="Date the medication began",
        type='date',
        order=order,
        is_collection=False,
        is_required=False,
        )
    order += 1

    stop_date = model.Attribute(
        schema=newSchema,
        name='stop_date',
        title='Start Date',
        description="Date the medication stopped",
        type='date',
        order=order,
        is_collection=False,
        is_required=False,
        )
    order += 1

    start_reason = model.Attribute(
        schema=newSchema,
        name='start_reason',
        title='Prescription Reason',
        description="",
        type='string',
        order=order,
        is_collection=False,
        is_required=False,
        )

    for i, (name, title, value) in enumerate([('pep', 'Post Exposure Prophylaxis', 'pep'),('prep', 'Pre Exposure Prophylaxis', 'prep')]):
        newChoice = model.Choice(
            name = name,
            title = unicode(title),
            order = i,
            value = unicode(value)
            )
        start_reason.choices.append(newChoice)
    order += 1

    adverse_event = model.Attribute(
        schema=newSchema,
        name='adverse_event',
        title='Adverse Reaction',
        description="",
        type='string',
        order=order,
        is_collection=False,
        is_required=False,
        )

    events = gatherBBlStopReasons(BBLSession)
    for i, (event) in enumerate(events):
        newChoice = model.Choice(
            name = event,
            title = unicode(event),
            order = i,
            value = unicode(event)
            )
        adverse_event.choices.append(newChoice)

    order += 1

    adverse_event_grade = model.Attribute(
        schema=newSchema,
        name='adverse_event_grade',
        title='Adverse Reaction Grade',
        description="",
        type='integer',
        order=order,
        is_collection=False,
        is_required=False,
        )
    for i, (grade, title) in enumerate([(1, '1 - Mild'),(2,'2 - Moderate'), (3,'3 - Severe'), (4,  '4 - Life Threatening')]):
        newChoice = model.Choice(
            name = str(grade),
            title = unicode(title),
            order = i,
            value = grade
            )
        adverse_event_grade.choices.append(newChoice)
    order += 1

    notes = model.Attribute(
        schema=newSchema,
        name='notes',
        title='Notes',
        description="",
        type='text',
        order=order,
        is_collection=False,
        is_required=False,
        )

    Session.add(newSchema)
    Session.flush()
    return newSchema

def fillModern(Session, medSchema):
    for use in Session.query(drugmodel.DrugUsage):
        kwargs = {}
        patient = use.patient

        kwargs['drug'] =  use.type.code
        kwargs['start_date'] = use.start_date
        kwargs['stop_date'] = use.stop_date

        kwargs['notes'] = use.notes
        if kwargs['notes'] is not None:
            if kwargs['notes'].lower().find("prep") >= 0:
                kwargs['start_reason'] = "prep"
            elif kwargs['notes'].lower().find("pep") >=0:
                kwargs['start_reason'] = "pep"
            elif kwargs['notes'].lower().find("post exposure prophylaxis") >= 0:
                kwargs['start_reason'] = "pep"

        collect_date = use.create_date.date()
        visits = (
            Session.query(model.Visit)
            .filter(model.Visit.patient_id == patient.id)
            .filter(model.Visit.visit_date >= kwargs['start_date'])
            )

        if kwargs['stop_date'] is not None:
            visits = visits.filter(model.Visit.visit_date <= kwargs['stop_date'])

        if visits.count():
            for i,visit in enumerate(visits):
                collect_date = visit.visit_date
                formstate =  u'complete'
                formTitle = "-".join([medSchema.name, datetime.now().isoformat(), str(i)])
                newEntity = model.Entity(schema=medSchema, name=formTitle, title=medSchema.name, state=formstate, collect_date=collect_date)
                for key, value in kwargs.iteritems():
                    newEntity[key] = value
                Session.add(newEntity)
                Session.flush()
                visit.entities.add(newEntity)
                patient.entities.add(newEntity)
        else:
            formstate =  u'complete'
            formTitle = "-".join([medSchema.name, datetime.now().isoformat(), str(i)])
            newEntity = model.Entity(schema=medSchema, name=formTitle, title=medSchema.name, state=formstate, collect_date=collect_date)
            for key, value in kwargs.iteritems():
                newEntity[key] = value
            Session.add(newEntity)
            Session.flush()
            patient.entities.add(newEntity)


def fillLegacy(Session, BBLSession, medSchema):
    patientdict = {}
    for kwargs in gatherBBLData(BBLSession):
        aeh_number = kwargs.pop('aeh_number')
        if aeh_number in ['05-01-0575-2', '05-01-1010-9']:
            continue
        collect_date = kwargs.pop('visit_date')
        if patientdict.has_key(aeh_number):
            patient = patientdict[aeh_number]
        else:
            patientQuery = (
                Session.query(model.Patient).filter_by(legacy_number=aeh_number)
                )
            try:
                patient = patientQuery.one()
            except sa.orm.exc.NoResultFound:
                aeh_no_checkdigit = "-".join(aeh_number.split('-')[0:3])
                patTry2 = (
                Session.query(model.Patient).filter_by(legacy_number=aeh_no_checkdigit)
                )
                try:
                    patient = patTry2.one()
                except sa.orm.exc.NoResultFound:
                    print "no patient?"
                else:
                    patientdict[aeh_number] = patient
            else:
                patientdict[aeh_number] = patient

        if patientdict.has_key(aeh_number):
            visitQ = (
            Session.query(model.Visit)
            .filter(model.Visit.patient == patientdict[aeh_number])
            .filter_by(visit_date = collect_date)
            )
            if visitQ.count():
                for i,visit in enumerate(visitQ):
                    collect_date = collect_date
                    formstate =  u'complete'
                    formTitle = "-".join([medSchema.name, datetime.now().isoformat(), str(i)])
                    newEntity = model.Entity(schema=medSchema, name=formTitle, title=medSchema.name, state=formstate, collect_date=collect_date)
                    for key, value in kwargs.iteritems():
                        newEntity[key] = value
                    Session.add(newEntity)
                    Session.flush()
                    visit.entities.add(newEntity)
                    patient.entities.add(newEntity)
            else:
                formstate =  u'complete'
                formTitle = "-".join([medSchema.name, datetime.now().isoformat()])
                newEntity = model.Entity(schema=medSchema, name=formTitle, title=medSchema.name, state=formstate, collect_date=collect_date)
                for key, value in kwargs.iteritems():
                    newEntity[key] = value
                Session.add(newEntity)
                Session.flush()
                patient.entities.add(newEntity)

def gatherBBLData(Session):
    results = Session.execute("""
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
    then 1
    when e.eharvtg = 2
    then 2
    WHEN e.eharvtg = 3
    then 3
    when e.eharvtg = 4
    then 4
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
    then 1
    when e.eharvtg = 2
    then 2
    WHEN e.eharvtg = 3
    then 3
    when e.eharvtg = 4
    then 4
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
    """)
    keys = [
    'aeh_number',
    'visit_date',
    'drug',
    'start_date',
    'stop_date',
    'adverse_event',
    'adverse_event_grade']
    for entry in results:
        yield dict(zip(keys, entry))

def gatherBBLDrugList(Session):
    results = Session.execute("""
select * from
(
select distinct med.ehaiedm as code, med.ehmed1t as name
from aeh_ehmedl1 med
join aeh_eharv01 e
    on e.ehmed1i = med.ehaiedm
union all
select distinct med.ehaiedm as code, med.ehmed1t as name
from aeh_ehmedl1 med
join aehpart_eharv01 e
    on e.ehmed1i = med.ehaiedm
) as meds
group by code, name
order by code
    """)
    return dict([r for r in results])

def gatherBBlStopReasons(Session):
    results = Session.execute("""
select eharv1t from aeh_eharvl1
where eharv1i > 11
order by eharv1i asc
    """)
    for result in results:
        yield result[0]

def mergeDrugs(Session, BBLSession):
    bbldrugs = gatherBBLDrugList(BBLSession)
    drug_query = (
        Session.query(drugmodel.DrugType)
        .order_by(drugmodel.DrugType.code.asc())
        )
    drugdict = {'06150412':'06150412 - Unknown Drug',
                '08180422': '08180422 - Altripla / EFV / TDF / FTC',
                '08180809': '08180809 - ETR / ETV / Etravirine / Intelegence',
                '08180814': '08180814 - RAL / RGV / Raltegravir',
                '08181220': '08181220 - DRV / Darunavir / Prezista',
                '08182006': '08182006 - Unknown Drug'
                }
    for drug in drug_query:
        all_names = ' / '.join([n.value for n in drug.names])
        title = "%s - %s" % (drug.code, all_names)
        drugdict[drug.code] = title
    for key, value in bbldrugs.items():
        if key not in drugdict.keys():
            drugdict[key] = value
    return drugdict

if __name__ == '__main__':
    main()
