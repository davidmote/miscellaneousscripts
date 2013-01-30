#!/Users/davidmote/Environment/mojojojo/bin/zopepy
from avrc.aeh import model
from occams.symptom import model as symptommodel
from occams.datastore import model as datastore
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from datetime import date, datetime
import sys

# Script for migrating symptom data from the old OCCAMS.symptom product

def main():
    usage = """symptom_forms.py FIACONNECT"""
    Session = configureSession(sys.argv[1])
    try:
        symptomSchema = (
            Session.query(model.Schema)
            .filter_by(name="Symptoms")
            .filter_by(publish_date=date(2010, 9,1))
            .one()
        )
    except sa.orm.exc.NoResultFound:
        symptomSchema = buildSchema(Session)
    fillModern(Session, symptomSchema)
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

def buildSchema(Session):

    newSchema = model.Schema(
            name = "Symptoms",
            title = "Symptoms",
            state="published",
            publish_date=date(2010, 9,1)
            )

    order = 1
    symptom = model.Attribute(
        schema=newSchema,
        name='symptom',
        title='Symptom',
        description="",
        type='string',
        order=order,
        is_collection=False,
        is_required=False,
        )
    symptoms = gatherSymptoms(Session)
    for i, (symptom_key, symptom_value) in enumerate(sorted(symptoms.items(), key=lambda t: t[0])):
        newChoice = model.Choice(
            name = symptom_key,
            title = unicode(symptom_value),
            order = i,
            value = unicode(symptom_key)
            )
        symptom.choices.append(newChoice)

    order += 1

    type_other = model.Attribute(
        schema=newSchema,
        name='type_other',
        title='If Other, specify',
        description="",
        type='string',
        order=order,
        is_collection=False,
        is_required=False,
        )

    order += 1
    start_date = model.Attribute(
        schema=newSchema,
        name='start_date',
        title='Start Date',
        description="Date the symptom began",
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
        description="Date the symptom stopped",
        type='date',
        order=order,
        is_collection=False,
        is_required=False,
        )
    order += 1

    is_attended = model.Attribute(
        schema=newSchema,
        name='is_attended',
        title='Did the subject seek medical attention?',
        description="",
        type='boolean',
        order=order,
        is_collection=False,
        is_required=False,
        )
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

def fillModern(Session, symptomSchema):
    for symptom in Session.query(symptommodel.Symptom):
        kwargs = {}
        patient = symptom.patient
        formstate =  u'complete'
        collect_date = symptom.create_date.date()

        kwargs['symptom'] =  symptom.type.value
        kwargs['start_date'] = symptom.start_date
        kwargs['stop_date'] = symptom.stop_date
        kwargs['type_other'] = symptom.type_other
        kwargs['is_attended'] = symptom.is_attended

        visits = (
            Session.query(model.Visit)
            .filter(model.Visit.patient_id == patient.id)
            .filter(model.Visit.visit_date >= kwargs['start_date'])
            )

        if kwargs['stop_date'] is not None:
            visits = visits.filter(model.Visit.visit_date <= kwargs['stop_date'])
        if not visits.count():
            visits = (Session.query(model.Visit)
            .filter(model.Visit.patient_id == patient.id)
            .filter(model.Visit.visit_date == collect_date)
            )
        if visits.count():
            for i,visit in enumerate(visits):
                collect_date = visit.visit_date
                formTitle = "-".join([symptomSchema.name, datetime.now().isoformat(), str(i)])
                newEntity = model.Entity(schema=symptomSchema, name=formTitle, title=symptomSchema.name, state=formstate, collect_date=collect_date)
                for key, value in kwargs.iteritems():
                    newEntity[key] = value
                Session.add(newEntity)
                Session.flush()
                visit.entities.add(newEntity)
                patient.entities.add(newEntity)
        else:
            formstate =  u'complete'
            formTitle = "-".join([symptomSchema.name, datetime.now().isoformat(), str(i)])
            newEntity = model.Entity(schema=symptomSchema, name=formTitle, title=symptomSchema.name, state=formstate, collect_date=collect_date)
            for key, value in kwargs.iteritems():
                newEntity[key] = value
            Session.add(newEntity)
            Session.flush()
            patient.entities.add(newEntity)


def gatherSymptoms(Session):
    symptomdict = {}
    symptom_query = (
        Session.query(symptommodel.SymptomType)
        .order_by(symptommodel.SymptomType.id.asc())
        )
    for symptom in symptom_query:
        symptomdict[symptom.value] = symptom.value
    return symptomdict

if __name__ == '__main__':
    main()
