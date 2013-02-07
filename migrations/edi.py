#!/Users/davidmote/Environment/mojojojo/bin/zopepy
from avrc.aeh import model
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from datetime import date, datetime
import sys
import zope.interface
import zope.schema
import occams.datastore

#from occams.datastore import interfaces as dsinterfaces
# Script for migrating symptom data from the old OCCAMS.symptom product

def main():
    usage = """symptom_forms.py FIACONNECT"""
    Session = configureSession(sys.argv[1])

    patient = Session.query(model.Patient).filter_by(our='222-22j').one()
    patient_entities = (
        Session.query(model.Entity)
        .filter(model.Entity.state.in_('pending-review', 'complete'))
        .join(model.Entity.schema)
        .join(model.Context, model.Entity.id == model.Context.entity_id)
        .filter(model.Context.external=='patient')
        .filter(model.Context.key == patient.id)
    )
    viral_load = patient_entities.filter(model.Schema.name == 'ViralLoad')
    hiv_serology = patient_entities.filter(model.Schema.name == 'HivSerology')
    rapid_test = patient_entities.filter(model.Schema.name == 'RapidTest')
    western_blot = patient_entities.filter(model.Schema.name == 'WesternBlot')
    detuned_eia = patient_entities.filter(model.Schema.name == 'DetunedEIA')
    screening = patient_entities.filter(model.Schema.name == 'ScreeningRouteOfTransmission')
    screening027 = patient_entities.filter(model.Schema.name == 'ScreeningRouteOfTransmission027')

    viral_load = viral_load.order_by(model.Entity.collect_date.asc())
    import pdb; pdb.set_trace( )

def configureSession(connect):
    """Set up Session for data manipulation and create new tables as side
       effect."""
    new_engine = create_engine(connect)
    Session = scoped_session(
        sessionmaker(
            class_=occams.datastore.model.DataStoreSession,
            user=(lambda : "bitcore@ucsd.edu"), # can be called by a library
            bind=new_engine
            )
        )
    return Session

# class IEDI(zope.interface.Interface):
#     """
#     """
#     hiv_status = zope.schema.Bool(
#         title = u"HIV Status",
#         description = u"",
#         required=False,
#         read_only=True,
#         )

#     klass = zope.schema.Choice(
#         title = u"EDI Class",
#         description=u"",
#         values=sorted([ 'A-1.0',
#                         'A-2.0',
#                         'A-3.0',
#                         'A-3.1',
#                         'E-1.0A',
#                         'E-1.0B',
#                         'E-1.0C',
#                         'E-2.0A',
#                         'E-2.0B',
#                         'E-3.0',
#                         'E-0.0',
#                         ]),
#         required = False,
#         read_only = True
#         )

#     edi_date = zope.schema.Date(
#         title=u'Estimated Date of Infection',
#         description=u"",
#         required=False,
#         )

#     day_zero = zope.schema.Date(
#         title=u'Day Zero',
#         description=u"",
#         required=False,
#         )

#     last_negative_eia = zope.schema.Object(
#         title='Last Negative EIA',
#         description=("Defined as the last negative result BEFORE a "
#                      "positive western blot result (if any)"),
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )

#     last_negative_hiv_serology = zope.schema.Object(
#         title='Last Negative HIV Serology',
#         description=("Defined as the last negative result BEFORE a "
#                      "positive western blot result (if any)"),
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )

#     last_negative_rapid = zope.schema.Object(
#         title='Last Negative Rapid Test',
#         description=("Defined as the last negative result BEFORE a "
#                      "positive western blot result (if any)"),
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )

#     first_positive_rna = zope.Schema.Object(
#         title='First Positive RNA',
#         description="",
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )

#     first_positive_western_blot = zope.schema.Object(
#         title='First Positive Western Blot',
#         description="",
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )

#     first_indeterminate_western_blot = zope.schema.Object(
#         title='First Indeterminate Western Blot',
#         description="",
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )

#     last_indeterminate_western_blot = zope.schema.Object(
#         title='Last Indeterminate Western Blot',
#         description="The latest Indeterminate western blot BEFORE a positive WB",
#         schema = occams.datastore.interfaces.IEntity,
#         required=False
#         )



if __name__ == '__main__':
    main()
