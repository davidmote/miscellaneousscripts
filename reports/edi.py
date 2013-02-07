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
    import pdb; pdb.set_trace( )

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


if __name__ == '__main__':
    main()
