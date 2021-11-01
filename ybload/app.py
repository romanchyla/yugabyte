
from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
from ybload.models import ChangeLog, IdentifierMapping, MetricsBase, MetricsModel, Records
from adsputils import ADSCelery, create_engine, sessionmaker, scoped_session, contextmanager
from sqlalchemy.orm import load_only as _load_only
from sqlalchemy import Table, bindparam
import adsputils
import json
from sqlalchemy import exc


class YBLoader(ADSCelery):
    def __init__(self, app_name, *args, **kwargs):
        ADSCelery.__init__(self, app_name, *args, **kwargs)
        #self.create_database()
        

    def update_storage(self, bibcode, type, payload):
        """Update the document in the database, every time
        empty the solr/metrics processed timestamps.

        returns the sql record as a json object or an error string """

        if not isinstance(payload, basestring):
            payload = json.dumps(payload)

        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                r = Records(bibcode=bibcode)
                session.add(r)
            now = adsputils.get_date()
            oldval = None
            if type == 'metadata' or type == 'bib_data':
                oldval = r.bib_data
                r.bib_data = payload
                r.bib_data_updated = now
            elif type == 'nonbib_data':
                oldval = r.nonbib_data
                r.nonbib_data = payload
                r.nonbib_data_updated = now
            elif type == 'orcid_claims':
                oldval = r.orcid_claims
                r.orcid_claims = payload
                r.orcid_claims_updated = now
            elif type == 'fulltext':
                oldval = 'not-stored'
                r.fulltext = payload
                r.fulltext_updated = now
            elif type == 'metrics':
                oldval = 'not-stored'
                r.metrics = payload
                r.metrics_updated = now
            elif type == 'augment':
                # payload contains new value for affilation fields
                # r.augments holds a dict, save it in database
                oldval = 'not-stored'
                r.augments = payload
                r.augments_updated = now
            else:
                raise Exception('Unknown type: %s' % type)
            session.add(ChangeLog(key=bibcode, type=type, oldvalue=oldval))

            r.updated = now
            out = r.toJSON()
            try:
                session.commit()
                return out
            except exc.IntegrityError:
                self.logger.exception('error in app.update_storage while updating database for bibcode {}, type {}'.format(bibcode, type))
                session.rollback()
                raise
    
    def get_record(self, bibcode, load_only=None):
        if isinstance(bibcode, list):
            out = []
            with self.session_scope() as session:
                q = session.query(Records).filter(Records.bibcode.in_(bibcode))
                if load_only:
                    q = q.options(_load_only(*load_only))
                for r in q.all():
                    out.append(r.toJSON(load_only=load_only))
            return out
        else:
            with self.session_scope() as session:
                q = session.query(Records).filter_by(bibcode=bibcode)
                if load_only:
                    q = q.options(_load_only(*load_only))
                r = q.first()
                if r is None:
                    return None
                return r.toJSON(load_only=load_only)

    def mark_processed(self, bibcodes, type, checksums=None, status=None):
        """
        Updates the timesstamp for all documents that match the bibcodes.
        Optionally also sets the status (which says what actually happened
        with the document) and the checksum.
        Parameters bibcodes and checksums are expected to be lists of equal
        size with correspondence one to one, except if checksum is None (in
        this case, checksums are not updated).
        """

        # avoid updating whole database (when the set is empty)
        if len(bibcodes) < 1:
            return

        now = adsputils.get_date()
        updt = {'processed': now}
        if status:
            updt['status'] = status
        self.logger.debug('Marking docs as processed: now=%s, num bibcodes=%s', now, len(bibcodes))
        if type == 'solr':
            timestamp_column = 'solr_processed'
            checksum_column = 'solr_checksum'
        elif type == 'metrics':
            timestamp_column = 'metrics_processed'
            checksum_column = 'metrics_checksum'
        elif type == 'links':
            timestamp_column = 'datalinks_processed'
            checksum_column = 'datalinks_checksum'
        else:
            raise ValueError('invalid type value of %s passed, must be solr, metrics or links' % type)
        updt[timestamp_column] = now
        checksums = checksums or [None] * len(bibcodes)
        for bibcode, checksum in zip(bibcodes, checksums):
            updt[checksum_column] = checksum
            with self.session_scope() as session:
                session.query(Records).filter_by(bibcode=bibcode).update(updt, synchronize_session=False)
        session.commit()