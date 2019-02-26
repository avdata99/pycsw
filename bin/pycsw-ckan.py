#!/usr/bin/python

import ConfigParser
import getopt
import logging
import sys
import datetime
import requests
import io
import os

from jinja2 import Environment, FileSystemLoader
from lxml import etree
from pycsw import admin, config, repository, metadata, util

logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)

CONTEXT = config.StaticContext()


def usage():
    """Provide usage instructions"""
    return '''
NAME
    pycsw-ckan.py - pycsw ckan utility

SYNOPSIS
    pycsw-ckan.py -c <command> -f <cfg> [-h]

    Available options:

    -c    Command to be performed:
              - setup_db
              - load

    -f    Filepath to pycsw configuration

    -h    Usage message

    -u    URL of CSW


EXAMPLES

    1.) setup_db: Creates repository tables and indexes

        pycsw-ckan.py -c setup_db -f default.cfg

    2.) load: Loads CKAN datasets as records into the pycsw db.

        pycsw-ckan.py -c load -f default.cfg


'''

COMMAND = None
CFG = None

if len(sys.argv) == 1:
    print usage()
    sys.exit(1)

try:
    OPTS, ARGS = getopt.getopt(sys.argv[1:], 'c:f:ho:p:ru:x:s:t:y')
except getopt.GetoptError as err:
    print '\nERROR: %s' % err
    print usage()
    sys.exit(2)

for o, a in OPTS:
    if o == '-c':
        COMMAND = a
    if o == '-f':
        CFG = a
    if o == '-h':  # dump help and exit
        print usage()
        sys.exit(3)

SCP = ConfigParser.SafeConfigParser()
SCP.readfp(open(CFG))

DATABASE = SCP.get('repository', 'database')
URL = SCP.get('server', 'url')
METADATA = dict(SCP.items('metadata:main'))
try:
    TABLE = SCP.get('repository', 'table')
except ConfigParser.NoOptionError:
    TABLE = 'records'
CKAN_URL = SCP.get('defaults', 'ckan_url')


def to_date(unixtime):
    """Convert unix time to YYYY-MM-DD"""
    try:
        return datetime.datetime.fromtimestamp(unixtime).strftime('%Y-%m-%d')
    except ValueError:
        return '-1'


def keyword_list(value):
    """Ensure keywords are treated as lists"""
    if isinstance(value, list):  # list already
        return value
    else:  # csv string
        return value.split(',')


def get_record(context, repo, ckan_url, ckan_id, ckan_info):

    query = ckan_url + '/harvest/object/%s'
    url = query % ckan_info['harvest_object_id']
    print 'Fetching %s' % url
    response = requests.get(url)

    if not response.ok:
        print 'Could not get Harvest object for id %s (%d: %s)' % \
                  (ckan_id, response.status_code, response.reason)
        return

    if ckan_info['source'] in ['arcgis', 'datajson']:  # convert json to iso
        result = response.json()
        tmpldir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               '..',
                               'src/pycsw/pycsw/templates')
        env = Environment(loader=FileSystemLoader(tmpldir))

        if ckan_info['source'] == 'arcgis':
            print 'ArcGIS detected. Converting ArcGIS JSON to ISO XML: %s' % ckan_id
            env.filters['to_date'] = to_date
            tmpl = 'arcgisjson2iso.xml'
        else:
            print 'Open Data JSON detected. Converting to ISO XML: %s' % ckan_id
            env.filters['keyword_list'] = keyword_list
            tmpl = 'datajson2iso.xml'

        template = env.get_template(tmpl)
        content = template.render(json=result)

    else:  # harvested ISO XML
        content = response.content

    # from here we have an ISO document no matter what
    try:
        try:
            print 'parsing XML as is'
            xml = etree.parse(io.BytesIO(content))
        except:
            print 'parsing XML with .encode("utf8")'
            xml = etree.parse(io.BytesIO(content.encode("utf8")))
    except Exception, err:
        print 'Could not pass xml doc from %s, Error: %s' % (ckan_id, err)
        return

    try:
        print 'Parsing ISO XML'
        record = metadata.parse_record(context, xml, repo)[0]
        if not record.identifier:  # force id into ISO XML
            print 'gmd:fileIdentifier is empty. Inserting id %s' % ckan_id

            record.identifier = ckan_id

            gmd_ns = 'http://www.isotc211.org/2005/gmd'
            gco_ns = 'http://www.isotc211.org/2005/gco'

            xname = xml.find('{%s}fileIdentifier' % gmd_ns)
            if xname is None:  # doesn't exist, insert it
                print 'Inserting new gmd:fileIdentifier'
                fileid = etree.Element('{%s}fileIdentifier' % gmd_ns)
                etree.SubElement(fileid, '{%s}CharacterString' % gco_ns).text = ckan_id
                xml.insert(0, fileid)
            else:  # gmd:fileIdentifier exists, check for gco:CharacterString
                print 'Updating'
                value = xname.find('{%s}CharacterString' % gco_ns)
                if value is None:
                    print 'missing gco:CharacterString'
                    etree.SubElement(xname, '{%s}CharacterString' % gco_ns).text = ckan_id
                else:
                    print 'empty gco:CharacterString'
                    value.text = ckan_id
            record.xml = etree.tostring(xml)

    except Exception, err:
        print 'Could not extract metadata from %s, Error: %s' % (ckan_id, err)
        return

    record.ckan_id = ckan_id
    record.ckan_modified = ckan_info['metadata_modified']
    record.ckan_collection = ckan_info['ckan_collection']
    if 'collection_package_id' in ckan_info:
        record.parentidentifier = ckan_info['collection_package_id']

    return record


if COMMAND is None:
    print '-c <command> is a required argument'
    sys.exit(4)

if COMMAND not in ['setup_db', 'load', 'set_keywords']:
    print 'ERROR: invalid command name: %s' % COMMAND
    sys.exit(5)

if COMMAND == 'setup_db':
    from sqlalchemy import Table, Boolean, Column, Text, MetaData, \
            create_engine

    ckan_columns = [
        Column('ckan_id', Text, index=True),
        Column('ckan_modified', Text),
        Column('ckan_collection', Boolean),
    ]
    try:
        admin.setup_db(DATABASE, TABLE,
                home='',
                create_plpythonu_functions=False,
                extra_columns=ckan_columns)
    except Exception as err:
        print err
        print 'ERROR: DB creation error.  Database tables already exist'
        print 'Delete tables or database to reinitialize'

    # create system_info table
    engine = create_engine(DATABASE)
    schema, table = util.sniff_table('system_info')
    mdata = MetaData(engine, schema=schema)

    log.info('Creating system info table')
    system_info = Table(
        'system_info', mdata,
        Column('key', Text, index=True),
        Column('value', Text, index=True),
    )

    try:
        system_info.create()
        engine.execute("INSERT INTO system_info VALUES('keywords', '');")
    except Exception as err:
        print err
        print 'ERROR: DB creation error.  Database tables already exist'

elif COMMAND == 'load':
    repo = repository.Repository(DATABASE, CONTEXT, table=TABLE)
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    ckan_api = requests.Session()
    ckan_api.mount('https://', adapter)
    log.info('Started gathering CKAN datasets identifiers.')

    def __reconcile(gathered_records, existing_records):
        """
        Compares the gathered_records against existing ones.
        """

        new = set(gathered_records) - set(existing_records)
        deleted = set(existing_records) - set(gathered_records)
        changed = set()

        for key in set(gathered_records) & set(existing_records):
            if gathered_records[key]['metadata_modified'] > existing_records[key]:
                changed.add(key)

        for ckan_id in deleted:
            try:
                repo.session.begin()
                repo.session.query(repo.dataset.ckan_id).filter_by(
                ckan_id=ckan_id).delete()
                log.debug('Deleted %s', ckan_id)
                repo.session.commit()
            except Exception:
                repo.session.rollback()
                raise

        for ckan_id in new:
            ckan_info = gathered_records[ckan_id]
            record = get_record(CONTEXT, repo, CKAN_URL, ckan_id, ckan_info)
            if not record:
                # Is this a potential error?
                log.warning('Skipped record %s', ckan_id)
                continue
            try:
                repo.insert(record, 'local', util.get_today_and_now())
                log.debug('Inserted %s', ckan_id)
            except Exception:
                log.exception('Failed to insert %s', ckan_id)

        for ckan_id in changed:
            ckan_info = gathered_records[ckan_id]
            record = get_record(CONTEXT, repo, CKAN_URL, ckan_id, ckan_info)
            if not record:
                # Error?
                log.warning('Skipped record %s', ckan_id)
                continue
            update_dict = dict([(getattr(repo.dataset, key),
            getattr(record, key)) \
            for key in record.__dict__.keys() if key != '_sa_instance_state'])
            try:
                repo.session.begin()
                repo.session.query(repo.dataset).filter_by(
                ckan_id=ckan_id).update(update_dict)
                repo.session.commit()
                log.info('Changed %s', ckan_id)
            except Exception, err:
                repo.session.rollback()
                raise RuntimeError, 'ERROR: %s' % str(err)


    start = 0
    while True:
        log.info('Fetching records from start=%s', start)
        gathered_records = {}
        response = ckan_api.get('%s/api/search/dataset' % CKAN_URL, params={
            'qjson': {
                'fl': ','.join([
                    'id',
                    'metadata_modified',
                    'extras_harvest_object_id',
                    'extras_source_datajson_identifier',
                    'extras_metadata_source',
                    'extras_collection_package_id',
                ]),
                'q': 'harvest_object_id:["" TO *]',
                'limit': 1000,
                'start': start,
            }
        })

        listing = response.json()
        if not isinstance(listing, dict):
            raise RuntimeError, 'Wrong API response: %s' % listing
        results = listing.get('results')
        if not results:
            break
        for result in results:
            gathered_records[result['id']] = {
                'metadata_modified': result['metadata_modified'],
                'harvest_object_id': result['extras']['harvest_object_id'],
                'source': result['extras'].get('metadata_source')
            }
            # This seems backwards to me. If collection_package_id exists,
            # doesn't that make it a collection?
            is_collection = 'collection_package_id' not in result['extras']
            if is_collection:
                gathered_records[result['id']]['collection_package_id'] = \
                    result['extras']['collection_package_id']

            gathered_records[result['id']]['ckan_collection'] = is_collection
            if 'source_datajson_identifier' in result['extras']:
                gathered_records[result['id']]['source'] = 'datajson'

        start = start + 1000
        log.info('Gathered %s', start)

        # Fetch corresponding records, if any
        existing_records = {}
        query = repo.session
            .query(repo.dataset.ckan_id, repo.dataset.ckan_modified)
            .filter(repo.dataset.ckan_id.in_(gathered_records.keys()))
        for row in query:
            existing_records[row.ckan_id] = row.ckan_modified
        log.debug('Found count=%s existing datasets', len(existing_records.keys()))
        # Reconcile what's been fetched
        __reconcile(gathered_records, existing_records)


elif COMMAND == 'set_keywords':
    """set pycsw service metadata keywords from top limit CKAN tags"""
    limit = 20

    print 'Fetching tags from %s' % CKAN_URL
    url = CKAN_URL + '/api/action/package_search?facet.field=["tags"]&rows=0&facet.limit=%s' % limit
    response = requests.get(url)
    tags = response.json()['result']['facets']['tags']

    print 'Deriving top %d tags' % limit
    # uniquify and sort by top limit
    tags_sorted = sorted(tags, key=tags.__getitem__, reverse=True)
    tags_trimmed = [x for x in tags_sorted if x != '']
    keywords = ','.join(tags_trimmed)

    from sqlalchemy import create_engine
    engine = create_engine(DATABASE)
    engine.execute("UPDATE system_info SET value='%s' WHERE key='keywords'" % keywords)
    print 'Tags saved to system_info table.'

print 'Done'
