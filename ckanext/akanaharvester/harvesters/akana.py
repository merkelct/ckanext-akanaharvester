#coding: utf-8
import requests
import logging
import json
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra



log = logging.getLogger(__name__)

akanaJSON = {
            "api-id": "6930cd9c-97d1-47a2-ae4e-e3609a4516fe.enterpriseapi"

    }

akanaCont = {"title": "THis is a test",
             "url" : "https://www.google.com",
             "keyword": "akana",
             "api-id": "6930cd9c-97d1-47a2-ae4e-e3609a4516fe.enterpriseapi"}


# need to break some of this out into a base.py and import
def get_object_extra(harvest_object, key):
    '''
    Helper function for retrieving the value from a harvest object extra,
    given the key
    '''
    for extra in harvest_object.extras:
        if extra.key == key:
            return extra.value
    return None

class AkanaHarvester(SingletonPlugin):

    implements(IHarvester)

    def info(self):
        return {
            'name': 'akana',
            'title': 'Akana API Gateway',
            'description': 'Harvester for Akana API Gateway'
        }

    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''

    def get_original_url(self, harvest_object_id):
        '''

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''


    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.AKANA.gather')
        log.debug('Akana gather_stage for job: %r', harvest_job)

        # Get akana ID's contents

        ids = []
        obid=[]
        ids.append(akanaJSON['api-id'])
        # this is where we loop through the results collecting ID's and passing them
        obj = HarvestObject(guid=ids[0], job=harvest_job, extras=[HOExtra(key='status', value='new')])
        obj.save()
        obid.append(obj.id)

        print obj
        # need to return the list of ID's here that are created above
        return obid

    def fetch_stage(self, harvest_object):
        log.debug('In AkanaHarvester fetch_stage')

        # Check harvest object status
        status = get_object_extra(harvest_object, 'status')

        if status == 'new':
            try:
                harvest_object.content = akanaCont
                harvest_object.save()
                return True
            except Exception, e:
                log.exception(e)
                log.debug('Unable to get content for dataset: %s: %r' %
                          (harvest_object, e), harvest_object)

    def import_stage(self, harvest_object):
        log.debug('In AkanaHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False

        try:
            package_dict = akanaJSON
        except Exception, e:
            log.exception(e)
            self._save_object_error('%r'%e,harvest_object,'Import')

        package_dict['id'] = harvest_object.guid
        if not package_dict['name']:
            package_dict['name'] = self._gen_new_name(package_dict['title'])

        # Set the modification date
        if 'date_modified' in package_dict['extras']:
            package_dict['metadata_modified'] = package_dict['extras']['date_modified']

        # Common extras
        package_dict['extras']['harvest_catalogue_name'] = u'Akana Gateway API Portal'
        package_dict['extras']['harvest_catalogue_url'] = u'https://api-portal-np.monsanto.net'
        package_dict['extras']['harvest_dataset_url'] = harvest_object.guid

        return self._create_or_update_package(package_dict,harvest_object)

