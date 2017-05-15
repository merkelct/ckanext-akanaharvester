#coding: utf-8
import requests
import logging
import json
from pingi.ping_client import PingAuth
from pylons import config
from ckan import plugins as p
from ckan import model
from ckan.logic import validators as v
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import (HarvestObject, HarvestGatherError, HarvestObjectError, HarvestJob)
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from urlparse import urlparse
from os import environ


environ['PING_CLIENT_ID'] = config.get('ckan.harvester.id', '')
environ['PING_CLIENT_SECRET'] = config.get('ckan.harvester.secret', '')
pingi_env = config.get('ckan.harvester.pingi.env', '')
pingi_url = config.get('ckan.harvester.pingi.url', '')
akana_portal_url = config.get('ckan.harvester.akana.portal.url', '')

# pingi.monsanto
ping_environments = {pingi_env}
ping_urls = {
    pingi_env: pingi_url
}


log = logging.getLogger(__name__)

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
    _user_name = None

    implements(IHarvester)

    _save_gather_error = HarvestGatherError.create
    _save_object_error = HarvestObjectError.create

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
        log.info('Akana gather_stage for job: %r', harvest_job)

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name()
        }
        # get the current objevcts ids and add them to a set
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
            filter(HarvestObject.current == True). \
            filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = guid_to_package_id.keys()

        # Get akana ID's contents
        # make request to get object from akana based on tag search
        url = harvest_job.source.url
        pa = PingAuth()

        resp = pa.get(url)
        resp_dict = json.loads(resp.content)

        if resp.status_code == 200:
            try:
                ids = []
                obid= []
                x = 0
                for api in resp_dict:
                    uuid = api['api-id'] + api['swagger']['info']['version'] + harvest_job.source_id
                    ids.append(uuid)
                    json_api = json.dumps(api)

                    if uuid in guids_in_db:
                        log.info("This package is already in ckan and is going to be updated: %r", uuid)
                        status = "update"
                    else:
                        log.info("This package is being created: %r", uuid)
                        status = "new"

                    obj = HarvestObject(guid=ids[x], job=harvest_job, extras=[HOExtra(key='status', value=status)], content=json_api)
                    obj.save()
                    obid.append(obj.id)
                    x += 1

                obj_del = list(set(guids_in_db) - set(ids))

                if obj_del:
                    for uuid in obj_del:
                        log.info("This package is being deleted: %r", uuid)
                        obj = HarvestObject(guid=uuid, job=harvest_job, extras=[HOExtra(key='status', value="delete")], content=[])
                        model.Session.query(HarvestObject). \
                            filter_by(guid=guid). \
                            update({'current': False}, False)
                        obj.save()
                        obid.append(obj.id)



                # need to return the list of ID's here that are created above
                return obid
            except Exception, e:
                log.error('Exception: %s' % e)
                self._save_gather_error('Error gathering the identifiers from the AKANA server [%s]' % str(e), harvest_job)
                return None
        else:
            log.error('Akana api returned non-200 status.  Returned status: {code} . Message: {message}'.format(code=resp.status_code, message=resp_dict['messages']))
            self._save_gather_error('Akana api returned non-200 status.  Returned status: {code} . Message: {message}'.format(code=resp.status_code, message=resp_dict['messages']), harvest_job)
            return None

    def fetch_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.AKANA.fetch')
        log.info('In AkanaHarvester fetch_stage')

        # Check harvest object status
        status = get_object_extra(harvest_object, 'status')

        if status == 'new':
            try:
                harvest_object.save()
                return True
            except Exception, e:
                log.exception(e)
                log.debug('Unable to get content for dataset: %s: %r' %
                          (harvest_object, e), harvest_object)
        elif status == 'update':
            try:
                harvest_object.save()
                return True
            except Exception, e:
                log.exception(e)
                log.debug('Unable to get content for dataset: %s: %r' %
                          (harvest_object, e), harvest_object)
        elif status == 'delete':
            return True

    def import_stage(self, harvest_object):
        log.info('In AkanaHarvester import_stage')

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }


        log.info('Current user: %s.', self._get_user_name())
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(HarvestObject.harvest_job_id != harvest_object.harvest_job_id) \
            .order_by(HarvestObject.gathered.desc()) \
            .first()

        # Check harvest object status
        status = get_object_extra(harvest_object, 'status')

        if status == 'delete':
            try:
                p.toolkit.get_action('package_delete')(context, {"id": harvest_object.guid})
                p.toolkit.get_action('dataset_purge')(context, {"id": harvest_object.guid})
                delete_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.guid == harvest_object.guid) .filter(HarvestObject.report_status != "deleted")
                for guid in delete_object:
                    guid.delete()

                log.info('Deleted package {0} with guid {1}'.format(harvest_object.guid, harvest_object.guid))
                model.Session.flush()
                model.Session.commit()

                return True
            except Exception, e:  # TODO p.toolkit.ValidationError handling
                log.exception(e)
                self._save_object_error('%r'%e.error_summary, harvest_object,'Import')

        # Flag previous object as not current anymore
        if previous_object:
            log.info('Setting current status to false of harvest_object with guid: {0}'.format(previous_object.guid))
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        if status == 'new':
            try:
                package_dict = self._get_package_dict(harvest_object)

                # validate tags
                for tag in package_dict['tags']:
                    try:
                        validator_response = v.tag_name_validator(tag['name'], context)
                    except Exception, e:
                        log.exception('Error processing tags: %r', e)
                        self._save_object_error('Error processing tag: %r', tag, 'Import')
                        return None

                # Save reference to the package on the object
                harvest_object.package_id = package_dict['id']

                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()

                # package_dict = akanaJSON
                package_id = p.toolkit.get_action('package_create')(context, package_dict)
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except Exception, e:  # TODO p.toolkit.ValidationError handling
                log.exception(e)
                self._save_object_error('%r'%e.error_summary,harvest_object,'Import')
        elif status == 'update':
            try:

                # Delete the previous object to avoid cluttering the object table
                if previous_object and previous_object.report_status != 'added':
                    log.info('Deleting previous harvest_object, guid: {0} report status: {1}'.format(previous_object.guid, previous_object.report_status))
                    previous_object.delete()
                    model.Session.flush()

                    previous_job_object_count = model.Session.query(HarvestObject) \
                        .filter(HarvestObject.harvest_job_id == previous_object.harvest_job_id) \
                        .count()

                    if previous_job_object_count == 0:
                        log.info('Deleting empty harvest_job with id: %r', previous_object.harvest_job_id)
                        model.Session.query(HarvestJob) \
                            .filter(HarvestJob.id == previous_object.harvest_job_id).delete()




                package_dict = self._get_package_dict(harvest_object)

                # validate tags
                for tag in package_dict['tags']:
                    try:
                        validator_response = v.tag_name_validator(tag['name'], context)
                    except Exception, e:
                        log.exception('Error processing tags: %r', e)
                        self._save_object_error('Error processing tag: %r', tag, 'Import')
                        return None

                # Save reference to the package on the object
                harvest_object.package_id = package_dict['id']
                harvest_object.add()
                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()

                # package_dict = akanaJSON
                package_id = p.toolkit.get_action('package_update')(context, package_dict)
                # log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except Exception, e:  # TODO p.toolkit.ValidationError handling
                log.exception(e)
                self._save_object_error('%r'%e.error_summary,harvest_object,'Import')


        model.Session.commit()

        return True

    def _get_user_name(self):
        '''
        Returns the name of the user that will perform the harvesting actions
        (deleting, updating and creating datasets)

        By default this will be the internal site admin user. This is the
        recommended setting, but if necessary it can be overridden with the
        `ckanext.spatial.harvest.user_name` config option, eg to support the
        old hardcoded 'harvest' user:

           ckanext.spatial.harvest.user_name = harvest

        '''
        if self._user_name:
            return self._user_name

        context = {'model': model,
                   'ignore_auth': True,
                   'defer_commit': True, # See ckan/ckan#1714
                   }
        self._site_user = p.toolkit.get_action('get_site_user')(context, {})

        config_user_name = config.get('ckanext.spatial.harvest.user_name')
        if config_user_name:
            self._user_name = config_user_name
        else:
            self._user_name = self._site_user['name']

        return self._user_name

    def _get_package_dict(self, harvest_object):
        package_dict = {}
        tags = []
        groups = []
        content_dict = json.loads(harvest_object.content)
        source_dataset = p.toolkit.get_action("package_show")({}, {"id": harvest_object.harvest_source_id})

        if source_dataset['owner_org']:
            package_dict['owner_org'] = source_dataset['owner_org']

        harvester_config = json.loads(source_dataset['config'])

        id = content_dict['api-id'] + content_dict['swagger']['info']['version'] + harvest_object.harvest_source_id

        if harvester_config['swagger'] == 'true':
            swag = content_dict['swagger']
            if 'host' not in swag:
                swag['host'] = content_dict['api-gateway']['proxy-endpoint-info']['deployment-zone'] + ":443"

            if 'basePath' not in swag:
                swag['basePath'] = "/" + content_dict['api-gateway']['proxy-endpoint-info']['root-path']

            if 'schemes' not in swag:
                swag['schemes'] = ["https"]

            if 'security' not in swag:
                swag['security'] = [{"api_key": []}]

            if 'securityDefinitions' not in swag:
                swag['securityDefinitions'] = {"api_key": {"type": "apiKey", "in": "header", "name": "Authorization", "description": "Provide the header contents 'bearer YOUR_AUTH_TOKEN' in the text field below"}}

            extras = {'key': 'swagger', 'value': json.dumps(swag)}
            package_dict['extras'] = [extras]

        for tag in harvester_config['default_tags']:
            tags.append(tag)

        for tag in content_dict['api-gateway']['tags']:
            tags.append({'name': tag.replace('=', '-')})

        for group in harvester_config['default_groups']:
            groups.append({'name': group})

        for group in content_dict['api-gateway']['groups']:
            groups.append({'name': group})

        package_dict.update({
            'id': id.lower(),
            'name':  id.lower().replace(".", "-").replace(" ", "_"),
            'title': content_dict['api-gateway']['name'],
            'notes': content_dict['api-gateway']['description'],
            'private': harvester_config['isprivate'],
            'tags': tags,
            'groups': groups,
            'version': content_dict['swagger']['info']['version'],
            'resources': [
                {
                    'name': content_dict['api-gateway']['name'] + ' ' + 'Akana API Portal Link',
                    'url': akana_portal_url + content_dict['api-id'] + '/details',
                    'format': 'API'

                }]

        })

        return package_dict

