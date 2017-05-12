import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit


def str_to_obj(str):
    if str == "":
        pass
    else:
        return eval(str)


def get_tags(obj):
    tag_list = ""
    for tag in obj:
        if tag_list == "":
            tag_list = tag['name']
        else:
            tag_list += "," + tag['name']

    return tag_list


def get_grps(obj):
    grp_list = ""
    for grp in obj:
        if grp_list == "":
            grp_list = grp
        else:
            grp_list += "," + grp

    return grp_list

class AkanaharvesterPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'akanaharvester')

    def get_helpers(self):
        return {
            'harvester_str_to_obj': str_to_obj,
            'harvester_get_tags': get_tags,
            'harvester_get_grps': get_grps
        }
