# ckanext-akanaharvester
This extension provides IHarvest Integration for pulling API's from Akana portal

Works for ckan>=2.5

## Installation

Use `pip` to install this plugin.


```
pip install -e 'git+https://github.com/merkelct/ckanext-akanaharvester

## Configuration
***You must have a working ckanext-harvester before this extension will function properly
Make sure to add the following in your config file:

```
ckan.harvester.id = <APP_CLIENT_ID>
ckan.harvester.secret = <APP_SECRET>
```

## Helper Functions

custom helpers available to the templates

```
get_grps() - returns the obj tags
get_tags() - returns the obj grps

```

all helpers will be available as h.<helper name>(<vars>)


## Usage

You must set up a harvester in the Harvest panel and then you can manually harvest

Dependencies
------------

* pingi (must be installed for auth to the service and configured with clinet and app ID)

