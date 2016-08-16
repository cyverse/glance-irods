glance-irods
============

iRODS Storage Backend for Openstack Glance

Dependencies
------------
* Python-irodsclient (Maintained by irods)
* Openstack Glance (of course)

Installation
------------
These steps assume the installation onto a glance server.

1. **Install iRODS** ``` pip install git+git://github.com/irods/python-irodsclient.git```
2. **Configuration** Add or change the following configuration to your glance-api.conf file, which is /etc/glance/glance-api.conf for the glance-api packaged by Ubuntu Cloud Archive.  These are options are found in the [DEFAULT] section:

    \# I prefer irods  
    default_store = irods

    \# you can also tack it onto the end of the list as well  
    known_stores = glance.store.irods\_store.Store

    \# iRODS Store Options  
    irods_store_host = my.exampleirodshost.org  
    irods\_store\_port = 1247  
    irods\_store\_zone = tempZone  
    irods\_store\_path = /tempZone/home/openstack\_images  
    irods\_store\_user = openstack\_images  
    irods\_store\_password = somepassword  

3. **Setup** Copy-paste irods_store.py to /usr/lib/python2.7/glance/store and restart glance-api


Questions?
----------
Feel free to email me at edwin@iplantcollaborative.org

License
-------
It's a standard BSD License. See LICENSE.txt
