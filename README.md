# glance-irods: iRODS Storage Back-end for OpenStack Glance

## Dependencies
* [Python-irodsclient](https://pypi.python.org/pypi/python-irodsclient)
* OpenStack Glance

## Manual Installation

This procedure is intended for OpenStack Newton release; procedure may differ for other versions of OpenStack.

These steps assume you are installing glance-irods on an OpenStack Glance server or container.

1. Install python-irodsclient

```
pip install python-irodsclient
```

2. Configure Glance

Add or change the following configuration to the appropriate sections of your `glance-api.conf`; this file is stored in `/etc/glance/glance-api.conf` for glance-api as packaged by OpenStack-Ansible and Ubuntu Cloud Archive.

```
[default]
# Replace the following with connection details for your own iRODS deployment
irods_store_host = my.exampleirodshost.org
irods_store_port = 1247
irods_store_zone = tempZone
irods_store_path = /tempZone/home/openstack_images
irods_store_user = openstack_images
irods_store_password = somepassword

[glance_store]
stores = irods  # You may have a comma-separated list of multiple back-ends
default_store = irods

[glance_store.drivers]
glance.store.irods.Store = glance_store._drivers.irods:Store
irods = glance_store._drivers.irods:Store
```


3. Install glance-irods

- Copy irods.py to the `_drivers` folder of your glance_store Python package. For an OpenStack-Ansible deployment, this may be
`/openstack/venvs/glance-14.0.4/lib/python2.7/site-packages/glance_store/_drivers`

- Restart glance-api, e.g. `systemctl restart glance-api`


## Questions?

Feel free to email me at edwin@iplantcollaborative.org

## License

It's a standard BSD License. See LICENSE.txt
