glance-irods
============

iRODS Storage Backend for Openstack Glance

It is also as much a WIP as OpenStack is :).

Dependencies
------------
* PyRods, https://code.google.com/p/irodspython/wiki/PyRods
* iRODS, http://irods.org
* Openstack Glance (of course)

Installation
------------
These steps assume the installation onto a glance server.

1. **Install iRODS.** PyRods requires the installation of iRODS server (you won't need to run it).  Just install the basic server; don't get fancy with advanced options.  If, during the setup of PyRods, you receive shared library related errors, you will most likely need to export the CFLAGS and CCFLAGS environment variables to include the -fPIC flag before the irodssetup process.  I installed iRODS into /opt/iRODS, and installed the postgresql (as part of irodssetup) into /opt/iRODS/postgres
2. **Give permissions to glance user.**  The glance user, or whoever will launch glance-api daemon, will need access to the shared libraries underneath iRODS (i.e. chmod -R a+rx /opt/iRODS).
3. **Install PyRods.** Just read the README, including the instructions to set the iRODS directory.
4. **Add to the system shared library path.** Make sure that your libodbc.so.1 of postgresql is in the shared library path for the system, or at least accessible to the user that runs glance.  For Ubuntu, you can add an /etc/ld.so.conf.d/irods.conf with the /opt/iRODS/postgres/pgsql/lib directory, if you followed my iRODS installation in step #1.  Then, run ldconfig.
5. **Copy irods_store.py.** Copy the irods_store.py into /usr/lib/python2.7/dist-packages/glance/store/ directory (or the right location for where python glance packages are installed)
6. **Configuration.** Add or change the following configuration to your glance-api.conf file, which is /etc/glance/glance-api.conf for the glance-api packaged by Ubuntu Cloud Archive.  These are options are found in the [DEFAULT] section:

    \# I prefer irods  
    default_store = irods

    \# you can also tack it onto the end of the list as well  
    known_stores = glance.store.irods\_store.Store

    \# iRODS Store Options  
    irods_store_host = my.exampleirodshost.org  
    irods\_store\_port = 1247  
    irods\_store\_zone = tempZone  
    irod\s_store\_path = /tempZone/home/openstack\_images  
    irods\_store\_user = openstack\_images  
    irods\_store\_password = somepassword  

Questions?
----------
Feel free to email me at edwin@iplantcollaborative.org

License
-------
It's a standard BSD License.
