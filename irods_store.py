"""
Copyright (c) 2011, The Arizona Board of Regents on behalf of
The University of Arizona
All rights reserved.
Developed by: iPlant Collaborative as a collaboration between
participants at BIO5 at The University of Arizona (the primary hosting
institution), Cold Spring Harbor Laboratory, The University of Texas at
Austin, and individual contributors. Find out more at
http://www.iplantcollaborative.org/.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
 * Neither the name of the iPlant Collaborative, BIO5, The University
   of Arizona, Cold Spring Harbor Laboratory, The University of Texas at
   Austin, nor the names of other contributors may be used to endorse or
   promote products derived from this software without specific prior
   written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
Assumption: in iRODs, the path (aka collection) usually embeds the zone.  In
some cases, such as in a federated environment, the zone and a path to a data
object may not be the same.  This implementation assumes that the data object
exists within the same zone.  This could be modified later, if there is a real
use case for it.
Assumption: this implementation also assumes an installation of PyRODS, which
is left to a qualified systems person to get everything installed correctly ;).
https://code.google.com/p/irodspython/wiki/PyRods
Author: Edwin Skidmore
Email: edwin@iplantcollaborative.org
"""

import hashlib
import httplib
import re
import tempfile
import urlparse
from irods.session import iRODSSession
from glance.common import exception
from glance.common import utils
from oslo.config import cfg
import glance.openstack.common.log as logging
import glance.store
import glance.store.base
import glance.store.location
LOG = logging.getLogger(__name__)

irods_opts = [
    cfg.StrOpt('irods_store_host'),
    cfg.IntOpt('irods_store_port', default=1247),
    cfg.StrOpt('irods_store_zone'),
    cfg.StrOpt('irods_store_path'),
    cfg.StrOpt('irods_store_primary_res'),
    cfg.StrOpt('irods_store_replica_res'),
    cfg.StrOpt('irods_store_user', secret=True),
    cfg.StrOpt('irods_store_password', secret=True)
]

CONF = cfg.CONF
CONF.register_opts(irods_opts)


class IrodsManager(object):
    """
    iRODS object for handling all the iRods related functionality
    """

    username = ''
    password = ''
    host = ''
    port = ''
    zone = ''
    datastore = ''
    image_name = ''
    test_path = ''
    irods_conn_object = ''

    def __init__(self, conn_dict):

        self.user = conn_dict['user']
        self.password = conn_dict['password']
        self.host = conn_dict['host']
        self.port = conn_dict['port']
        self.zone = conn_dict['zone']
        self.datastore = conn_dict['path']
        self.test_path = '/%s/home/%s' % (self.zone, self.user)
        # Test Connection
        self.connect_and_confirm()

    def connect_and_confirm(self):
        """ 
        Confirm connection to irods with the given credentials 
        
        """
        

        try:
            msg = (_("connecting to irods://%(host)s:%(port)s%(path)s " +
                     "in zone %(zone)s") %
                   ({'host': self.host, 'port': self.port,
                     'path': self.datastore, 'zone': self.zone}))
            LOG.debug(msg)

            sess = iRODSSession(user=str(self.user), password=str(self.password), host=str(
                self.host), port=int(self.port), zone=str(self.zone))
            coll = sess.collections.get(self.test_path)

        except Exception as e:
            reason = (_("Could not connect with host, port, zone," +
                        " path, or user/password. %s" % (e)))
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="irods",
                                                  reason=reason)
        else:
            self.irods_conn_object = sess
            self.irods_conn_object.cleanup()
        finally:
            sess.cleanup()
            

        # check if file exists

        LOG.debug(_("attempting to check the collection %s" % self.datastore))
        try:
            coll = sess.collections.get(self.datastore)
        except Exception:
            reason = _("collection '%s' is not valid or must be " +
                       "created beforehand") % self.datastore
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="irods",
                                                  reason=reason)
        finally:
            sess.cleanup()

        LOG.debug(_("success"))
        return True

    def get_image_file(self, full_data_path):
        """
        Looks for the image on the path specified or raises exception
        """
        try:

            file_object = self.irods_conn_object.data_objects.get(
                full_data_path)

        except:
            msg = _("image file %s not found") % full_data_path
            Log.error(msg)
            raise exception.NotFound(msg)

        LOG.debug("path = %(path)s, size = %(data_size)s" %
                  ({'path': full_data_path,
                    'data_size': file_object.size}))

        return file_object, file_object.size

    def get_image_file_size(self, full_data_path):
        """
        Returns image size for the file specified or returns 0
        """
        try:

            file_object = self.irods_conn_object.data_objects.get(
                full_data_path)

        except:
            msg = _("image size %s not found") % full_data_path
            Log.error(msg)
            return 0

        LOG.debug("path = %(path)s, size = %(data_size)s" %
                  ({'path': full_data_path,
                    'data_size': file_object.size}))
        return file_object.size

    def delete_image_file(self, full_data_path):
        """
        Deletes image file or returns Exception
        """

        try:
            LOG.debug("opening file %s" % full_data_path)
            file_object = self.irods_conn_object.data_objects.get(
                full_data_path)
        except:
            LOG.error("file not found")
            raise exception.NotFound(msg)

        try:
            file_object.unlink()
            self.irods_conn_object.cleanup()
        except:
            LOG.error("cannot delete file")
            raise exception.Forbidden(store_name="irods", reason=reason)
        LOG.debug("delete success")

    def add_image_file(self, full_data_path, image_file):
        """
        Add image file or return exception
        """

        try:
            LOG.debug("attempting to create image file in irods '%s'" %
                      full_data_path)
            file_object = self.irods_conn_object.data_objects.create(
                full_data_path)
        except:
            LOG.error("file with same name exists in the same path")
            raise exception.Duplicate(_("image file %s already exists "
                                        + "or no perms")
                                      % filepath)

        LOG.debug("performing the write")
        checksum = hashlib.md5()
        bytes_written = 0

        try:
            with file_object.open('r+') as f:
                for buf in utils.chunkreadable(image_file,
                                               ChunkedFile.CHUNKSIZE):
                    bytes_written += len(buf)
                    checksum.update(buf)
                    f.write(buf)
        except Exception as e:
            # let's attempt an delete
            file_object.unlink()
            reason = _('paritial write, transfer failed')
            LOG.error(e)
            raise exception.StorageWriteDenied(reason)
        else:
            checksum_hex = checksum.hexdigest()

            LOG.debug(_("Wrote %(bytes_written)d bytes to %(full_data_path)s, "
                        "checksum = %(checksum_hex)s") % locals())
        finally:
            self.irods_conn_object.cleanup()
            file_object=None
            return [bytes_written, checksum_hex]


class StoreLocation(glance.store.location.StoreLocation):

    """Class describing a Filesystem URI"""

    def process_specs(self):
        self.scheme = self.specs.get('scheme', 'irods')
        self.host = self.specs.get('host')
        self.port = self.specs.get('port')
        self.zone = self.specs.get('zone')
        self.path = self.specs.get('path')
        self.user = self.specs.get('user')
        self.password = self.specs.get('password')
        self.data_name = self.specs.get('data_name')

    def _get_credstring(self):
        if self.user:
            return '%s:%s' % (self.user, self.password)
        return ''

    def get_uri(self):
        return "irods://%s:%s@%s:%s%s/%s" % (
            self.user, self.password, self.host, self.port,
            self.path, self.data_name)

    def parse_uri(self, uri):
        """
        Parses the uri's
        """
        pieces = urlparse.urlparse(uri)
        assert pieces.scheme in ('irods_store', 'irods')
        self.scheme = pieces.scheme
        self.host = pieces.hostname
        self.path = pieces.path.rstrip('/')
        self.user = pieces.username
        self.password = pieces.password

        path_parts = self.path.split('/')
        self.zone = path_parts[1]
        self.data_name = path_parts[path_parts.__len__() - 1]

        if pieces.port is None:
            self.port = 1247
        else:
            self.port = pieces.port

        if self.host is None or self.user is None \
           or self.path is None is None or self.data_name is None:
            reason = _("iRODS URI is invalid")
            LOG.error(reason)
            raise exception.BadStoreUri(message=reason)


class Store(glance.store.base.Store):

    def get_schemes(self):
        return ('irods', 'irods_store')

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exception.BadStoreConfiguration`
        """

        self.host = self._option_get('irods_store_host')
        self.port = self._option_get('irods_store_port')
        self.zone = self._option_get('irods_store_zone')
        self.path = self._option_get('irods_store_path').rstrip('/')
        self.primary_res = self._option_get('irods_store_primary_res')
        self.replica_res = self._option_get('irods_store_replica_res')
        self.user = self._option_get('irods_store_user')
        self.password = self._option_get('irods_store_password')

        if self.host is None or self.zone is None \
           or self.path is None or self.user is None:
            reason = (_("Invalid configuration options, host = '%(host)s', " +
                        "port = '%(port)s', zone = '%(zone)s', " +
                        "path = '%(path)s', user = '%(user)'") %
                      ({'host': self.host, 'port': self.port,
                        'zone': self.zone, 'path': self.path,
                        'user': self.user}))
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="irods",
                                                  reason=reason)

        self.irods_manager = IrodsManager({
            'host': self.host,
            'port': self.port,
            'zone': self.zone,
            'path': self.path,
            'user': self.user,
            'password': self.password,
        })

    def get(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file, and returns a tuple of generator
        (for reading the image file) and image_size
        :param location `glance.store.location.Location` object, supplied
                        from glance.store.location.get_location_from_uri()
        :raises `glance.exception.NotFound` if image does not exist
        """
        full_data_path = self.path + "/" + location.store_location.data_name

        LOG.debug(_("connecting to %(host)s for %(data)s" %
                    ({'host': self.host, 'data': full_data_path})))
        image_file, size = self.irods_manager.get_image_file(full_data_path)

        msg = _("found image at %s. Returning in ChunkedFile.") \
            % full_data_path
        LOG.debug(msg)
        return (ChunkedFile(image_file, self.irods_manager.irods_conn_object), size)

    def get_size(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file, and returns the image_size (or 0
        if unavailable)
        :param location `glance.store.location.Location` object, supplied
                        from glance.store.location.get_location_from_uri()
        """

        full_data_path = self.path + "/" \
            + location.store_location.data_name

        return self.irods_manager.get_image_file_size(full_data_path)

    def delete(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file to delete
        :location `glance.store.location.Location` object, supplied
                  from glance.store.location.get_location_from_uri()
        :raises NotFound if image does not exist
        :raises Forbidden if cannot delete because of permissions
        """
        full_data_path = self.path + "/" + location.store_location.data_name

        LOG.debug("connecting to %(host)s for %(data)s" %
                  ({'host': self.host, 'data': full_data_path}))

        self.irods_manager.delete_image_file(full_data_path)

    def add(self, image_id, image_file, image_size):
        """
        Stores an image file with supplied identifier to the backend
        storage system and returns an `glance.store.ImageAddResult` object
        containing information about the stored image.
        :param image_id: The opaque image identifier
        :param image_file: The image data to write, as a file-like object
        :param image_size: The size of the image data to write, in bytes
        :retval `glance.store.ImageAddResult` object
        :raises `glance.common.exception.Duplicate` if the image already
                existed
        :note By default, the backend writes the image data to a file
              `/<DATADIR>/<ID>`, where <DATADIR> is the value of
              the filesystem_store_datadir configuration option and <ID>
              is the supplied image ID.
        """
        full_data_path = self.path + "/" + image_id

        LOG.debug("connecting to %(host)s for %(data)s" %
                  ({'host': self.host, 'data': full_data_path}))

        bytes_written, checksum_hex = self.irods_manager.add_image_file(
            full_data_path, image_file)

        loc = StoreLocation({'scheme': 'irods',
                             'host': self.host,
                             'port': self.port,
                             'zone': self.zone,
                             'path': self.path,
                             'user': self.user,
                             'password': self.password,
                             'data_name': image_id})
        return (loc.get_uri(), bytes_written, checksum_hex, {})

    def _option_get(self, param):
        result = getattr(CONF, param)
        if not result:
            reason = _("Could not find %(param)s in configuration options.") \
                % locals()
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="s3",
                                                  reason=reason)
        return result


class ChunkedFile(object):

    """
    We send this back to the Glance API server as
    """

    """original: CHUNKSIZE = 65536"""
    """256MB"""
    CHUNKSIZE = 256 * 1024 * 1024

    def __init__(self, fp, conn_obj):
        self.fp = fp
        self.conn_obj = conn_obj

    def __iter__(self):
        """Return an iterator over the image file"""
        try:
            f = self.fp.open('r+')
            while True:
                chunk = f.read(ChunkedFile.CHUNKSIZE)
                if chunk:
                    yield chunk
                else:
                    break

        except:
            print "Error while reading file in chunks. Please see ChunkedFile class"
        finally:
            self.close()
    def close(self):
        """ Close internal file pointer """
        if self.fp:
            self.fp = None
            self.conn_obj.cleanup()
