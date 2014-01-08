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

from irods import *

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


class StoreLocation(glance.store.location.StoreLocation):
    """
    Class describing an iRODS URI, in the form:

    irods://user:password@host:port/zone/subpath/to/collection

    NOTE: in irods, path actually includes the zone.
    """

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

        # for now, unless there is a compelling use case,
        # let's just assume the zone embedded in the path
        path_parts = self.path.split('/')
        self.zone = path_parts[1]
        self.data_name = path_parts[path_parts.__len__()-1]

        # we'll assume a default if not set
        if pieces.port is None:
            self.port = 1247
        else:
            self.port = pieces.port

        # let's do a few preliminary checks, values are
        # according to urlparse
        if self.host is None or self.user is None \
           or self.path is None is None or self.data_name is None:
            reason = _("iRODS URI is invalid")
            LOG.error(reason)
            raise exception.BadStoreUri(message=reason)


class Store(glance.store.base.Store):

    def get_schemes(self):
        return ('irods_store', 'irods')

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
        self.scheme = 'irods'

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

        try:
            msg = (_("connecting to irods://%(host)s:%(port)s%(path)s " +
                     "in zone %(zone)s") %
                   ({'host': self.host, 'port': self.port,
                     'path': self.path, 'zone': self.zone}))
            LOG.debug(msg)
            conn, err = rcConnect(self.host, self.port, self.user, self.zone)
            status = clientLoginWithPassword(conn, self.password)
        except Exception:
            reason = (_("Could not connect with host, port, zone," +
                        " path, or user/password"))
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="irods",
                                                  reason=reason)

        # check if file exists
        # using queryCollAcl, if for no other reason than to easily check
        # if it exists.  There ought to be a easier way PyRods!
        LOG.debug("attempting to check the collection %s" % self.path)
	try:
	    c = irodsCollection(conn, self.path)
	except Exception:
            reason = _("collection '%s' is not valid or must be " +
                       "created beforehand") % self.path
            LOG.error(reason)
            conn.disconnect()
            raise exception.BadStoreConfiguration(store_name="irods",
                                                  reason=reason)

        # for now, let's just close junk up
        LOG.debug("success")
        conn.disconnect()

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

        LOG.debug("connecting to %(host)s for %(data)s" %
                  ({'host': self.host, 'data': full_data_path}))
        conn, err = rcConnect(self.host, self.port, self.user, self.zone)
        status = clientLoginWithPassword(conn, self.password)
        if self.primary_res is None:
            f = irodsOpen(conn, full_data_path, 'r')
        else:
            f = irodsOpen(conn, full_data_path, 'r', self.primary_res)

        if f is None:
            msg = _("image file %s not found") % full_data_path
            Log.debug(msg)
            conn.disconnect()
            raise exception.NotFound(msg)
        else:
            s = self.get_size(location)

            msg = _("found image at %s. Returning in ChunkedFile.") \
                % full_data_path
            LOG.debug(msg)

            # it is assumed that the ChunkedFile will close the file correctly
            return (ChunkedFile(f, conn), s)

    def get_size(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file, and returns the image_size (or 0
        if unavailable)

        :param location `glance.store.location.Location` object, supplied
                        from glance.store.location.get_location_from_uri()
        """
        try:

            full_data_path = self.path + "/" \
                + location.store_location.data_name

            LOG.debug("connecting to %(host)s for %(data)s" %
                      ({'host': self.host, 'data': full_data_path}))
            conn, err = rcConnect(self.host, self.port, self.user, self.zone)
            status = clientLoginWithPassword(conn, self.password)

            LOG.debug("obtaining collection to file resources")
            c = irodsCollection(conn, self.path)
            objs = c.getObjects()

            # I have to grab the resource in this way, bleh
            # this assumes the data object exists within the
            # collection of the store
            resource = None
            for sublist in objs:
                if sublist[0] == location.store_location.data_name:
                    resource = sublist[1]

            # I need to obtain the resource, but that is only
            # accessible from the collection, apparently
            if (resource is None):
                msg = "could not obtain resource for %s" % full_data_path
                LOG.error(msg)
                raise BackendException(msg)
            info = getFileInfo(conn, full_data_path, resource)

            conn.disconnect()
            LOG.debug("path = %(path)s, size = %(data_size)s" %
                      ({'path': full_data_path,
                        'data_size': info['data_size']}))
            return info['data_size']
        except Exception:
            conn.disconnect()
            return 0

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
        conn, err = rcConnect(self.host, self.port, self.user, self.zone)
        status = clientLoginWithPassword(conn, self.password)

        LOG.debug("opening file %s" % full_data_path)
        f = irodsOpen(conn, full_data_path, 'r')
        if f is None:
            LOG.error("file not found")
            if conn is not None:
                conn.disconnect
            raise exception.NotFound(store_name="irods", reason=reason)

        retval = f.delete()
        conn.disconnect()
        if retval == 0:
            LOG.debug("delete success")
        else:
            LOG.error("apparently, cannot delete file!")
            raise exception.Forbidden(store_name="irods", reason=reason)

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
        conn, err = rcConnect(self.host, self.port, self.user, self.zone)
        status = clientLoginWithPassword(conn, self.password)

        LOG.debug("attempting to open irods file '%s'" % full_data_path)
        if self.primary_res is None:
            f = irodsOpen(conn, full_data_path, "w")
        else:
            f = irodsOpen(conn, full_data_path, "w", self.primary_res)

        if f is None:
            conn.close()
            raise exception.Duplicate(_("image file %s already exists "
                                        + "or no perms")
                                      % filepath)

        LOG.debug("performing the write")
        checksum = hashlib.md5()
        bytes_written = 0

        try:
            for buf in utils.chunkreadable(image_file,
                                           ChunkedFile.CHUNKSIZE):
                bytes_written += len(buf)
                checksum.update(buf)
                f.write(buf)
        except Exception:
            # let's attempt an delete
            f.delete()
            reason = _('paritial write, transfer failed')
            LOG.error(reason)
            f.close()
            conn.disconnect()
            raise exception.StorageWriteDenied(reason)

        f.close()
        conn.disconnect()

        # let's attempt a replication, but only if the replica resource is not the same as the primary resource
        # if the replication fails, then just ignore
	# TODO: irodsFile.replicate() does not work with PyRods v3.2.6 due to a type error.   For now, replication will be disabled! boo
	#        if self.replica_res is not None and self.replica_res != self.primary_res :
	#            try:
	#                f = irodsOpen(conn, full_data_path, 'r')
	#                f.replicate(self.replica_res)
	#                f.close()
	#            except Exception:
	#                LOG.error("WARNING: could not replicate '" + full_data_path + "' to resource '" + self.replica_res + "'")

        checksum_hex = checksum.hexdigest()

        LOG.debug(_("Wrote %(bytes_written)d bytes to %(full_data_path)s, "
                  "checksum = %(checksum_hex)s") % locals())
        loc = StoreLocation({'scheme': self.scheme,
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
    CHUNKSIZE = 256*1024*1024

    def __init__(self, fp, conn):
        self.fp = fp
        self.conn = conn

    def __iter__(self):
        """Return an iterator over the image file"""
        try:
            while True:
                chunk = self.fp.read(ChunkedFile.CHUNKSIZE)
                if chunk:
                    yield chunk
                else:
                    break
        finally:
            self.close()

    def close(self):
        """Close the internal file pointer"""
        if self.fp:
            self.fp.close()
            self.fp = None
            self.conn.disconnect()
            self.conn = None
