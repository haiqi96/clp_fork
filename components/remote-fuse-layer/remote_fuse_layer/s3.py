import errno
import io
import logging
import math
import os
import pathlib
import queue
import shutil
import threading
import time
import typing
import uuid
from functools import partial

import boto3
import diskcache
import smart_open
from fuse import FUSE, FuseOSError, Operations
from s3path import PureS3Path

logger = logging.getLogger()
# Create a console handler and set its log level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and attach it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


# Constants
S3_SEQUENTIAL_READ_BUFFER_SIZE = 128 * 1024   # 128KB
# No point in caching if we are reading the file sequentially just once, since the stream is already buffered
ENABLE_READ_KERNEL_FILE_CACHE = False
S3_SEQUENTIAL_WRITE_STREAM_BUFFER_SIZE = 128 * 1024  # 128KB
METADATA_USE_KERNEL_PAGE_CACHE_FOR_WRITE = True
SEGMENT_USE_KERNEL_PAGE_CACHE_FOR_WRITE = True

S3_SEQUENTIAL_READ_STREAM_BUFFER_SIZE = 128 * 1024  # 128KB
METADATA_KERNEL_FILE_CACHE_FOR_READ = True
SEGMENT_KERNEL_FILE_CACHE_FOR_READ = True   # Change this to False if we there's no affinity to the partition id searched and if there's a lot of data

METADATA_GENERATED_FILES = [
    "metadata.db-journal",
    "metadata.db-wal"
]

METADATA_FILENAMES = [
    "logtype.dict",
    "logtype.segindex",
    "metadata",
    "metadata.db",
    "var.dict",
    "var.segindex",
]

# Types
class DiskCacheEvictionPolicies:
    LRS = "least-recently-stored"  # fastest
    LRU = "least-recently-used"
    LFU = "least-frequently-used"
    N = "none"


class DirectoryNode:
    __slots__ = ('directory_names', 'files')

    def __init__(self):
        self.directory_names = set()
        self.files = set()


class FileMetadata:
    __slots__ = ('st_size', 'ordered_read_pending')

    def __init__(self, st_size: int, ordered_read_pending: bool = True):
        self.st_size = st_size
        self.ordered_read_pending = ordered_read_pending


class GetAttrCache:
    __slots__ = ('files', 'gid', 'uid', 'time')

    def __init__(self):
        self.files = {}
        self.gid = os.getgid()
        self.uid = os.getuid()
        self.time = time.time()

    def get_with_fuse_path_str(self, fuse_path_str: str):
        try:
            if any(fuse_path_str.endswith(extension) for extension in [".gz", ".zstd", "xz"]):
                # To make sure cp/clp or other commands which depends on file size to work properly, file attribute st_size is increased just to be safe
                # i.e. 1MB file will appear as 1GB file (1024x the original size). The real file size can be identified by reading the file until EOF.
                st_size = 0
            else:
                st_size = self.files[fuse_path_str].st_size
            return {
                'st_atime': self.time,  # time of most recent access
                'st_ctime': self.time,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
                # Platform dependent: time of most recent metadata change on Unix; time of creation on Windows
                'st_gid': self.gid,  # group id of owner
                'st_mode': 33060,  # protection bits -r--r--r--
                'st_mtime': self.time,  # time of most recent content modification
                'st_nlink': 1,  # number of hard links
                'st_size': st_size,  # size of file
                'st_uid': self.uid,  # user id of owner
                'st_blocks': math.ceil(st_size / 512)  # hard code to 512 bytes block size
            }
        except KeyError:
            # Directory
            return {
                'st_atime': self.time,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
                'st_ctime': self.time,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
                # Platform dependent: time of most recent metadata change on Unix; time of creation on Windows
                'st_gid': self.gid,  # group id of owner
                'st_mode': 16676,  # protection bits dr--r--r--
                'st_mtime': self.time,  # Does not matter in the current S3 fuse implementation
                'st_nlink': 1,  # number of hard links
                'st_size': 512,  # size of file, in bytes
                'st_uid': self.uid,  # user id of owner,
                'st_blocks': 1
            }

    def add_with_fuse_path_str(self, fuse_path_str: str, st_size: int, ordered_read_pending: bool = False):
        fuse_path = pathlib.Path(fuse_path_str)
        if not fuse_path.is_absolute():
            fuse_path = pathlib.Path('/') / fuse_path
        self.files[str(fuse_path)] = FileMetadata(st_size=st_size, ordered_read_pending=ordered_read_pending)

    def set_ordered_read_pending_state_from_fuse_path_str(self, fuse_path_str: str, state: bool):
        self.files[fuse_path_str].ordered_read_pending = state


class ReadDirCache:
    """
    Class to cache FUSE path directory entries
    """
    __slots__ = 'directory_nodes'

    def __init__(self):
        self.directory_nodes = {"/": DirectoryNode()}   # Use relative key (relative to cwd) as "key"

    def exists(self, fuse_path_str):
        return fuse_path_str in self.directory_nodes

    def get_with_fuse_path_str(self, fuse_path_str: str):
        directory_node = self.directory_nodes[fuse_path_str]
        yield from ['.', '..']
        yield from directory_node.directory_names
        yield from directory_node.files

    def add_file_with_fuse_path_str(self, fuse_path_str: str):
        fuse_path = pathlib.Path(fuse_path_str)
        fuse_path_parts = fuse_path.parts
        parent_dir = pathlib.Path('/')
        parent_dir_node = self.directory_nodes[str(parent_dir)]
        for i in range(0, len(fuse_path_parts) - 1):
            current_dir_name = fuse_path_parts[i]
            current_dir = parent_dir / current_dir_name
            current_dir_str = str(current_dir)

            # Get or create corresponding directory node
            if current_dir_str in self.directory_nodes:
                current_dir_node = self.directory_nodes[current_dir_str]
            else:
                # New directory
                current_dir_node = DirectoryNode()
                self.directory_nodes[current_dir_str] = current_dir_node

                # Add to parent directory
                parent_dir_node.directory_names.add(current_dir_name)

            parent_dir = current_dir
            parent_dir_node = current_dir_node

        # NOTE: We can't simply use fuse_path_str or str(fuse_path) as the key since it won't include the root parent directory
        parent_dir_node.files.add(fuse_path_parts[-1])


class S3MountConfig:
    @staticmethod
    def __validate_s3_path(s3_path_str):
        """
        Checks whether the given S3 path: i) contains redundant path characters (e.g., "./"), ii) is absolute, iii) ends with a trailing slash.
        :param s3_path_str:
        :return: A tuple -- (whether the path contained redundant characters, whether the path is absolute, whether the path ends with a '/')
        """
        s3_path = PureS3Path(s3_path_str)

        s3_path_is_absolute = s3_path.is_absolute()

        cleaned_s3_path = str(s3_path)
        if cleaned_s3_path == s3_path_str:
            return False, s3_path_is_absolute, False

        if len(s3_path_str) > 1 and '/' == s3_path_str[-1] and cleaned_s3_path == s3_path_str[:-1]:
            return False, s3_path_is_absolute, True

        return True, s3_path_is_absolute, False

    __slots__ = (
        'access_key_id',
        'secret_access_key',
        'session_token',

        'endpoint_url',

        's3_path_prefix',
        's3_bucket',
        's3_object_key_prefix',
        's3_path_prefix_for_reconstruction',
    )

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        s3_path_prefix_str: str,  # to be deprecated
        endpoint_url: str,
        s3_path_prefix_to_remove_from_mount: typing.Optional[str] = None,
        s3_path_prefix: typing.Optional[str] = None,
        session_token: typing.Optional[str] = None,
    ):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.endpoint_url = endpoint_url

        # Prefer `s3_path_prefix` but keep the old arg `s3_path_prefix_str`
        if s3_path_prefix is None:
            s3_path_prefix = s3_path_prefix_str
        self.s3_path_prefix = s3_path_prefix

        # Ensure S3 path contains no redundant characters
        path_contains_redundant_chars, path_is_absolute, path_ends_with_trailing_slash = self.__validate_s3_path(s3_path_prefix)
        if path_contains_redundant_chars:
            raise ValueError(f"S3 path '{s3_path_prefix}' contains redundant path characters.")
        # Ensure S3 path is absolute
        if not path_is_absolute:
            raise ValueError(f"S3 path '{s3_path_prefix}' must be absolute.")

        # Ensure prefix to remove is part of complete path
        if s3_path_prefix_to_remove_from_mount is not None and s3_path_prefix.find(s3_path_prefix_to_remove_from_mount) != 0:
            raise ValueError(f"Could not find prefix '{s3_path_prefix_to_remove_from_mount}' in '{s3_path_prefix}'")

        parsed_s3_path_prefix = PureS3Path(s3_path_prefix)
        self.s3_bucket = parsed_s3_path_prefix.bucket
        s3_object_key_prefix = parsed_s3_path_prefix.key

        if s3_object_key_prefix != "" and path_ends_with_trailing_slash:
            # Add the trailing slash
            s3_object_key_prefix += '/'
        self.s3_object_key_prefix = s3_object_key_prefix

        if s3_path_prefix_to_remove_from_mount is None:
            self.s3_path_prefix_for_reconstruction = ""
        else:
            self.s3_path_prefix_for_reconstruction = s3_path_prefix_to_remove_from_mount

            # Setup prefix for object key deconstruction and path reconstruction (from a FUSE path)
            leftover_path_str = self.s3_path_prefix[len(s3_path_prefix_to_remove_from_mount):]
            if len(leftover_path_str) == 0 or PureS3Path(leftover_path_str).is_absolute():
                # NOTE: Either the prefix is a bucket alone or the leftover part begins with a trailing slash. In either case, we need to append a '/' so that
                #  so that reconstruction is simply a join process
                self.s3_path_prefix_for_reconstruction += '/'

    def create_new_connection(self):
        return Boto3Connection(self)

    def get_s3_path_prefix_as_uri(self):
        # NOTE: s3_path_prefix is absolute, so we can remove one slash from "s3://"
        return "s3:/" + self.s3_path_prefix

    def get_fuse_path(self, key):
        """
        Converts an object key into its corresponding path in the FUSE mount
        :param key:
        :return: The FUSE mount path corresponding to the key
        """
        key_contains_redundant_characters, key_is_absolute, key_ends_with_trailing_slash = self.__validate_s3_path(key)
        if key_contains_redundant_characters or key_is_absolute or key_ends_with_trailing_slash:
            # NOTE:
            # - If key is absolute, that means it is preceded by a redundant '/' that should only be present when joined with the bucket
            # - If key ends with a trailing slash, that means the object name contains a slash
            raise ValueError("S3 object key '" + key + "' contains redundant path characters.")

        fuse_path_str = "/{}/{}".format(self.s3_bucket, key)

        return fuse_path_str[len(self.s3_path_prefix_for_reconstruction):]


class Boto3Connection:
    __slots__ = ('session', 'resource', 'client')

    def __init__(self, s3_mount_config: S3MountConfig):
        if s3_mount_config is not None:
            self.session = boto3.session.Session(
                aws_access_key_id=s3_mount_config.access_key_id,
                aws_secret_access_key=s3_mount_config.secret_access_key,
                aws_session_token=s3_mount_config.session_token,
            )
            self.resource = self.session.resource("s3", endpoint_url=s3_mount_config.endpoint_url)
            self.client = self.resource.meta.client
        else:   # User wants a blank connection object
            self.session = None
            self.resource = None
            self.client = None


class ArchiveMetadataContainerFile:
    __slots__ = ('file', 'file_pos', 'max_file_pos', 'file_path', 'is_open')

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_pos = 0
        self.max_file_pos = 0
        self.is_open = False

    def create_and_open(self):
        self.file = open(self.file_path, mode="wb+")
        self.file_pos = 0
        self.max_file_pos = 0
        self.is_open = True

    def write(self, buf, offset):
        file_pos = self.file_pos
        if offset != file_pos:
            self.file.seek(offset)
            file_pos = offset
        num_bytes_written = self.file.write(buf)
        self.file_pos = file_pos + num_bytes_written
        if self.file_pos > self.max_file_pos:
            self.max_file_pos = self.file_pos
        return num_bytes_written

    def read(self, offset, length):
        file_pos = self.file_pos
        if offset != file_pos:
            self.file.seek(offset)
            file_pos = offset
        self.file_pos = file_pos + length
        if self.file_pos > self.max_file_pos:
            self.file_pos = self.max_file_pos
        result = self.file.read(length)
        return result

    def reopen_for_writing(self):
        self.file = open(self.file_path, mode="wb+")
        self.file_pos = 0
        self.max_file_pos = os.path.getsize(self.file_path)
        self.is_open = True

    def close(self):
        self.is_open = False
        self.file.close()
        self.file_pos = 0

    def reopen_for_reading(self):
        self.file = open(self.file_path, mode="rb")
        self.is_open = True

    def get_size(self):
        return self.max_file_pos

    def unlink(self):
        os.unlink(self.file_path)

class ArchiveMetadataContainer:
    __slots__ = ("metadata_files", "filename_to_index_mapping")

    def __init__(self, mount_cache_path: pathlib.Path, archive_name: str):
        archive_dir = mount_cache_path / archive_name
        archive_dir.mkdir(parents=True, exist_ok=True)

        self.metadata_files = []
        self.filename_to_index_mapping = {}
        index = 0
        for filename in METADATA_FILENAMES:
            self.metadata_files.append(ArchiveMetadataContainerFile(archive_dir / filename))
            self.filename_to_index_mapping[filename] = index
            index += 1
        # Note: We do want to avoid writing those information at end.
        for filename in METADATA_GENERATED_FILES:
            self.metadata_files.append(ArchiveMetadataContainerFile(archive_dir / filename))
            self.filename_to_index_mapping[filename] = index
            index += 1

    def all_files_done_writing(self):
        result = True
        for i in range(len(METADATA_FILENAMES)):
            filename = METADATA_FILENAMES[i]
            if self.metadata_files[i].is_open:
                return False
        logger.debug("All metadata files closed")
        return True

    def size_bytes(self):
        total_size = 0
        for metadata_file in self.metadata_files:
            total_size += metadata_file.max_file_pos
        return total_size

    def read_into(self, object_stream: io.BufferedIOBase):
        # Only when all files are closed do we flush the files. We write the calculated offset of the next files in the beginning
        # File layout: [32bit offset1, 32bit offset2, ...][file1][file2]...
        file_end_positions = []
        num_offsets = len(METADATA_FILENAMES) - 1
        offset = num_offsets * 4  # Init to the header length
        for i in range(0, num_offsets):
            offset += self.metadata_files[i].max_file_pos
            file_end_positions.append(offset)
            file_name = METADATA_FILENAMES[i]
        # Write offset to object stream
        for file_end_pos in file_end_positions:
            object_stream.write(file_end_pos.to_bytes(4, byteorder="little"))   # Writing to network, maybe big endian is better

        # Write each file to the object stream, then close all of them
        for index in range(len(METADATA_FILENAMES)):
            metadata_file = self.metadata_files[index]
        #for metadata_file in self.metadata_files:
            metadata_file.reopen_for_reading()
            for chunk in iter(partial(metadata_file.file.read, S3_SEQUENTIAL_WRITE_STREAM_BUFFER_SIZE), b''):
                mv = memoryview(chunk)   # https://julien.danjou.info/high-performance-in-python-with-zero-copy-and-the-buffer-protocol/
                while mv:   # Ensure we copied all of the bytes
                    bytes_written = object_stream.write(mv)
                    mv = mv[bytes_written:]
            metadata_file.close()
        object_stream.close()


class ManagedSequentialReadOptimizedStream:   # Advanced version with small file prefetch
    __slots__ = (
        'boto3_connection',
        'fuse_path_str',
        's3_path_prefix_for_reconstruction',
        'object_stream',
        'bytes_read',
        'small_read_buf',
        'uri',
    )

    def __init__(self, s3_mount_config: S3MountConfig):
        self.boto3_connection = s3_mount_config.create_new_connection()   # Each instance has its own independent boto3 connection for thread safety
        self.s3_path_prefix_for_reconstruction = s3_mount_config.s3_path_prefix_for_reconstruction

        self.fuse_path_str = None
        self.uri = None
        self.object_stream = None
        self.bytes_read = 0
        self.small_read_buf = None

    def open_with_fuse_path_str(self, fuse_path_str: str, st_size: typing.Optional[int] = None):
        self.fuse_path_str = fuse_path_str

        # Convert FUSE path into its corresponding S3 URI
        # NOTE: The path prefix and fuse path both begin with a '/', so we need to remove them before the join
        self.uri = "s3://" + self.s3_path_prefix_for_reconstruction[1:] + fuse_path_str[1:]

        # Close previous object stream if it's not terminated
        if self.object_stream:
            self.close()

        self.object_stream = smart_open.open(self.uri, mode='rb', transport_params={
            "buffer_size": S3_SEQUENTIAL_READ_BUFFER_SIZE,
            "resource": self.boto3_connection.resource,
        }, ignore_ext=True)

        if st_size is not None and st_size <= S3_SEQUENTIAL_READ_BUFFER_SIZE:
            # For small files, we'll store the content of the stream into a bytesIO buffer and close the stream
            self.small_read_buf = io.BytesIO(self.object_stream.read())
            self.small_read_buf.seek(0)
            self.object_stream.close()
        return self

    def close(self):
        if self.is_opened():
            self.object_stream.close()
            self.object_stream = None
        if self.small_read_buf:
            self.small_read_buf.close()
            self.small_read_buf = None
        self.bytes_read = 0  # Clean up
        return self

    def is_closed(self):
        return self.object_stream is None

    def is_opened(self):
        return self.object_stream is not None

    def read(self, length, offset):
        if self.is_closed():
            return b""
        if self.small_read_buf:   # Small read buf available
            return self.small_read_buf.read(length, offset)
        else:
            if offset != self.bytes_read:
                self.object_stream.seek(offset)
                self.bytes_read = offset  # Clean up
            stream_bytes = self.object_stream.read(length)
            self.bytes_read += len(stream_bytes)
        return stream_bytes


class SequentialAccessOptimizedObjectStream:
    __slots__ = ('object_stream', 'bytes_processed')

    def __init__(self, object_stream):
        self.object_stream = object_stream
        self.bytes_processed = 0

    def read(self, length, offset):
        if offset != self.bytes_processed:
            self.object_stream.seek(offset)
        stream_bytes = self.object_stream.read(length)
        self.bytes_processed += len(stream_bytes)
        return stream_bytes

    def write(self, buf, offset):
        if offset != self.bytes_processed:
            self.object_stream.seek(offset)
            self.bytes_processed = offset
        bytes_written = self.object_stream.write(buf)
        self.bytes_processed += bytes_written
        return bytes_written

    def fsync(self):
        self.flush()

    def flush(self):
        self.object_stream.flush()

    def close(self):
        self.object_stream.close()

    def get_stream(self):
        return self.object_stream

    def tell(self):
        return self.object_stream.tell()


class S3FuseSequentialRead(Operations):
    __slots__ = (
        's3_mount_config',
        'readdir_cache',
        'getattr_cache',
        'preopened_object_stream_queue',
        'recycled_object_stream_queue',
        'preopened_object_streams',
        'opened_object_streams',
        'ordered_reads'
    )

    def __init__(self, s3_mount_config: S3MountConfig, readdir_cache: ReadDirCache, getattr_cache: GetAttrCache, preopened_object_stream_queue,
                 recycled_object_stream_queue, ordered_reads):
        self.s3_mount_config = s3_mount_config
        self.readdir_cache = readdir_cache
        self.getattr_cache = getattr_cache
        self.preopened_object_stream_queue = preopened_object_stream_queue
        self.recycled_object_stream_queue = recycled_object_stream_queue

        self.preopened_object_stream = {}
        self.opened_object_streams = {}
        self.ordered_reads = ordered_reads

        logger.debug("Initialized read only S3 FUSE file system optimized for clp compression from S3")

    def create_object_stream(self, fuse_path_str: str):
        try:
            opened_object_stream = self.recycled_object_stream_queue.get_nowait()
            opened_object_stream.open_with_fuse_path_str(fuse_path_str)
            return opened_object_stream
        except queue.Empty:
            unordered_object_stream = ManagedSequentialReadOptimizedStream(self.s3_mount_config)
            unordered_object_stream.open_with_fuse_path_str(fuse_path_str)
            return unordered_object_stream

    # Filesystem methods
    # ==================
    def getattr(self, fuse_path_str, fh=None):
        logger.debug("getattr(" + fuse_path_str + ')')
        # Everything should be cached. However, if one enters an incorrect path, it may appear as a folder instead
        return self.getattr_cache.get_with_fuse_path_str(fuse_path_str)

    def opendir(self, path):
        if not self.readdir_cache.exists(path):
            raise FuseOSError(errno.ENOENT)
        return 0

    def readdir(self, fuse_path_str, fh):
        logger.debug("readdir(" + fuse_path_str + ')')
        try:
            yield from self.readdir_cache.get_with_fuse_path_str(fuse_path_str)
        except KeyError as key_error:
            logger.error("Trying to perform readdir on the invalid directory " + fuse_path_str)
            raise FuseOSError(errno.ENOENT) from key_error

    # File methods
    # ============
    def open(self, fuse_path_str, raw_fi):
        logger.debug("open(" + fuse_path_str + ')')
        if fuse_path_str in self.opened_object_streams:
            # Does not support opening the same file multiple times simultaneously
            raise FuseOSError(errno.EBUSY)

        if any(fuse_path_str.endswith(extension) for extension in [".gz", ".zstd", "xz"]):
            raw_fi.direct_io = True
        raw_fi.keep_cache = ENABLE_READ_KERNEL_FILE_CACHE

        if self.ordered_reads:
            # Seek to the object stream we need
            while True:
                opened_object_stream = self.preopened_object_stream_queue.get()

                if fuse_path_str == opened_object_stream.fuse_path_str:
                    self.opened_object_streams[fuse_path_str] = opened_object_stream
                    return 0

                opened_object_stream.close()
                self.recycled_object_stream_queue.put(opened_object_stream)
        else:
            try:
                # Transfer stream from preopened to opened dictionary
                self.opened_object_streams[fuse_path_str] = self.preopened_object_stream.pop(fuse_path_str)
            except KeyError:
                try:
                    # Attempt to retrieve everything in the pre-opening queue
                    while True:
                        opened_object_stream = self.preopened_object_stream_queue.get_nowait()
                        self.getattr_cache.set_ordered_read_pending_state_from_fuse_path_str(opened_object_stream.fuse_path_str, False)
                        if fuse_path_str == opened_object_stream.fuse_path_str:
                            self.opened_object_streams[opened_object_stream.fuse_path_str] = opened_object_stream
                            return 0   # Stream opened successfully
                        else:
                            self.preopened_object_stream[fuse_path_str] = opened_object_stream
                except queue.Empty:
                    # Object stream not yet pre-opened, inside the finally logic
                    self.getattr_cache.set_ordered_read_pending_state_from_fuse_path_str(fuse_path_str, False)
                    try:
                        new_object_stream = self.create_object_stream(fuse_path_str)
                    except Exception as e:
                        logger.error(e)
                        raise FuseOSError(errno.ENOENT)
                    self.opened_object_streams[fuse_path_str] = new_object_stream
                    logger.debug("Opened an object stream for " + new_object_stream.uri)
        return 0

    def read(self, fuse_path_str, length, offset, raw_fi):
        if fuse_path_str not in self.opened_object_streams:   # Not in ordered stream, create an unordered one
            try:
                new_object_stream = self.create_object_stream(fuse_path_str)
            except Exception as e:
                logger.error(e)
                raise FuseOSError(errno.ENOENT)
            self.opened_object_streams[fuse_path_str] = new_object_stream
            logger.debug("Opened an object s3 stream for " + new_object_stream.uri)
        return self.opened_object_streams[fuse_path_str].read(length, offset)

    def write(self, fuse_path_str, buf, offset, fh):
        raise FuseOSError(errno.EROFS)

    def release(self, fuse_path_str, raw_fi):
        object_stream = self.opened_object_streams.pop(fuse_path_str)
        object_stream.close()
        self.recycled_object_stream_queue.put(object_stream)
        logger.debug("Recycled object stream for " + fuse_path_str)


class CLPS3FuseSequentialRead(Operations):
    __slots__ = (
        's3_uri_prefix',
        'transport_params',
        'metadata_cache',
        'cache_loading_queue',
        'object_streams',
        'metadata_object_streams',
        'fake_file_size',
        'writable_file',
    )

    def __init__(self, s3_mount_config: S3MountConfig, metadata_cache: diskcache.Cache, cache_loading_queue: queue.Queue, fake_file_size: int, file_size):
        self.s3_uri_prefix = s3_mount_config.get_s3_path_prefix_as_uri()

        boto3_connection = s3_mount_config.create_new_connection()
        self.transport_params = {
            'resource': boto3_connection.resource,
            'buffer_size': S3_SEQUENTIAL_READ_STREAM_BUFFER_SIZE,
        }

        # Configure caches
        self.metadata_cache = metadata_cache
        self.cache_loading_queue = cache_loading_queue

        self.metadata_object_streams = {}
        self.object_streams = {}

        self.fake_file_size = fake_file_size
        self.writable_file = {}
        self.file_size = file_size
        logger.debug("CLPS3FuseSequentialRead mount started.")

    @staticmethod
    def is_valid_archive_name(archive_name: str):
        try:
            uuid.UUID(archive_name, version=4)
        except ValueError:
            return False

        return True

    @staticmethod
    def generate_dir_attribute(st_size: int = 512, st_atime: float = 0, st_ctime: float = 0, st_mtime: float = 0):
        return {
            'st_atime': st_atime,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
            'st_ctime': st_ctime,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
            # Platform dependent: time of most recent metadata change on Unix; time of creation on Windows
            'st_gid': os.getgid(),  # group id of owner
            'st_mode': 16676,  # protection bits dr--r--r--
            'st_mtime': st_mtime,  # Does not matter in the current S3 fuse implementation
            'st_nlink': 1,  # number of hard links
            'st_size': st_size,  # size of file, in bytes
            'st_uid': os.getuid(),  # user id of owner,
            'st_blocks': 1
        }

    @staticmethod
    def generate_file_attribute(st_size: int, st_atime: float = 0.0, st_ctime: float = 0, st_mtime: float = 0):
        return {
            'st_atime': st_atime,  # time of most recent access
            'st_ctime': st_ctime,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
            # Platform dependent: time of most recent metadata change on Unix; time of creation on Windows
            'st_gid': os.getgid(),  # group id of owner
            'st_mode': 33060,  # protection bits -r--r--r--
            'st_mtime': st_mtime,  # time of most recent content modification
            'st_nlink': 1,  # number of hard links
            'st_size': st_size,  # size of file
            'st_uid': os.getuid(),  # user id of owner
            'st_blocks': math.ceil(st_size / 512)  # hard code to 512 bytes block size
        }

    @staticmethod
    def is_segment_directory_name(directory_name: str):
        return directory_name == "s"

    @staticmethod
    def is_logs_directory_name(directory_name: str):  # on-disk file
        return directory_name == "l"

    @staticmethod
    def is_valid_uuid4(uuid4_str_candidate: str):
        try:
            uuid.UUID(uuid4_str_candidate, version=4)
            return True
        except ValueError:
            return False

    @staticmethod
    def get_name(fuse_path_str: str):
        return fuse_path_str[fuse_path_str.rindex("/") + 1:]

    # Filesystem methods
    # ==================
    def getattr(self, fuse_path_str, fh=None):
        logger.debug('getattr: path=' + fuse_path_str)
        # We assume all directory traversed by CLP will be valid
        if fuse_path_str == "/":
            return self.generate_dir_attribute()

        name = self.get_name(fuse_path_str)
        if self.is_valid_archive_name(name):
            # If we are getting the archive directory attributes, we can predict with high probability that metadata will be accessed very soon
            # This is the earliest point where we can check if metadata are cached, if not we'll fetch it asynchronously
            for metadata_filename in METADATA_FILENAMES:
                # Any metadata cache entry will trigger background cache refresh
                metadata_file_path = '/' + name + '/' + metadata_filename
                if metadata_file_path not in self.metadata_cache:
                    # Background thread will fetch the necessary metadata automatically
                    self.cache_loading_queue.put((name, self.s3_uri_prefix + '/' + name + "/merged_metadata"))
                    break

            return self.generate_dir_attribute()
        elif self.is_segment_directory_name(name) or self.is_logs_directory_name(name):
            return self.generate_dir_attribute()
        elif name in METADATA_GENERATED_FILES:
            if fuse_path_str in self.writable_file:
                on_disk_path = self.writable_file[fuse_path_str]['on_disk_path']
                true_size = os.path.getsize(on_disk_path)
                fake_attribute = self.generate_file_attribute(true_size)
                # allow write and read on the file
                fake_attribute['st_mode'] = 33206
                return fake_attribute
            raise FuseOSError(errno.ENOENT)

        else:
            file_attribute = self.generate_file_attribute(self.fake_file_size)
            if 'metadata.db' in fuse_path_str:
                total_sleep_time = 0
                while fuse_path_str not in self.file_size:
                    time.sleep(0.05)
                    total_sleep_time += 0.05
                    if total_sleep_time > 1 and total_sleep_time % 1 == 0:
                        logger.warning("Metadata.db not yet downloaded. Sleeping " + str(total_sleep_time) + " seconds.")
                metadata_db_size = self.file_size[fuse_path_str]
                logger.warning(f"Metadata.db has size {metadata_db_size}")
                file_attribute['st_size'] = metadata_db_size
                file_attribute['st_mode'] = 33206
            # We don't actually need to know the file size, so putting a number here
            return file_attribute

    # File methods
    # ============
    def open(self, fuse_path_str, raw_fi):
        logger.debug('open: path=' + fuse_path_str)

        # Metadata file is fetch asynchronously so we try to give it as much time as possible to fetch those data
        # Therefore, open os a no-op for metadata files
        filename = self.get_name(fuse_path_str)
        if filename in METADATA_GENERATED_FILES:
            raise FuseOSError(errno.ENOENT)

        if filename in METADATA_FILENAMES:
            raw_fi.keep_cache = METADATA_KERNEL_FILE_CACHE_FOR_READ  # keep every kernel memory page cache in kernel page buffer if possible
            return 0

        if fuse_path_str in self.object_streams:
            logger.error(fuse_path_str + " is already open and opening it multiple times is unsupported.")
            raise FuseOSError(errno.EBUSY)

        s3_uri = self.s3_uri_prefix + fuse_path_str
        try:
            logger.debug("Streaming object from s3 " + s3_uri)
            object_stream = SequentialAccessOptimizedObjectStream(smart_open.open(s3_uri, mode="rb", transport_params=self.transport_params))
            self.object_streams[fuse_path_str] = object_stream
            raw_fi.keep_cache = SEGMENT_KERNEL_FILE_CACHE_FOR_READ
        except OSError:
            logger.error("Object " + s3_uri + " not found in s3")
            raise FuseOSError(errno.ENOENT)

        return 0

    def create(self, fuse_path_str, mode, raw_fi=None):
        logger.debug('create: path=' + fuse_path_str)
        raise FuseOSError(errno.EROFS)   # Read-only

    def read(self, fuse_path_str, length, offset, raw_fi):
        logger.debug(f'read: path={fuse_path_str}, size={length}, offset={offset}')
        filename = self.get_name(fuse_path_str)
        if filename in METADATA_GENERATED_FILES:
            raise FuseOSError(errno.EROFS)
        if filename in METADATA_FILENAMES:
            if fuse_path_str not in self.metadata_object_streams:   # Not available as file handle yet
                total_sleep_time = 0
                while fuse_path_str not in self.metadata_cache:
                    time.sleep(0.05)
                    total_sleep_time += 0.05
                    if total_sleep_time > 1 and total_sleep_time % 1 == 0:
                        logger.warning("Metadata files not yet downloaded. Sleeping " + str(total_sleep_time) + " seconds.")
                # Get the read buffer from teh cache
                self.metadata_object_streams[fuse_path_str] = self.metadata_cache.get(fuse_path_str, read=True)
                logger.debug("Serving from metadata-cache " + self.s3_uri_prefix + fuse_path_str)
            self.metadata_object_streams[fuse_path_str].seek(offset)
            return self.metadata_object_streams[fuse_path_str].read(length)
        else:
            return self.object_streams[fuse_path_str].read(length, offset)

    def write(self, fuse_path_str, buf, offset, fh):
        logger.debug('create: path=' + fuse_path_str)
        raise FuseOSError(errno.EROFS)

    def truncate(self, fuse_path_str, length, raw_fi=None):
        logger.debug('Truncate: path=' + fuse_path_str)
        raise FuseOSError(errno.EROFS)   # RO fuse mount, no truncate needed

    def release(self, fuse_path_str, raw_fi):
        logger.debug('release: path=' + fuse_path_str)
        filename = self.get_name(fuse_path_str)
        if filename in METADATA_FILENAMES:
            object_stream = self.metadata_object_streams.pop(fuse_path_str, None)
            if object_stream:   # metadata may be opened, but not read, therefore noop
                object_stream.close()
        else:
            self.object_streams.pop(fuse_path_str).close()


class S3FuseIRSequentialWrite(Operations):
    __slots__ = (
        's3_uri_prefix',
        'transport_params',
        'object_streams',
        'created_directory_paths',
        'created_file_paths'
    )

    def __init__(self, s3_mount_config: S3MountConfig):
        self.s3_uri_prefix = s3_mount_config.get_s3_path_prefix_as_uri()

        boto3_connection = s3_mount_config.create_new_connection()
        self.transport_params = {
            "resource": boto3_connection.resource,
            "buffer_size": S3_SEQUENTIAL_WRITE_STREAM_BUFFER_SIZE,
        }

        # Open streams
        self.object_streams = {}

        self.created_directory_paths = set()
        self.created_file_paths = set()

        logger.debug("FUSE file system for Archive to IR initialized successfully")

    @staticmethod
    def generate_file_attr():
        return {
            # See stat documentation for the meaning of these parameters
            # NOTE: Except for the permissions, no other parameters below are
            # used
            'st_atime': 0.0,
            'st_ctime': 0.0,
            'st_gid': os.getgid(),
            'st_mode': 33060,  # permissions: -r--r--r--
            'st_mtime': 0.0,
            'st_nlink': 1,
            'st_size': 512,
            'st_uid': os.getuid(),
            'st_blocks': 1,
        }

    @staticmethod
    def generate_dir_attr():
        return {
            # See stat documentation for the meaning of these parameters
            # NOTE: Except for the permissions, no other parameters below are
            # used
            'st_atime': 0.0,
            'st_ctime': 0.0,
            'st_gid': os.getgid(),
            'st_mode': 16676,  # permissions: dr-xr-xr-x
            'st_mtime': 0.0,
            'st_nlink': 1,
            'st_size': 512,
            'st_uid': os.getuid(),
            'st_blocks': 1,
        }

    # Filesystem methods
    # ==================
    def getattr(self, fuse_path_str, fh=None):
        logger.debug(f"getattr: path={fuse_path_str}")
        # We use a heuristic to determine whether a path is a  file or directory
        if fuse_path_str == "/" or fuse_path_str in self.created_directory_paths:
            return self.generate_dir_attr()
        elif fuse_path_str in self.created_file_paths:
            return self.generate_file_attr()
        raise FuseOSError(errno.ENOENT)

    def mkdir(self, fuse_path_str, mode):
        logger.debug(f"mkdir: path={fuse_path_str}")
        self.created_directory_paths.add(fuse_path_str)

    # File methods
    # ============
    def open(self, fuse_path_str, raw_fi):
        logger.debug(f"open: path={fuse_path_str}")
        # We don't support opening existing files for writing, only creating
        # new files
        raise FuseOSError(errno.EACCES)

    def create(self, fuse_path_str, mode, raw_fi=None):
        logger.debug(f"create: path={fuse_path_str}")
        self.created_file_paths.add(fuse_path_str)

        raw_fi.keep_cache = SEGMENT_USE_KERNEL_PAGE_CACHE_FOR_WRITE  # Enable kernel file cache
        self.object_streams[fuse_path_str] = SequentialAccessOptimizedObjectStream(
            smart_open.open(self.s3_uri_prefix + fuse_path_str, mode='wb',
                            transport_params=self.transport_params))

        return 0

    def write(self, fuse_path_str, buf, offset, fh):
        logger.debug(f"write: path={fuse_path_str}, length: {len(buf)} bytes, offset: {offset}")

        # We don't support opening existing files for writing, only creating new
        # files
        if fuse_path_str not in self.object_streams:
            logger.debug(f"Trying to write to file that is not created: {fuse_path_str}")
            raise FuseOSError(errno.EACCES)

        return self.object_streams[fuse_path_str].write(buf, offset)

    def release(self, fuse_path_str, raw_fi):
        logger.debug(f"release: path={fuse_path_str}")

        self.object_streams.pop(fuse_path_str).close()   # Auto cleanup
        logger.debug(f"Streamed file to s3: {fuse_path_str}")

class S3FuseMostlySequentialWrite(Operations):
    __slots__ = (
        's3_uri_prefix',
        'transport_params',
        'mount_cache_path',
        'object_streams',
        'archive_metadata_containers',
        'created_directory_paths',
        'created_file_paths'
    )

    def __init__(self, s3_mount_config: S3MountConfig, mount_cache_path: pathlib.Path):
        self.s3_uri_prefix = s3_mount_config.get_s3_path_prefix_as_uri()

        boto3_connection = s3_mount_config.create_new_connection()
        self.transport_params = {
            "resource": boto3_connection.resource,
            "buffer_size": S3_SEQUENTIAL_WRITE_STREAM_BUFFER_SIZE,
        }

        self.mount_cache_path = mount_cache_path

        # Open streams
        self.object_streams = {}
        self.archive_metadata_containers = {}

        self.created_directory_paths = set()
        self.created_file_paths = set()

        logger.debug("FUSE file system for clp compression initialized successfully")

    @staticmethod
    def generate_file_attr(st_size: int = 0):
        return {
            'st_atime': 0.0,  # time of most recent access
            'st_ctime': 0.0,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
            # Platform dependent: time of most recent metadata change on Unix; time of creation on Windows
            'st_gid': os.getgid(),  # group id of owner
            'st_mode': 33060,  # protection bits -r--r--r--
            'st_mtime': 0.0,  # time of most recent content modification
            'st_nlink': 1,  # number of hard links
            'st_size': st_size,  # size of file
            'st_uid': os.getuid(),  # user id of owner
            'st_blocks': math.ceil(st_size / 512)  # hard code to 512 bytes block size
        }

    @staticmethod
    def generate_dir_attr(st_size: int = 512):
        return {
            'st_atime': 0.0,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
            'st_ctime': 0.0,  # Does not matter in the current S3 fuse implementation, hard-coded to beginning of unix time
            # Platform dependent: time of most recent metadata change on Unix; time of creation on Windows
            'st_gid': os.getgid(),  # group id of owner
            'st_mode': 16676,  # protection bits dr--r--r--
            'st_mtime': 0.0,  # Does not matter in the current S3 fuse implementation
            'st_nlink': 1,  # number of hard links
            'st_size': st_size,  # size of file, in bytes
            'st_uid': os.getuid(),  # user id of owner,
            'st_blocks': math.ceil(st_size / 512)
        }

    @staticmethod
    def get_name(fuse_path_str: str):
        return fuse_path_str[fuse_path_str.rindex("/") + 1:]

    @staticmethod
    def get_archive_name(fuse_path_str: str):
        return fuse_path_str[1:fuse_path_str.index("/", 1)]

    # Filesystem methods
    # ==================
    def getattr(self, fuse_path_str, fh=None):
        logger.debug("getattr: path=" + fuse_path_str)
        # We'll be generating the file and directory attributes since it's not necessary for clp compression.
        # As long as type (directory|file) is correct, it meets the clp compression requirement
        # We use a hard-code heuristic to determine whether a file path is a file or directory
        if fuse_path_str == "/" or fuse_path_str in self.created_directory_paths:
            return self.generate_dir_attr()
        elif fuse_path_str in self.created_file_paths:
            fake_file_attribute = self.generate_file_attr()
            filename = self.get_name(fuse_path_str)
            if filename in METADATA_GENERATED_FILES or filename == 'metadata.db':
                archive_name = self.get_archive_name(fuse_path_str)
                index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[filename]
                file_size = self.archive_metadata_containers[archive_name].metadata_files[index].get_size()
                fake_file_attribute['st_size'] = file_size
                logger.debug(f"get attri return actual file size {fake_file_attribute['st_size']}")
            return fake_file_attribute
        raise FuseOSError(errno.ENOENT)

    def mkdir(self, fuse_path_str, mode):
        logger.debug("mkdir: path=" + fuse_path_str)
        self.created_directory_paths.add(fuse_path_str)

    # File methods
    # ============
    def open(self, fuse_path_str, raw_fi):
        logger.debug('open: path=' + fuse_path_str)
        filename = self.get_name(fuse_path_str)
        if filename in METADATA_GENERATED_FILES:
            archive_name = self.get_archive_name(fuse_path_str)
            index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[filename]
            self.archive_metadata_containers[archive_name].metadata_files[index].reopen_for_writing()
            return 0
            
        # We don't support opening existing files for writing, only creating new files
        raise FuseOSError(errno.EACCES)

    def chmod(self, fuse_path_str, mode):
        logger.debug('chmod: path=' + fuse_path_str)
        logger.debug('chmod: mod=' + str(mode))

    def create(self, fuse_path_str, mode, raw_fi=None):
        logger.debug('create: path=' + fuse_path_str)
        self.created_file_paths.add(fuse_path_str)

        # Only creation of files goes to this path, so we need to determine whether it is a metadata file
        name = self.get_name(fuse_path_str)
        #if name in METADATA_FILENAMES or name in METADATA_GENERATED_FILES:
        if name in METADATA_FILENAMES or name in METADATA_GENERATED_FILES:
            raw_fi.keep_cache = METADATA_USE_KERNEL_PAGE_CACHE_FOR_WRITE  # Enable kernel file cache
            archive_name = self.get_archive_name(fuse_path_str)
            if archive_name not in self.archive_metadata_containers:
                self.archive_metadata_containers[archive_name] = ArchiveMetadataContainer(self.mount_cache_path, archive_name)
            index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[name]
            logger.debug(f'create: path with metadata file=' + fuse_path_str)
            self.archive_metadata_containers[archive_name].metadata_files[index].create_and_open()
        else:
            logger.debug('create: path with object stream=' + fuse_path_str)
            # Segment files are usually large and is written to its own file
            raw_fi.keep_cache = SEGMENT_USE_KERNEL_PAGE_CACHE_FOR_WRITE  # Enable kernel file cache
            self.object_streams[fuse_path_str] = \
                SequentialAccessOptimizedObjectStream(smart_open.open(self.s3_uri_prefix + fuse_path_str, mode='wb', transport_params=self.transport_params))
        return 0

    def write(self, fuse_path_str, buf, offset, fh):
        logger.debug('write: path=' + fuse_path_str + " length: " + str(len(buf)) + " bytes, offset: " + str(offset))

        name = self.get_name(fuse_path_str)
        archive_name = self.get_archive_name(fuse_path_str)
        if name in METADATA_FILENAMES or name in METADATA_GENERATED_FILES:
            index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[name]
            return self.archive_metadata_containers[archive_name].metadata_files[index].write(buf, offset)

        return self.object_streams[fuse_path_str].write(buf, offset)

    def read(self, fuse_path_str, length, offset, raw_fi):
        logger.debug('read: path=' + fuse_path_str)
        filename = self.get_name(fuse_path_str)
        logger.debug('read: filename=' + filename)
        if 'metadata.db' == filename or filename in METADATA_GENERATED_FILES:
            archive_name = self.get_archive_name(fuse_path_str)
            logger.debug('read: archive=' + archive_name)
            if archive_name in self.archive_metadata_containers:
                index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[filename]
                return self.archive_metadata_containers[archive_name].metadata_files[index].read(offset, length)
            logger.debug('Archive metadata not created yet')
        raise FuseOSError(errno.EACCES)

    # def getxattr(self, path, name, position=0):
    #     logger.debug('getxattr: path=' + path)
    #     logger.debug('getxattr: name=' + name)
    #     return []

    def unlink(self, fuse_path_str):
        filename = self.get_name(fuse_path_str)
        logger.debug('unlink: path=' + fuse_path_str)
        if filename in METADATA_GENERATED_FILES:
            archive_name = self.get_archive_name(fuse_path_str)
            if archive_name in self.archive_metadata_containers:
                index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[filename]
                self.archive_metadata_containers[archive_name].metadata_files[index].unlink()
                return 
            logger.debug(f"unlink: invalid {archive_name}")
        logger.debug(f"unlink not supported on {fuse_path_str}")
        raise FuseOSError(errno.EROFS)

    def release(self, fuse_path_str, raw_fi):
        logger.info('release: path=' + fuse_path_str)

        name = self.get_name(fuse_path_str)
        if name in METADATA_FILENAMES or name in METADATA_GENERATED_FILES:
            archive_name = self.get_archive_name(fuse_path_str)
            index = self.archive_metadata_containers[archive_name].filename_to_index_mapping[name]
            self.archive_metadata_containers[archive_name].metadata_files[index].close()

            if self.archive_metadata_containers[archive_name].all_files_done_writing():
                metadata_container = self.archive_metadata_containers.pop(archive_name)  # Auto cleanup
                merged_metadata_file_path = "/".join([self.s3_uri_prefix, archive_name, "merged_metadata"])
                with smart_open.open(merged_metadata_file_path, mode="wb", transport_params=self.transport_params) as object_stream:
                    metadata_container.read_into(object_stream)  # Auto cleanup
                logger.debug("Streamed metadata to s3 " + merged_metadata_file_path)

            return

        self.object_streams.pop(fuse_path_str).close()   # Auto cleanup
        logger.debug("Streamed segment to s3 " + fuse_path_str)


def paginate_s3_key_and_st_size(s3_client: boto3.client, bucket_name: str, object_key_prefix: str):
    paginator = s3_client.get_paginator('list_objects_v2')
    for response in paginator.paginate(Bucket=bucket_name, Prefix=object_key_prefix):
        if 0 == response["KeyCount"]:
            return

        for s3_file_metadata in response['Contents']:
            yield s3_file_metadata['Key'], s3_file_metadata['Size']


def generate_caches(s3_mount_config: S3MountConfig, using_ordered_reads: bool):
    readdir_cache = ReadDirCache()
    getattr_cache = GetAttrCache()

    boto3_connection = s3_mount_config.create_new_connection()
    for key, st_size in paginate_s3_key_and_st_size(boto3_connection.client,
                                                    bucket_name=s3_mount_config.s3_bucket,
                                                    object_key_prefix=s3_mount_config.s3_object_key_prefix):
        equivalent_fuse_path = s3_mount_config.get_fuse_path(key)
        readdir_cache.add_file_with_fuse_path_str(fuse_path_str=equivalent_fuse_path)
        getattr_cache.add_with_fuse_path_str(fuse_path_str=equivalent_fuse_path, st_size=st_size, ordered_read_pending=using_ordered_reads)

    return getattr_cache, readdir_cache


def generate_caches_from_existing_metadata(file_paths, st_sizes, using_ordered_reads: bool):
    readdir_cache = ReadDirCache()
    getattr_cache = GetAttrCache()

    for file_path, st_size in zip(file_paths, st_sizes):
        equivalent_fuse_path = str(file_path)
        readdir_cache.add_file_with_fuse_path_str(fuse_path_str=equivalent_fuse_path)
        getattr_cache.add_with_fuse_path_str(fuse_path_str=equivalent_fuse_path, st_size=st_size, ordered_read_pending=using_ordered_reads)
    return getattr_cache, readdir_cache


def preopener_thread_method(preopened_object_stream_queue: queue.Queue, recycled_object_stream_queue: queue.Queue, files, mount_point_uri: str):
    logger.debug("Object stream pre-openener thread started")

    # Pre-open an ordered list of objects into object streams
    for fuse_path_str, metadata in files.items():
        # Pre-open object stream and put it in the preopened queue
        if metadata.ordered_read_pending:
            object_stream: ManagedSequentialReadOptimizedStream = recycled_object_stream_queue.get()
            object_stream.open_with_fuse_path_str(fuse_path_str)
            preopened_object_stream_queue.put(object_stream)
            logger.debug("Pre-opened object stream for " + object_stream.uri)

    logger.debug("Object stream pre-opening completed")

    # Finished pre-reading files
    while True:
        # Retrieve but don't re-use it; let GC take care of deleting
        recycled_object_stream_queue.get()


def metadata_downloader(exit_request_event: threading.Event, s3_mount_config: S3MountConfig, metadata_cache: diskcache.Cache,
                        cache_loading_queue: queue.Queue, file_size):
    logger.debug("S3 metadata downloader thread started")

    # Background downloader needs an independent boto3 session
    boto3_connection = s3_mount_config.create_new_connection()
    transport_params = {
        "resource": boto3_connection.resource,
        "buffer_size": S3_SEQUENTIAL_READ_STREAM_BUFFER_SIZE
    }

    logger.debug("S3 object cache population started")
    while not exit_request_event.is_set():
        # Get S3 files which may or may not need loading
        archive_name, s3_path_uri = cache_loading_queue.get()
        if archive_name is None:
            # Woken up by another thread
            continue
        logger.debug("Asynchronously fetching metadata file for archive " + archive_name)

        # Stream metadata file from S3 and decompose into parts along the way, and insert it into the cache
        with smart_open.open(s3_path_uri, mode='rb', transport_params=transport_params) as s3_file:
            # Breakup the combined metadata file into its components
            num_offsets = len(METADATA_FILENAMES) - 1
            file_end_offsets = [int.from_bytes(s3_file.read(4), byteorder='little') for _ in range(num_offsets)]
            prev_offset = num_offsets * 4
            for i in range(0, num_offsets):
                name = METADATA_FILENAMES[i]
                file_end_offset = file_end_offsets[i]

                fuse_path_str = '/' + archive_name + '/' + name
                file_size[fuse_path_str] = file_end_offset - prev_offset
                in_memory_file = io.BytesIO(s3_file.read(file_end_offset - prev_offset))
                metadata_cache.add(fuse_path_str, in_memory_file, read=True)
                prev_offset = file_end_offset
                logger.debug("Added " + name + " file for archive " + archive_name + " to the metadata cache")
            name = METADATA_FILENAMES[-1]
            fuse_path_str = '/' + archive_name + '/' + name
            metadata_cache.add(fuse_path_str, s3_file, read=True)
            logger.debug("Added " + name + " file for archive " + archive_name + " to the metadata cache")


def s3_fuse_sequential_read_thread_method(s3_mount_config: S3MountConfig, readdir_cache: ReadDirCache, getattr_cache: GetAttrCache,
                                          preopened_object_stream_queue: queue.Queue, recycled_object_stream_queue: queue.Queue, mount_path: pathlib.Path):
    # NOTE: nothreads must be true for this to work
    FUSE(S3FuseSequentialRead(s3_mount_config, readdir_cache, getattr_cache, preopened_object_stream_queue, recycled_object_stream_queue, True),
         mountpoint=str(mount_path.resolve()), raw_fi=True, foreground=True, allow_other=True, nothreads=True)


def s3_fuse_mostly_sequential_write_thread_method(s3_mount_config: S3MountConfig, mount_path: pathlib.Path, mount_cache_path: pathlib.Path):
    # NOTE: nothreads must be true for this to work
    FUSE(S3FuseMostlySequentialWrite(s3_mount_config, mount_cache_path),
         mountpoint=str(mount_path.resolve()), raw_fi=True, foreground=True, allow_other=True, nothreads=True)


def s3_fuse_ir_write_thread_method(s3_mount_config: S3MountConfig, mount_path: pathlib.Path):
    # NOTE: nothreads must be true for this to work
    FUSE(S3FuseIRSequentialWrite(s3_mount_config),
         mountpoint=str(mount_path.resolve()), raw_fi=True, foreground=True, allow_other=True, nothreads=True)


def clp_s3_fuse_sequential_read_thread_method(s3_mount_config: S3MountConfig, mount_dir: pathlib.Path, mount_cache_dir: pathlib.Path, max_file_size: int):
    # Create directories
    mount_dir.mkdir(parents=True, exist_ok=True)
    if mount_cache_dir.exists():
        shutil.rmtree(mount_cache_dir)
    mount_cache_dir.mkdir(parents=True, exist_ok=True)

    # Create metadata cache and start thread to download it upon request
    exit_request_event = threading.Event()
    on_disk_metadata_cache = diskcache.Cache(directory=str(mount_cache_dir), size_limit=1 * 1024 * 1024 * 1024,
                                             eviction_policy=DiskCacheEvictionPolicies.LRU, enable=False)
    file_size = {}
    cache_requests_queue = queue.Queue(maxsize=500)
    metadata_downloader_thread = threading.Thread(target=metadata_downloader,
                                                  args=(exit_request_event, s3_mount_config, on_disk_metadata_cache, cache_requests_queue, file_size))
    metadata_downloader_thread.start()

    # NOTE: nothreads must be true for this to work
    FUSE(CLPS3FuseSequentialRead(s3_mount_config, on_disk_metadata_cache, cache_requests_queue, max_file_size, file_size), mountpoint=str(mount_dir), raw_fi=True,
         foreground=True, allow_other=True, nothreads=True)

    # Ask metadata_downloader to exit and wait for it
    exit_request_event.set()
    # Put something on the cache queue to unblock the thread if it's waiting
    cache_requests_queue.put((None, None))
    metadata_downloader_thread.join()

    # Remove directories
    shutil.rmtree(mount_dir)
    shutil.rmtree(mount_cache_dir)
