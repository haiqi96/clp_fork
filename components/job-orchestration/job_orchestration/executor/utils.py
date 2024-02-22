import errno
import os
import subprocess
import time
from pathlib import Path
from shutil import rmtree
from threading import Thread
from typing import Any, Dict, Optional, Tuple

from celery.utils.log import get_task_logger
from job_orchestration.executor.credentials import HARDCODED_S3_CONFIG, USE_FUSE_LAYER
from remote_fuse_layer.s3 import (
    clp_s3_fuse_sequential_read_thread_method,
    s3_fuse_mostly_sequential_write_thread_method,
    S3MountConfig,
)

# Setup logging
logger = get_task_logger(__name__)


def _get_mount_cache_path(mount_path: Path) -> Path:
    return mount_path.parent / ("." + mount_path.name)


def _remove_and_mkdir(dir_path: Path, is_fuse_mount: bool = False) -> None:
    try:
        if dir_path.exists():
            rmtree(dir_path)
    except OSError as e:
        if is_fuse_mount and e.errno == errno.ENOTCONN:
            # ENOTCONN corresponds to
            # "transport endpoint is not connected: <mount_point>"
            subprocess.run(["fusermount", "-u", str(dir_path)])
            if dir_path.exists():
                rmtree(dir_path)
        else:
            raise e

    dir_path.mkdir(parents=True)


def _get_s3_mount_config(s3_yaml_config: Dict[str, Any]) -> S3MountConfig:
    return S3MountConfig(
        access_key_id=s3_yaml_config["access_key_id"],
        secret_access_key=s3_yaml_config["secret_access_key"],
        s3_path_prefix_str=s3_yaml_config["s3_path_prefix"],
        endpoint_url=s3_yaml_config["endpoint_url"],
        s3_path_prefix_to_remove_from_mount=None,
    )


def _await_fuse_mount(mount_path: Path) -> None:
    logger.info(f"Waiting for {mount_path} to mount...")
    while not os.path.ismount(mount_path):
        time.sleep(0.01)
    logger.info(f"{mount_path} mounted.")


def _unmount_fuse(mount_path: Path, fuse_thread: Thread) -> None:
    logger.info(f"Unmounting {mount_path}...")
    subprocess.run(
        ["fusermount", "-u", str(mount_path)],
        close_fds=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )

    logger.info("Waiting for the FUSE thread to exit...")
    fuse_thread.join()
    logger.info("FUSE thread exited.")

    rmtree(mount_path)
    rmtree(_get_mount_cache_path(mount_path))


def _create_reader_fuse_thread(
    s3_reader_config: Dict[str, Any], mount_path: Path, max_file_size: int
) -> Thread:
    # Load reader S3 config
    _remove_and_mkdir(mount_path, is_fuse_mount=True)

    # Create a real on disk cache
    reader_mount_cache_path = _get_mount_cache_path(mount_path)
    _remove_and_mkdir(reader_mount_cache_path)

    s3_reader_mount_config = _get_s3_mount_config(s3_reader_config)

    # Create the thread as a daemon so that it exits immediately when the main
    # thread is cancelled
    reader_fuse_thread = Thread(
        target=clp_s3_fuse_sequential_read_thread_method,
        args=(
            s3_reader_mount_config,
            mount_path,
            reader_mount_cache_path,
            max_file_size,
        ),
        daemon=True,
    )

    return reader_fuse_thread


def _create_writer_fuse_thread(s3_writer_config: Dict[str, Any], mount_path: Path) -> Thread:
    # Create mount path
    _remove_and_mkdir(mount_path, is_fuse_mount=True)

    # Create mount cache
    mount_cache_path = _get_mount_cache_path(mount_path)
    _remove_and_mkdir(mount_cache_path, is_fuse_mount=False)

    s3_writer_mount_config = _get_s3_mount_config(s3_writer_config)

    # create the thread as daemon so they exit immediately
    # when the main thread is cancelled
    # TODO: verify if cancel can be run properly?
    writer_fuse_thread = Thread(
        target=s3_fuse_mostly_sequential_write_thread_method,
        args=(s3_writer_mount_config, mount_path, mount_cache_path),
        daemon=True,
    )

    return writer_fuse_thread


def setup_reader_archive_directory(
    job_id: str,
    task_id: str,
) -> Tuple[Path, Optional[Dict[str, Any]]]:
    # for the normal file system flow
    if not USE_FUSE_LAYER:
        return Path(os.getenv("CLP_ARCHIVE_OUTPUT_DIR")), None

    # for the S3 Flow
    s3_config = HARDCODED_S3_CONFIG
    # 4GB should be a very safe guess.
    max_file_size = 4 * 1024 * 1024 * 1024

    # A temp hack for access key id and secret
    os.environ["AWS_ACCESS_KEY_ID"] = s3_config["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = s3_config["secret_access_key"]

    # generate reader_mount path
    mount_path = Path("/") / f"reader_mount_{job_id}_{task_id}"

    # Launch the mount thread
    fuse_thread = _create_reader_fuse_thread(s3_config, mount_path, max_file_size)
    fuse_thread.start()
    logger.info("Creating FUSE mount...")
    _await_fuse_mount(mount_path)

    # Record the thread for later usage
    setup_info = {"fuse_thread": fuse_thread}
    # point the storage dir to mounted path
    return mount_path, setup_info


def setup_writer_archive_directory(
    job_id: str,
    task_id: str,
) -> Tuple[Path, Optional[Dict[str, Any]]]:

    # for the normal file system flow
    if not USE_FUSE_LAYER:
        return Path(os.getenv("CLP_ARCHIVE_OUTPUT_DIR")), None

    # for the S3 Flow
    s3_config = HARDCODED_S3_CONFIG

    # A temp hack for access key id and secret
    os.environ["AWS_ACCESS_KEY_ID"] = s3_config["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = s3_config["secret_access_key"]

    # generate reader_mount path
    mount_path = Path("/") / f"writer_mount_{job_id}_{task_id}"

    # Launch the mount thread
    fuse_thread = _create_writer_fuse_thread(s3_config, mount_path)
    fuse_thread.start()
    logger.info("Creating FUSE mount...")
    _await_fuse_mount(mount_path)

    # Record the thread for later usage
    setup_info = {"fuse_thread": fuse_thread}

    # point the storage dir to mounted path
    return mount_path, setup_info


def post_cleanup(archive_dir: Path, setup_config: Optional[Dict[str, Any]]) -> None:
    # Normal flow, do nothing
    if not USE_FUSE_LAYER:
        return

    # For the S3 Flow
    _unmount_fuse(archive_dir, setup_config["fuse_thread"])
