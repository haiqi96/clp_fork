import datetime
import errno
import json
import os
import time
from pathlib import Path
import shutil
import subprocess
from typing import Dict, Any, Tuple

from threading import Thread

import yaml
from celery.app.task import Task
from celery.utils.log import get_task_logger

from clp_py_utils.clp_config import StorageEngine
from job_orchestration.executor.compress.celery import app
from job_orchestration.executor.compress.utils import make_clp_command, make_clp_s_command
from job_orchestration.scheduler.constants import CompressionTaskStatus
from job_orchestration.scheduler.job_config import ClpIoConfig, PathsToCompress
from job_orchestration.scheduler.scheduler_data import (
    CompressionTaskFailureResult,
    CompressionTaskSuccessResult,
)

from job_orchestration.executor.compress.credentials import (
    HARDCODED_S3_CONFIG
)

from remote_fuse_layer.s3 import (
    S3MountConfig,
    s3_fuse_mostly_sequential_write_thread_method
)
# Setup logging
logger = get_task_logger(__name__)


def _get_mount_cache_path(mount_path: Path) -> Path:
    return mount_path.parent / ("." + mount_path.name)


def _remove_and_mkdir(dir_path: Path, is_fuse_mount: bool = False) -> None:
    try:
        if dir_path.exists():
            shutil.rmtree(dir_path)
    except OSError as e:
        if is_fuse_mount and e.errno == errno.ENOTCONN:
            # ENOTCONN corresponds to
            # "transport endpoint is not connected: <mount_point>"
            subprocess.run(["fusermount", "-u", str(dir_path)])
            if dir_path.exists():
                shutil.rmtree(dir_path)
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
    shutil.rmtree(mount_path)
    shutil.rmtree(_get_mount_cache_path(mount_path))


def _create_writer_fuse_thread(
    s3_writer_config: Dict[str, Any], mount_path: Path
) -> Thread:

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


def run_clp(
    clp_config: ClpIoConfig,
    clp_home: Path,
    data_dir: Path,
    archive_output_dir: Path,
    logs_dir: Path,
    job_id: int,
    task_id: int,
    paths_to_compress: PathsToCompress,
    clp_metadata_db_connection_config,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Compresses files from an FS into archives on an FS

    :param clp_config: ClpIoConfig
    :param clp_home:
    :param data_dir:
    :param archive_output_dir:
    :param logs_dir:
    :param job_id:
    :param task_id:
    :param paths_to_compress: PathToCompress
    :param clp_metadata_db_connection_config
    :return: tuple -- (whether compression was successful, output messages)
    """
    clp_storage_engine = str(os.getenv("CLP_STORAGE_ENGINE"))

    instance_id_str = f"compression-job-{job_id}-task-{task_id}"

    # Generate database config file for clp
    db_config_file_path = data_dir / f"{instance_id_str}-db-config.yml"
    db_config_file = open(db_config_file_path, "w")
    yaml.safe_dump(clp_metadata_db_connection_config, db_config_file)
    db_config_file.close()

    if StorageEngine.CLP == clp_storage_engine:
        compression_cmd = make_clp_command(
            clp_home=clp_home,
            archive_output_dir=archive_output_dir,
            clp_config=clp_config,
            db_config_file_path=db_config_file_path,
        )
    elif StorageEngine.CLP_S == clp_storage_engine:
        compression_cmd = make_clp_s_command(
            clp_home=clp_home,
            archive_output_dir=archive_output_dir,
            clp_config=clp_config,
            db_config_file_path=db_config_file_path,
        )
    else:
        logger.error(f"Unsupported storage engine {clp_storage_engine}")
        return False, {"error_message": f"Unsupported storage engine {clp_storage_engine}"}

    # Prepare list of paths to compress for clp
    file_paths = paths_to_compress.file_paths
    log_list_path = data_dir / f"{instance_id_str}-log-paths.txt"
    with open(log_list_path, "w") as file:
        if len(file_paths) > 0:
            for path_str in file_paths:
                file.write(path_str)
                file.write("\n")
        if paths_to_compress.empty_directories and len(paths_to_compress.empty_directories) > 0:
            # Prepare list of paths to compress for clp
            for path_str in paths_to_compress.empty_directories:
                file.write(path_str)
                file.write("\n")

        compression_cmd.append("--files-from")
        compression_cmd.append(str(log_list_path))

    # Open stderr log file
    stderr_log_path = logs_dir / f"{instance_id_str}-stderr.log"
    stderr_log_file = open(stderr_log_path, "w")

    # Start compression
    logger.info("Compressing...")
    compression_successful = False
    proc = subprocess.Popen(compression_cmd, stdout=subprocess.PIPE, stderr=stderr_log_file)

    # Compute the total amount of data compressed
    last_archive_stats = None
    total_uncompressed_size = 0
    total_compressed_size = 0
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        stats = json.loads(line.decode("ascii"))
        if last_archive_stats is not None and stats["id"] != last_archive_stats["id"]:
            # We've started a new archive so add the previous archive's last
            # reported size to the total
            total_uncompressed_size += last_archive_stats["uncompressed_size"]
            total_compressed_size += last_archive_stats["size"]
        last_archive_stats = stats
    if last_archive_stats is not None:
        # Add the last archive's last reported size
        total_uncompressed_size += last_archive_stats["uncompressed_size"]
        total_compressed_size += last_archive_stats["size"]

    # Wait for compression to finish
    return_code = proc.wait()
    if 0 != return_code:
        logger.error(f"Failed to compress, return_code={str(return_code)}")
    else:
        compression_successful = True

        # Remove generated temporary files
        if log_list_path:
            log_list_path.unlink()
        db_config_file_path.unlink()
    logger.info("Compressed.")

    # Close stderr log file
    stderr_log_file.close()

    if compression_successful:
        return compression_successful, {
            "total_uncompressed_size": total_uncompressed_size,
            "total_compressed_size": total_compressed_size,
        }
    else:
        return compression_successful, {"error_message": f"See logs {stderr_log_path}"}

@app.task(bind=True)
def compress(
    self: Task,
    job_id: int,
    task_id: int,
    clp_io_config_json: str,
    paths_to_compress_json: str,
    clp_metadata_db_connection_config,
):
    clp_home_str = os.getenv("CLP_HOME")
    data_dir_str = os.getenv("CLP_DATA_DIR")
    archive_output_dir_str = os.getenv("CLP_ARCHIVE_OUTPUT_DIR")
    logs_dir_str = os.getenv("CLP_LOGS_DIR")

    clp_io_config = ClpIoConfig.parse_raw(clp_io_config_json)
    paths_to_compress = PathsToCompress.parse_raw(paths_to_compress_json)

    # Launch the writer mount thread
    os.environ["AWS_ACCESS_KEY_ID"] = HARDCODED_S3_CONFIG["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = HARDCODED_S3_CONFIG["secret_access_key"]

    mount_path = Path("/") / f"writer_mount_{job_id}_{task_id}"
    writer_fuse_thread = _create_writer_fuse_thread(HARDCODED_S3_CONFIG, mount_path)
    writer_fuse_thread.start()
    _await_fuse_mount(mount_path)

    start_time = datetime.datetime.now()
    logger.info(f"[job_id={job_id} task_id={task_id}] COMPRESSION STARTED.")
    try:
        compression_successful, worker_output = run_clp(
            clp_io_config,
            Path(clp_home_str),
            Path(data_dir_str),
            Path(mount_path),
            Path(logs_dir_str),
            job_id,
            task_id,
            paths_to_compress,
            clp_metadata_db_connection_config,
        )
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(f"[job_id={job_id} task_id={task_id}] COMPRESSION COMPLETED.")
    except Exception as err:
        compression_successful = False
        worker_output = {"error_message": str(err)}
        logger.exception(f"[job_id={job_id} task_id={task_id}] COMPRESSION FAILED on exception.")

    _unmount_fuse(mount_path, writer_fuse_thread)

    if compression_successful:
        return CompressionTaskSuccessResult(
            task_id=task_id,
            status=CompressionTaskStatus.SUCCEEDED,
            start_time=start_time,
            duration=duration,
            total_uncompressed_size=worker_output["total_uncompressed_size"],
            total_compressed_size=worker_output["total_compressed_size"],
        ).dict()
    else:
        return CompressionTaskFailureResult(
            task_id=task_id,
            status=CompressionTaskStatus.FAILED,
            start_time=start_time,
            duration=duration,
            error_message=worker_output["error_message"],
        ).dict()
