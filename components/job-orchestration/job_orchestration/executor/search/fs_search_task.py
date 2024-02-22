import errno
import os
import threading
from shutil import rmtree
import signal
import subprocess
import sys
import time
from pathlib import Path
from threading import Thread
from typing import Any, Dict

from celery.app.task import Task
from celery.utils.log import get_task_logger
from clp_py_utils.clp_config import StorageEngine
from clp_py_utils.clp_logging import set_logging_level
from job_orchestration.executor.search.celery import app
from job_orchestration.scheduler.job_config import SearchConfig
from job_orchestration.scheduler.scheduler_data import SearchTaskResult
from remote_fuse_layer.s3 import (
    S3MountConfig,
    clp_s3_fuse_sequential_read_thread_method
)
from job_orchestration.executor.credentials import HARDCODED_S3_CONFIG

# Setup logging
logger = get_task_logger(__name__)


def make_clo_command(
    clp_home: Path,
    archive_path: Path,
    search_config: SearchConfig,
    results_cache_uri: str,
    results_collection: str,
):
    # fmt: off
    search_cmd = [
        str(clp_home / "bin" / "clo"),
        results_cache_uri,
        results_collection,
        str(archive_path),
        search_config.query_string,
    ]
    # fmt: on

    if search_config.begin_timestamp is not None:
        search_cmd.append("--tge")
        search_cmd.append(str(search_config.begin_timestamp))
    if search_config.end_timestamp is not None:
        search_cmd.append("--tle")
        search_cmd.append(str(search_config.end_timestamp))
    if search_config.ignore_case:
        search_cmd.append("--ignore-case")
    if search_config.path_filter is not None:
        search_cmd.append(search_config.path_filter)

    return search_cmd


def make_clp_s_command(
    clp_home: Path,
    archives_dir: Path,
    archive_id: str,
    search_config: SearchConfig,
    results_cache_uri: str,
    results_collection: str,
):
    # fmt: off
    search_cmd = [
        str(clp_home / "bin" / "clp-s"),
        "s",
        str(archives_dir),
        "--archive-id", archive_id,
        search_config.query_string,
        "--mongodb-uri", results_cache_uri,
        "--mongodb-collection", results_collection,
    ]
    # fmt: on

    if search_config.begin_timestamp is not None:
        search_cmd.append("--tge")
        search_cmd.append(str(search_config.begin_timestamp))
    if search_config.end_timestamp is not None:
        search_cmd.append("--tle")
        search_cmd.append(str(search_config.end_timestamp))
    if search_config.ignore_case:
        search_cmd.append("--ignore-case")

    return search_cmd


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
    s3_reader_config: Dict[str, Any], mount_path: Path
) -> threading.Thread:
    # Load reader S3 config
    _remove_and_mkdir(mount_path, is_fuse_mount=True)

    # Create a real on disk cache
    reader_mount_cache_path = _get_mount_cache_path(mount_path)
    _remove_and_mkdir(reader_mount_cache_path)

    s3_reader_mount_config = _get_s3_mount_config(s3_reader_config)

    # Create the thread as a daemon so that it exits immediately when the main
    # thread is cancelled
    reader_fuse_thread = threading.Thread(
        target=clp_s3_fuse_sequential_read_thread_method,
        args=(
            s3_reader_mount_config,
            mount_path,
            reader_mount_cache_path,
            s3_reader_config["max_file_size"],
        ),
        daemon=True,
    )

    return reader_fuse_thread


def setup_archive_directory(
    job_id: str,
    task_id: str
) -> Dict[str, Any]:

    # # for the normal file system flow
    # archive_output_dir = os.getenv("CLP_ARCHIVE_OUTPUT_DIR")
    # if archive_output_dir:
    #     return {
    #         "archive_output_path": Path(archive_output_dir)
    #     }

    # for the S3 Flow
    s3_reader_config = HARDCODED_S3_CONFIG
    # 4GB should be a very safe guess.
    s3_reader_config["max_file_size"] = 4 * 1024 * 1024 * 1024

    # A temp hack for access key id and secret
    os.environ["AWS_ACCESS_KEY_ID"] = s3_reader_config["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = s3_reader_config["secret_access_key"]

    # generate reader_mount path
    reader_mount_path = Path("/") / f"reader_mount_{job_id}_{task_id}"

    # Launch the reader mount thread
    reader_fuse_thread = _create_reader_fuse_thread(s3_reader_config, reader_mount_path)
    reader_fuse_thread.start()
    logger.info("Creating FUSE mount...")
    _await_fuse_mount(reader_mount_path)

    # point the storage dir to mounted path
    return {
        "archive_output_path": reader_mount_path,
        "fuse_thread": reader_fuse_thread
    }


def post_search_cleanup(
    setup_config: Dict[str, Any]
) -> None:
    # # Normal flow, do nothing
    # if os.getenv("CLP_ARCHIVE_OUTPUT_DIR"):
    #     return

    # For the S3 Flow
    _unmount_fuse(
        setup_config["archive_output_path"],
        setup_config["fuse_thread"]
    )



@app.task(bind=True)
def search(
    self: Task,
    job_id: str,
    search_config_obj: dict,
    archive_id: str,
    results_cache_uri: str,
) -> Dict[str, Any]:
    task_id = str(self.request.id)
    clp_home = Path(os.getenv("CLP_HOME"))
    clp_logs_dir = Path(os.getenv("CLP_LOGS_DIR"))
    clp_logging_level = str(os.getenv("CLP_LOGGING_LEVEL"))
    clp_storage_engine = str(os.getenv("CLP_STORAGE_ENGINE"))

    # Setup logging to file
    worker_logs_dir = clp_logs_dir / job_id
    worker_logs_dir.mkdir(exist_ok=True, parents=True)
    set_logging_level(logger, clp_logging_level)
    clo_log_path = worker_logs_dir / f"{task_id}-clo.log"
    clo_log_file = open(clo_log_path, "w")

    logger.info(f"Started task for job {job_id}")

    search_config = SearchConfig.parse_obj(search_config_obj)
    setup_result = setup_archive_directory(job_id, task_id)
    archive_directory = setup_result["archive_output_path"]
    archive_path = archive_directory / archive_id

    if StorageEngine.CLP == clp_storage_engine:
        search_cmd = make_clo_command(
            clp_home=clp_home,
            archive_path=archive_path,
            search_config=search_config,
            results_cache_uri=results_cache_uri,
            results_collection=job_id,
        )
    elif StorageEngine.CLP_S == clp_storage_engine:
        search_cmd = make_clp_s_command(
            clp_home=clp_home,
            archives_dir=archive_directory,
            archive_id=archive_id,
            search_config=search_config,
            results_cache_uri=results_cache_uri,
            results_collection=job_id,
        )
    else:
        logger.error(f"Unsupported storage engine {clp_storage_engine}")
        return SearchTaskResult(
            success=False,
            task_id=task_id,
        ).dict()

    logger.info(f'Running: {" ".join(search_cmd)}')
    search_successful = False
    search_proc = subprocess.Popen(
        search_cmd,
        preexec_fn=os.setpgrp,
        close_fds=True,
        stdout=clo_log_file,
        stderr=clo_log_file,
    )

    def sigterm_handler(_signo, _stack_frame):
        logger.debug("Entered sigterm handler")
        if search_proc.poll() is None:
            logger.debug("Trying to kill search process")
            # Kill the process group in case the search process also forked
            os.killpg(os.getpgid(search_proc.pid), signal.SIGTERM)
            os.waitpid(search_proc.pid, 0)
            logger.info(f"Cancelling search task.")
            # Post search setup
            post_search_cleanup(setup_result)
        # Add 128 to follow convention for exit codes from signals
        # https://tldp.org/LDP/abs/html/exitcodes.html#AEN23549
        sys.exit(_signo + 128)

    # Register the function to kill the child process at exit
    signal.signal(signal.SIGTERM, sigterm_handler)

    logger.info("Waiting for search to finish")
    # communicate is equivalent to wait in this case, but avoids deadlocks if we switch to piping
    # stdout/stderr in the future.
    search_proc.communicate()
    return_code = search_proc.returncode
    if 0 != return_code:
        logger.error(f"Failed search task for job {job_id} - return_code={return_code}")
    else:
        search_successful = True
        logger.info(f"Search task completed for job {job_id}")

    # post search
    post_search_cleanup(setup_result)

    # Close log files
    clo_log_file.close()

    return SearchTaskResult(
        success=search_successful,
        task_id=task_id,
    ).dict()
