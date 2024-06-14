import argparse
import logging
import os
import pathlib
import subprocess
import sys
import uuid

import yaml

from clp_package_utils.general import (
    CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH,
    CONTAINER_CLP_HOME,
    generate_container_config,
    get_clp_home,
    validate_and_load_config_file,
    validate_and_load_db_credentials_file,
)

# Setup logging
# Create logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
# Setup console logging
logging_console_handler = logging.StreamHandler()
logging_formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logging_console_handler.setFormatter(logging_formatter)
logger.addHandler(logging_console_handler)


def main(argv):
    clp_home = get_clp_home()
    default_config_file_path = clp_home / CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH

    args_parser = argparse.ArgumentParser(description="Searches the compressed logs.")
    args_parser.add_argument(
        "--config",
        "-c",
        default=str(default_config_file_path),
        help="CLP package configuration file.",
    )
    args_parser.add_argument("orig_file_id", help="Original File ID.")
    args_parser.add_argument("msg_ix", type=int, help="Message index.")
    args_parser.add_argument(
        "--target-size",
        type=int,
        help="Target IR size"
    )
    parsed_args = args_parser.parse_args(argv[1:])

    # Validate and load config file
    try:
        config_file_path = pathlib.Path(parsed_args.config)
        clp_config = validate_and_load_config_file(
            config_file_path, default_config_file_path, clp_home
        )
        clp_config.validate_logs_dir()

        # Validate and load necessary credentials
        validate_and_load_db_credentials_file(clp_config, clp_home, True)
    except:
        logger.exception("Failed to load config.")
        return -1

    container_name = f"clp-extract-{str(uuid.uuid4())[-4:]}"

    container_clp_config, mounts = generate_container_config(clp_config, clp_home)
    container_config_filename = f".{container_name}-config.yml"
    container_config_file_path_on_host = clp_config.logs_directory / container_config_filename
    with open(container_config_file_path_on_host, "w") as f:
        yaml.safe_dump(container_clp_config.dump_to_primitive_dict(), f)

    clp_site_packages_dir = CONTAINER_CLP_HOME / "lib" / "python3" / "site-packages"
    # fmt: off
    container_start_cmd = [
        "docker", "run",
        "-i",
        "--rm",
        "--network", "host",
        "-w", str(CONTAINER_CLP_HOME),
        "-e", f"PYTHONPATH={clp_site_packages_dir}",
        "-u", f"{os.getuid()}:{os.getgid()}",
        "--name", container_name,
        "--log-driver", "local",
        "--mount", str(mounts.clp_home),
    ]
    # fmt: on
    necessary_mounts = [mounts.logs_dir]
    for mount in necessary_mounts:
        if mount:
            container_start_cmd.append("--mount")
            container_start_cmd.append(str(mount))
    container_start_cmd.append(clp_config.execution_container)

    # fmt: off
    extract_cmd = [
        "python3",
        "-m", "clp_package_utils.scripts.native.ir",
        "--config", str(container_clp_config.logs_directory / container_config_filename),
        parsed_args.orig_file_id,
        str(parsed_args.msg_ix)
    ]
    # fmt: on
    if parsed_args.target_size:
        extract_cmd.append("--target-size")
        extract_cmd.append(str(parsed_args.target_size))
    cmd = container_start_cmd + extract_cmd
    subprocess.run(cmd, check=True)

    # Remove generated files
    container_config_file_path_on_host.unlink()

    return 0


if "__main__" == __name__:
    sys.exit(main(sys.argv))
