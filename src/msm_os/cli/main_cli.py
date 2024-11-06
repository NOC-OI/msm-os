"""Command line interface (CLI) module."""

import logging
import sys

from ..object_store_handler import get_files, send, update
from .argument_parser import __version__, create_parser
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import math

logger = logging.getLogger(__name__)


def banner():
    """Log the msm_os banner."""
    logger.info(
        f"""
          .-~~~-.
  .- ~ ~-(       )_ _
 /                    ~ -.
|          msm-os         ',
 ¬                         .'
   ~- ._ ,. ,.,.,., ,.. -~
           '       '
       version: {__version__}

""",
        extra={"simple": True},
    )


def initialise_logging():
    """Initialise logging configuration."""
    logging.basicConfig(
        stream=sys.stdout,
        format="☁  msm_os ☁  | %(levelname)10s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_slurm_job(job: dict) -> Client:
    job_type = job.get("type")
    queue = job.get("queue", "par-single")
    cores = job.get("cores", 16)
    processes = job.get("processes", round(math.sqrt(cores)))
    memory = job.get("memory", "256GB")
    scale = job.get("scale", 1)
    logging.info(
        f"Creating a SLURM cluster with {cores} cores, {processes} processes, {memory} of memory, and {scale} jobs.")
    cluster = SLURMCluster(
        queue=queue,
        cores=cores,
        processes=processes,  # processes = sqrt(cores) - recommended by JASMIN
        memory=memory,
        walltime="12:00:00",
        job_extra_directives=[
            "--output=slurm-%j.out",
            "--error=slurm-%j.err",
        ],  # SLURM job output and error files
    )
    cluster.scale(jobs=scale)
    client = Client(cluster)
    return client

def parse_job(job: dict) -> Client:
    """Parse the job configuration.

    Args:
        job (dict): Job configuration.

    Returns:
        dask.distributed.Client: Dask client.
    """
    job_type = job.get("type", "local")
    if job_type == "slurm":
        client = parse_slurm_job(job)
    elif job_type == "local":
        client = Client()
    elif job_type == "threads":
        client = job.get("num_threads", 4)
    else:
        raise ValueError(f"Job type {job_type} not supported.")
    return {
        "type": job_type,
        "client": client,
        "job": job,
    }

def process_action(args):
    """Process the selected action."""
    if len(sys.argv) == 1:
        args.parser.print_help()
        sys.exit(0)

    variables = list(args.variables) if args.variables is not None else None

    if args.action == "send":
        if args.variables is not None and "compact" in args.variables:
            send_vars_indep = True
        else:
            send_vars_indep = False

        if args.job is not None:
            client = parse_job(args.job)
        else:
            client = None

        send(
            filepaths=list(args.filepaths),
            bucket=args.bucket,
            store_credentials_json=args.store_credentials_json,
            variables=variables,
            append_dim=args.append_dim,
            send_vars_indep=not send_vars_indep,
            object_prefix=args.object_prefix,
            rechunk=args.chunk_strategy,
            reproject=args.reproject,
            skip_integrity_check=args.skip_integrity_check,
            to_zarr_kwargs=None,
            client=client,
        )

    elif args.action == "update":
        update(
            filepaths=list(args.filepaths),
            bucket=args.bucket,
            store_credentials_json=args.store_credentials_json,
            variables=variables,
            object_prefix=args.object_prefix,
            to_zarr_kwargs=None,
        )

    elif args.action == "list":
        get_files(
            bucket=args.bucket,
            store_credentials_json=args.store_credentials_json,
        )
    else:
        raise NotImplementedError(f"Action {args.action} not implemented.")


def msm_os():
    """Run the msm_os cli."""
    initialise_logging()
    banner()

    parser = create_parser()
    args = parser.parse_args()

    process_action(args)

    logging.info("✔ msm_os terminated successfully ✔")
    sys.exit(0)
