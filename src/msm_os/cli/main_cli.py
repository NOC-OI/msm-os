"""msm_os command line interface."""
import logging
import sys

from ..object_store_handler import send, update
from .argument_parser import __version__, create_parser

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


def msm_os():
    """Run the msm_os cli."""
    logging.basicConfig(
        stream=sys.stdout,
        format="msm_os | %(levelname)s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    banner()

    parser = create_parser()
    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    if args.variables is None:
        variables = None
    else:
        variables = list(args.variables)

    if args.action == "send":
        if vars == "compact":
            send_vars_indep = False
        else:
            send_vars_indep = True

        send(
            filepaths=list(args.filepaths),
            bucket=args.bucket,
            store_credentials_json=args.store_credentials_json,
            variables=variables,
            append_dim=args.append_dim,
            send_vars_indep=send_vars_indep,
            object_prefix=args.object_prefix,
            to_zarr_kwargs=None,
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
    else:
        raise NotImplementedError(f"Action {args.action} not implemented.")

    logging.info("msm_os finished successfully.")
    sys.exit(0)
