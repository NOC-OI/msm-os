"""Argument parser module."""
import argparse

from ..__init__ import __version__


def create_parser():
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description=f"msm-os {__version__} command line interface",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Send and Update are mutually exclusive operations
    parser.add_argument(
        "action",
        choices=["send", "update"],
        help="Specify the action: 'send' to send a file or 'update' to update an existing object.",
    )

    # Always required
    parser.add_argument(
        "-f",
        "--filepaths",
        dest="filepaths",
        help="Paths to the the files to send.",
        nargs="+",
        required=True,
    )

    parser.add_argument(
        "-c",
        "--credentials",
        dest="store_credentials_json",
        help="Path to the JSON file containing the credentials for the object store.",
        required=True,
    )

    parser.add_argument(
        "-b",
        "--bucket",
        dest="bucket",
        help="Bucket name.",
        required=True,
    )

    # Optional arguments
    parser.add_argument(
        "-p",
        "--prefix",
        dest="object_prefix",
        help="Object prefix.",
        default=None,
    )

    parser.add_argument(
        "-a",
        "--append-dim",
        dest="append_dim",
        help="Append dimension.",
        default="time_counter",
    )

    parser.add_argument(
        "-v",
        "--variables",
        dest="variables",
        help="Variables to send.",
        nargs="+",
        default=None,
    )

    return parser
