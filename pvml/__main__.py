import argparse
import datetime
import logging
from pathlib import Path
import os
import sys

from .config import Config
from .exceptions import Error
from .joborder import Job
from .version import __version__

__copyright__ = "Copyright (C) 2009-2024 S[&]T, The Netherlands."


class ProcessorStdoutConsoleHandler(logging.Handler):
    def emit(self, record):
        print("     ---- stdout ----      " + record.getMessage())
        sys.stdout.flush()


class ProcessorStderrConsoleHandler(logging.Handler):
    def emit(self, record):
        print("     ---- stderr ----      " + record.getMessage())
        sys.stdout.flush()


class SimpleConsoleHandler(logging.Handler):
    def emit(self, record):
        if record.levelname in ["WARNING", "ERROR"]:
            print(record.levelname + ": " + record.getMessage())


class PVMLConsoleHandler(logging.Handler):
    def emit(self, record):
        timestamp = datetime.datetime.fromtimestamp(record.created).isoformat()
        print(" ".join((timestamp, record.levelname, record.getMessage())))
        sys.stdout.flush()


def main():
    logger = logging.getLogger("pvml")

    # This parser is used in combination with the parse_known_args() function as a way to implement a "--version"
    # option that prints version information and exits, and is included in the help message.
    #
    # The "--version" option should have the same semantics as the "--help" option in that if it is present on the
    # command line, the corresponding action should be invoked directly, without checking any other arguments.
    # However, the argparse module does not support user defined options with such semantics.
    version_parser = argparse.ArgumentParser(add_help=False)
    version_parser.add_argument("--version", action="store_true", help="output version information and exit")

    description = "Run the job as described in the PVML job config file. The PVML global config file reference is " \
        "optional. However, if you do not provide it you will have to set the PVML_CONFIG environment variable."
    epilog = "The PVML global config file can be either provided as a parameter to the pvml or you can set the " \
        "PVML_CONFIG enviroment variable to point to this file. If you supply both, the one provided as parameter " \
        "to the pvml takes precedence."

    parser = argparse.ArgumentParser(prog="pvml", description=description, epilog=epilog, parents=[version_parser])
    parser.add_argument("-d", "--debug", action="store_true", help="print full stack trace for general exceptions")
    parser.add_argument("--joborder", action="store_true", help="only generate the joborder and print this to stdout")
    parser.add_argument("global_config", type=Path, nargs="?", metavar="<PVML global config file>")
    parser.add_argument("job_config", type=Path, metavar="<PVML job config file>")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        print(f"Processor Verification Management Layer (PVML) v{__version__}")
        print(__copyright__)
        print()
        sys.exit(0)

    startupLogHandler = SimpleConsoleHandler()
    logger.addHandler(startupLogHandler)

    args = parser.parse_args(unused_args)

    if args.global_config is None:
        args.global_config = os.getenv("PVML_CONFIG")
        if args.global_config is None:
            logger.error("no PVML configuration file specified")
            sys.exit(1)

    try:
        config = Config()
        config.read_global_config(args.global_config)
        config.read_job_config(args.job_config)
        job = Job(config)
        if args.joborder:
            print(job.get_joborder().decode("utf-8"), end="")
        else:
            logger.removeHandler(startupLogHandler)
            logger.addHandler(PVMLConsoleHandler())
            logging.getLogger("pvml_processor_stdout").addHandler(ProcessorStdoutConsoleHandler())
            logging.getLogger("pvml_processor_stderr").addHandler(ProcessorStderrConsoleHandler())
            job.run()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.exit(1)
    except Error as _error:
        logger.error(str(_error))
        sys.exit(1)
    except Exception:
        if args.debug:
            print(str(sys.exc_info()[1]).strip("\n\r"))
            sys.exit(1)
        else:
            raise


if __name__ == "__main__":
    main()
