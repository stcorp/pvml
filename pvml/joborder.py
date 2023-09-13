from collections import OrderedDict
import copy
import datetime
from dataclasses import dataclass, field
import logging
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
from typing import Optional, Dict, List, Union

from .config import Config
from .exceptions import Error, ProcessorError
from .version import __version__


@dataclass
class InputProduct:
    reference: str  # archive backend specific reference to the file
    filename: str  # filename (as referenced in the joborder file)
    start_time: Optional[datetime.datetime] = None
    stop_time: Optional[datetime.datetime] = None


@dataclass
class Input:
    product_type: str
    products: List[InputProduct] = field(default_factory=list)


@dataclass
class OutputProduct:
    filepaths: List[Path]
    metadata_filepath: Optional[Path] = None


@dataclass
class Output:
    product_type: str
    products: List[OutputProduct] = field(default_factory=list)


@dataclass
class Task:
    name: str
    version: str
    executable: Path
    expected_exit_codes: List[int] = field(default_factory=lambda: [0])


logger = logging.getLogger("pvml")
logger.setLevel(logging.INFO)
processor_stdout_logger = logging.getLogger("pvml_processor_stdout")
processor_stdout_logger.setLevel(logging.INFO)
processor_stderr_logger = logging.getLogger("pvml_processor_stderr")
processor_stderr_logger.setLevel(logging.INFO)


class PVMLLogFileHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.pvml_log = None
        self.pvml_log_buffer = []

    def emit(self, record):
        timestamp = datetime.datetime.fromtimestamp(record.created).isoformat()
        line = timestamp + " " + record.levelname + " " + record.getMessage() + "\n"
        if self.pvml_log is None:
            self.pvml_log_buffer.append(line)
        else:
            if len(self.pvml_log_buffer) != 0:
                for buffered_line in self.pvml_log_buffer:
                    self.pvml_log.write(buffered_line)
                self.pvml_log_buffer = []
            self.pvml_log.write(line)
            self.pvml_log.flush()


def extract_lines(buf, start=0):
    """Extract lines from the provided buffer. A line is a sequence of
    characters that ends in "\\n". The newline characters ("\\n") are discarded
    from the output.

    Returns a list of extracted lines, as well as a new buffer that contains any
    remaining characters from the end of the original buffer.

    Keyword arguments:
    start   --  Position in buffer to start searching for newlines. Use this if
                it is already known that the start of the buffer does not
                contain any newlines.

    """
    end = buf.find("\n", start)
    if end == -1:
        return ([], buf)

    lines = [buf[:end]]
    start = end + 1
    while True:
        end = buf.find("\n", start)
        if end == -1:
            return (lines, buf[start:])

        lines.append(buf[start:end])
        start = end + 1


def wrapped_subprocess_call(exe, args=[], working_directory=None, stdout_callback=None, stderr_callback=None):
    """Execute another program as a subprocess. The optional callbacks for
    stdout and stderr are called per line of output produced by the subprocess.

    Keyword arguments:
    args                --  Command line arguments for the subprocess.
    working_directory   --  The working directory will be changed to this
                            directory before spawning the subprocess.
    stdout_callback     --  This callback will be called for each line of output
                            received from the subprocess on stdout.
    stderr_callback     --  This callback will be called for each line of output
                            received from the subprocess on stderr.
    """
    process = subprocess.Popen([exe] + args, cwd=working_directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    pipes = [process.stdout, process.stderr]
    pipe_state = {
        process.stdout.fileno(): ["", stdout_callback],
        process.stderr.fileno(): ["", stderr_callback]
    }

    while pipes:
        try:
            readable, _, _ = select.select(pipes, [], [])
        except select.error as _error:
            if _error.args[0] == errno.EINTR:
                continue
            raise

        for pipe in readable:
            buf, callback = pipe_state[pipe.fileno()]

            # Read a block of character data.
            data = os.read(pipe.fileno(), 8192)
            assert data is not None
            data = data.decode('utf-8', errors='replace')

            if len(data) == 0:
                # Execute callback for any characters remaining in the buffer.
                if len(buf) > 0 and callback is not None:
                    callback(buf)

                # Close pipe.
                pipe.close()
                pipes.remove(pipe)
                continue

            # Extract complete lines from the pipe buffer.
            lines, buf = extract_lines(buf + data, len(buf))
            pipe_state[pipe.fileno()][0] = buf

            # Execute callback for each extracted line.
            if callback is not None:
                for line in lines:
                    callback(line)

    return process.wait()


def log_callback(log, processor_logger):
    def _callback(line):
        processor_logger.info(line)
        log.write(f"{line}\n")
        log.flush()
    return _callback


def run_task(exe, name, version, job_path, job_id, expected_exit_codes, task_wrapper=None):
    logger.info(f"starting task '{name}/{version}'")
    logger.info(f"running command '{exe} {job_path}'")

    args = [job_path]
    if task_wrapper is not None:
        args = [exe, job_path]
        exe = task_wrapper
        if not os.path.split(exe)[0]:
            # determine full path based on PATH environment variable
            for path in os.environ["PATH"].split(os.pathsep):
                full_path = os.path.join(path, exe)
                if os.path.isfile(full_path):
                    exe = full_path
                    break

    log = "LOG." + job_id
    with open(log, "a") as log_fp:
        status = wrapped_subprocess_call(exe, args, os.getcwd(),
                                         log_callback(log_fp, processor_stdout_logger),
                                         log_callback(log_fp, processor_stderr_logger))

    if status == 0:
        logger.info(f"task '{name}/{version}' finished successfully")
    else:
        logger.info(f"task '{name}/{version}' finished with exit code {status}")
    if status not in expected_exit_codes:
        msg = f"task '{name}/{version}' finished with exit code {status} but expected "
        if len(expected_exit_codes) == 1:
            msg += str(expected_exit_codes[0])
        else:
            msg += "one of [" + ",".join([str(x) for x in expected_exit_codes]) + "]"
        raise ProcessorError(msg)


@dataclass
class Job:
    config: Config
    tasks: List[Task]
    inputs: Dict[str, Input]
    outputs: Dict[str, Output]
    working_directory: Path

    def __init__(self, config: Config) -> None:
        self.config = config
        self.tasks = []
        self.inputs = {}
        self.outputs = {}

        self.log_file_handler = PVMLLogFileHandler()
        logger.addHandler(self.log_file_handler)

        backend_module_name = f"pvml.{self.config.interface_backend.lower()}"
        try:
            __import__(backend_module_name)
            backend_module = sys.modules[backend_module_name]
        except ImportError:
            raise Error(f"import of '{backend_module_name}' failed")
        self.backend = backend_module.Backend()

        try:
            __import__(self.config.archive_backend)
            self.archive_module = sys.modules[self.config.archive_backend]
        except ImportError:
            raise Error(f"import of extension module '{self.config.archive_backend}' failed")

        if self.config.working_directory is None:
            self.working_directory = Path(self.config.workspace_directory, self.config.joborder_id)
        elif self.config.working_directory.is_absolute():
            self.working_directory = self.config.working_directory
        else:
            self.working_directory = Path(self.config.workspace_directory, self.config.working_directory)
        self.working_directory = Path(os.path.abspath(os.path.normpath(self.working_directory)))

    def _create_working_directory(self):
        if not self.working_directory.exists():
            try:
                os.mkdir(self.working_directory)
            except Exception as e:
                raise Error(f"could not create working directory '{self.working_directory}' ({e.__str__()})")
        else:
            current_files = os.listdir(self.working_directory)
            for entry in current_files:
                try:
                    path = Path(self.working_directory, entry)
                    if os.path.isfile(path) or os.path.islink(path):
                        os.remove(path)
                    else:
                        shutil.rmtree(path)
                except Exception as e:
                    raise Error(f"could not empty working directory, unable to delete '{entry}' ({e.__str__()})")

    def _retrieve_inputs(self):
        with self.archive_module.Archive(self.config) as archive:
            archive.retrieve(self.inputs.values(), self.working_directory)

    def _ingest_outputs(self) -> List[Union[str, List[str]]]:
        with self.archive_module.Archive(self.config) as archive:
            archive.ingest(self.outputs.values(), self.inputs.values())
        output_files = []  # type: List[Union[str, List[str]]]
        for output in self.outputs.values():
            for product in output.products:
                # return single-file products as a scalar and multi-file products as a list
                if len(product.filepaths) == 1:
                    output_files.append(product.filepaths[0].name)
                else:
                    output_files.append([path.name for path in product.filepaths])
        return sorted(output_files)

    def get_joborder(self):
        self.backend.initialize_job(self)
        return self.backend.write_joborder(self, dry_run=True)

    def run(self):
        processor_reference = f"{self.config.processor_name}/{self.config.processor_version}"
        logger.info(f"starting processor '{processor_reference}' using PVML {__version__}")

        self.backend.initialize_job(self)

        self._create_working_directory()
        try:
            os.chdir(self.working_directory)

            self.log_file_handler.pvml_log = open(Path(self.working_directory, "pvml.log"), "w")

            joborder = self.backend.write_joborder(self)

            self._retrieve_inputs()

            for task in self.tasks:
                run_task(task.executable, task.name, task.version, str(joborder), self.config.joborder_id,
                         task.expected_exit_codes, self.config.task_wrapper)

            self.backend.locate_and_check_outputs(self)

            output_products = self._ingest_outputs()
            logger.info(f"processor '{processor_reference}' finished successfully")
            return output_products
        except Exception:
            logger.info(f"processor '{processor_reference}' failed")
            raise
        finally:
            if self.log_file_handler.pvml_log is not None:
                self.log_file_handler.pvml_log.close()
                self.log_file_handler.pvml_log = None
