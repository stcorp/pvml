from collections import OrderedDict
import datetime
from dataclasses import dataclass, field
import glob
import logging
import os
from pathlib import Path
import re
from typing import List, Dict, Optional, Union

from lxml import etree

from .exceptions import Error, ProcessorError
from . import joborder

logger = logging.getLogger("pvml")


def intersects(start_a, stop_a, start_b, stop_b):
    return start_a <= stop_a and start_b <= stop_b and start_b <= stop_a and start_a <= stop_b


def intersection(start_a, stop_a, start_b, stop_b):
    return max(start_a, start_b), min(stop_a, stop_b)


def read_tasktable(config):
    tasktable = None
    xmlschema = None

    if config.tasktable_schema is not None:
        xmlschema = etree.XMLSchema(file=config.tasktable_schema)

    tasktable_paths = []
    for path in config.tasktable_path:
        path = path.resolve()
        if path.is_dir():
            for item in path.iterdir():
                if item.is_file() and item.name[0] != '.':
                    tasktable_paths.append(item)
        elif path.is_file():
            tasktable_paths.append(path)
        else:
            # try treating path as a glob pattern
            for item in glob.glob(str(path)):
                path = Path(item)
                if path.is_file():
                    tasktable_paths.append(path)

    for path in tasktable_paths:
        tree = etree.parse(path)
        if xmlschema is not None:
            etree.clear_error_log()
            try:
                xmlschema.assertValid(tree)
            except etree.DocumentInvalid as exc:
                logger.error(f"could not verify tasktable '{path}' against schema '{config.tasktable_schema}'")
                for error in exc.error_log:  # type: ignore
                    logger.error(f"{error.filename}:{error.line}: {error.message}")
                raise Error(f"invalid tasktable file '{path}'")
            logger.info(f"tasktable '{path}' valid according to schema '{config.tasktable_schema}'")

        processor_name = tree.findtext("Processor_Name")
        processor_version = tree.findtext("Version")
        if processor_name is not None and processor_version is not None:
            if processor_name == config.processor_name and processor_version == config.processor_version:
                if tasktable is not None:
                    raise Error(f"multiple tasktables for {processor_name}/{processor_version}")
                tasktable = tree

    if tasktable is None:
        raise Error(f"no tasktable found for {config.processor_name}/{config.processor_version}")

    return tasktable


@dataclass
class InputFile:
    file_name: str  # reference in the joborder file
    start_time: Optional[datetime.datetime] = None
    stop_time: Optional[datetime.datetime] = None


@dataclass
class Input:
    file_type: str
    file_name_type: str
    files: List[InputFile] = field(default_factory=list)
    input_source_data: Optional[bool] = None


@dataclass
class Output:
    file_type: str
    file_name_type: str
    file_name: str
    destination: str
    mandatory: bool


@dataclass
class Task:
    name: str
    version: str
    inputs: List[Input] = field(default_factory=list)
    outputs: List[Output] = field(default_factory=list)


@dataclass
class Backend:
    enable_test: bool = False
    config_files: List[str] = field(default_factory=list)
    config_spaces: Dict[str, str] = field(default_factory=dict)
    include_global_sensing_time: bool = True
    variant_dynamic_processing_parameters: bool = True
    variant_private_config_file: bool = False
    outputs: Dict[str, Output] = field(default_factory=dict)
    tasks: List[Task] = field(default_factory=list)
    parameters: OrderedDict[str, str] = field(default_factory=OrderedDict)
    sensing_start: Optional[datetime.datetime] = None
    sensing_stop: Optional[datetime.datetime] = None

    def initialize_job(self, job: joborder.Job) -> None:
        """
        Fully initialize job.tasks and job.inputs and resolve everything to be able to write the joborder file
        job.inputs should only have entries where origin is DB
        This function should not write anything to disk (don't assume that job.working_directory is created yet)
        This function is allowed to use job.archive_module to resolve input references and find unassigned inputs
        """
        assert len(self.tasks) == 0  # don't allow double initialisation

        # check configuration options
        if job.config.variant_use_numerical_order_id:
            try:
                int(job.config.joborder_id)
            except ValueError:
                raise Error(f"job order identifier '{job.config.joborder_id}' should be an integer")

        # read task table
        tree = read_tasktable(job.config)

        value = tree.findtext("Test")
        if value == "Yes":
            self.enable_test = True

        default_config_file = None  # type: Optional[int]
        config_element = tree.find("Private_Config")
        if config_element is not None:
            self.variant_private_config_file = True
            value = config_element.findtext("Default")
            if value is not None:
                default_config_file = int(value)
        else:
            self.variant_private_config_file = False
            config_element = tree
        self.config_files = []
        config_file_elements = config_element.findall("List_of_Cfg_Files/Cfg_File")
        if len(config_file_elements) == 0:
            # Try alternative name
            config_file_elements = config_element.findall("List_of_Cfg_Files/Cfg_Files")
        if self.variant_private_config_file:
            if len(config_file_elements) > 0:
                if default_config_file is None:
                    default_config_file = 0
                element = config_file_elements[default_config_file]
                filename = element.findtext("File_Name")
                if filename is not None:
                    self.config_files.append(filename)
        else:
            for element in config_file_elements:
                filename = element.findtext("File_Name")
                if filename is not None:
                    self.config_files.append(filename)

        for element in tree.findall("List_of_Config_Spaces/Config_Space"):
            config_space = element.text
            if config_space is not None:
                if config_space not in job.config.config_spaces:
                    raise Error(f"config space '{config_space}' is not assigned a value")
                self.config_spaces[config_space] = job.config.config_spaces[config_space]

        if job.config.variant_sensing_time_flag:
            # If the Sensing_Time_flag is allowed in the tasktable then the default value for this flag is False
            self.include_global_sensing_time = False
            value = tree.findtext("Sensing_Time_flag")
            if value == "true":
                self.include_global_sensing_time = True

        if tree.find("Processing_Parameters") is not None:
            element_path = "Processing_Parameters/Processing_Parameter"
            self.variant_dynamic_processing_parameters = False
        else:
            element_path = "List_of_Dyn_ProcParam/Dyn_ProcParam"
            self.variant_dynamic_processing_parameters = True
        for element in tree.findall(element_path):
            name = element.findtext("Param_Name")
            assert name is not None
            default_element = element.find("Param_Default")
            if name in job.config.processing_parameters:
                self.parameters[name] = job.config.processing_parameters[name]
            elif default_element is not None:
                self.parameters[name] = element.find("Param_Default").text
            elif element.get("mandatory") != "false":
                raise Error(f"mandatory processing parameter '{name}' is not assigned a value")
            if name in self.parameters:
                valid_list = [element.text for element in element.findall("Param_Valid")]
                if len(valid_list) > 0:
                    if self.parameters[name] not in valid_list:
                        raise Error(f"processing parameter '{name}' has invalid value '{self.parameters[name]}', " +
                                    "supported are " + ", ".join(valid_list))

        # assign inputs based on config
        self.sensing_start = job.config.sensing_start
        self.sensing_stop = job.config.sensing_stop
        predefined_inputs = {}  # type: Dict[str, joborder.Input]
        input_cache = {}  # type: Dict[str, Input]
        with job.archive_module.Archive(job.config) as archive:
            for config_input in job.config.inputs:
                if len(config_input.products) == 0:
                    continue
                input_products = []
                input_start_time = None
                input_stop_time = None
                for config_product in config_input.products:
                    validity_start = config_product.start_time
                    validity_stop = config_product.stop_time
                    product = archive.resolve_reference(config_product.reference, config_input.product_type)
                    if validity_start is None:
                        validity_start = product.validity_start
                    if validity_stop is None:
                        validity_stop = product.validity_stop
                    if job.config.variant_always_include_input_time_interval:
                        if validity_start is None:
                            raise Error(f"could not retrieve start time for '{config_input.product_type}' product " +
                                        f"'{config_product.reference}'")
                        if validity_stop is None:
                            raise Error(f"could not retrieve stop time for '{config_input.product_type}' product " +
                                        f"'{config_product.reference}'")
                    input_product = joborder.InputProduct(product.reference, product.filename, validity_start,
                                                          validity_stop)
                    if validity_start is not None:
                        if input_start_time is None or validity_start < input_start_time:
                            input_start_time = validity_start
                    if validity_stop is not None:
                        if input_stop_time is None or validity_stop > input_stop_time:
                            input_stop_time = validity_stop
                    input_products.append(input_product)
                predefined_inputs[config_input.product_type] = joborder.Input(config_input.product_type, input_products)
                if self.sensing_start is None and input_start_time is not None:
                    self.sensing_start = input_start_time
                if self.sensing_stop is None and input_stop_time is not None:
                    self.sensing_stop = input_stop_time

            if self.sensing_start is None:
                self.sensing_start = datetime.datetime.min
            if self.sensing_stop is None:
                self.sensing_stop = datetime.datetime.max
            logger.info("sensing start time is %s" % job.config.format_timestamp(self.sensing_start))
            logger.info("sensing stop time is %s" % job.config.format_timestamp(self.sensing_stop, round_up=True))

            named_inputs = {}  # type: Dict[str, Input]
            for pool_element in tree.findall("List_of_Pools/Pool"):
                for task_element in pool_element.findall("List_of_Tasks/Task"):
                    name = task_element.findtext("Name")
                    assert name is not None
                    version = task_element.findtext("Version")
                    assert version is not None
                    executable = task_element.findtext("File_Name")
                    assert executable is not None
                    job_task = joborder.Task(name, version, executable)
                    job.tasks.append(job_task)

                    task = Task(name, version)

                    for index, input_element in enumerate(task_element.findall("List_of_Inputs/Input"), start=1):
                        if input_element.get("ref") is not None:
                            id = input_element.get("ref")
                            if id not in named_inputs:
                                raise Error(f"task '{job_task.name}' contains input reference using unknown id '{id}'")
                            input = named_inputs[id]
                        else:
                            mode = input_element.findtext("Mode")
                            assert mode is not None
                            if mode != "ALWAYS" and mode != job.config.mode:
                                # Input is irrelevant given the job configuration and should be ignored
                                continue
                            mandatory = input_element.findtext("Mandatory") != "No"
                            input = None
                            alternatives = []
                            for alternative_element in input_element.findall("List_of_Alternatives/Alternative"):
                                alternative = {}
                                alternative["file_type"] = alternative_element.findtext("File_Type")
                                assert alternative["file_type"] is not None
                                alternative["file_name_type"] = alternative_element.findtext("File_Name_Type")
                                assert alternative["file_name_type"] is not None
                                alternative["origin"] = alternative_element.findtext("Origin")
                                assert alternative["origin"] is not None
                                alternative["retrieval_mode"] = alternative_element.findtext("Retrieval_Mode")
                                assert alternative["retrieval_mode"] is not None
                                value = alternative_element.findtext("Order")
                                alternative["order"] = 0 if value is None else int(value)
                                value = alternative_element.findtext("T0")
                                alternative["t0"] = 0 if value is None else datetime.timedelta(seconds=float(value))
                                value = alternative_element.findtext("T1")
                                alternative["t1"] = 0 if value is None else datetime.timedelta(seconds=float(value))
                                value = alternative_element.findtext("Input_Source_Data")
                                if value is not None:
                                    alternative["input_source_data"] = value in ["true", "1"]
                                alternatives.append(alternative)
                            alternatives = sorted(alternatives, key=lambda alternative: alternative["order"])
                            for alternative in alternatives:
                                if alternative["file_type"] in input_cache:
                                    input = input_cache[alternative["file_type"]]
                                    break
                                elif alternative["file_type"] in predefined_inputs:
                                    predefined_input = predefined_inputs[alternative["file_type"]]
                                    input = Input(alternative["file_type"], alternative["file_name_type"])
                                    for input_product in predefined_input.products:
                                        input.files.append(InputFile(input_product.filename, input_product.start_time,
                                                                     input_product.stop_time))
                                    if "input_source_data" in alternative:
                                        input.input_source_data = alternative["input_source_data"]
                                    assert alternative["file_type"] not in job.inputs
                                    job.inputs[alternative["file_type"]] = predefined_inputs[alternative["file_type"]]
                                    del predefined_inputs[alternative["file_type"]]
                                    break
                            if input is None:
                                for alternative in alternatives:
                                    if alternative["origin"] == "DB":
                                        products = archive.resolve_mmfi(alternative["file_type"],
                                                                        alternative["retrieval_mode"],
                                                                        self.sensing_start, self.sensing_stop,
                                                                        alternative["t0"], alternative["t1"])
                                        if len(products) > 0:
                                            input = Input(alternative["file_type"], alternative["file_name_type"])
                                            job_input = joborder.Input(alternative["file_type"])
                                            for product in products:
                                                input.files.append(InputFile(product.filename, product.validity_start,
                                                                             product.validity_stop))
                                                job_input.products.append(joborder.InputProduct(product.reference,
                                                                                                product.filename,
                                                                                                product.validity_start,
                                                                                                product.validity_stop))
                                            if "input_source_data" in alternative:
                                                input.input_source_data = alternative["input_source_data"]
                                            assert job_input.product_type not in job.inputs
                                            job.inputs[job_input.product_type] = job_input
                                            break
                                    elif alternative["origin"] in ["PROC", "LOG"]:
                                        input = Input(alternative["file_type"], alternative["file_name_type"])
                                        if alternative["origin"] == "LOG":
                                            input.files.append(InputFile("LOG." + job.config.joborder_id))
                                        elif alternative["file_name_type"] in ["Physical", "Stem"]:
                                            input.files.append(InputFile(alternative["file_type"]))
                                        elif alternative["file_name_type"] == "Regexp":
                                            if input.file_type not in job.config.product_types:
                                                raise Error(f"product type '{input.file_type}' not defined in " +
                                                            "global config")
                                            filename = job.config.product_types[input.file_type].match_expression
                                            if filename is None:
                                                raise Error(f"match expression for product type '{input.file_type}' " +
                                                            "missing in global config")
                                            input.files.append(InputFile(filename))
                                        else:
                                            input.files.append(InputFile(""))
                                        if "input_source_data" in alternative:
                                            input.input_source_data = alternative["input_source_data"]
                                        break
                                    else:
                                        raise Error(f"unknown input origin '{alternative['origin']}'")
                            if input is None and mandatory:
                                product_types = [alternative["file_type"] for alternative in alternatives]
                                expected = ""
                                if len(product_types) > 0:
                                    if len(product_types) > 1:
                                        expected = f" (expected one of {', '.join(product_types)})"
                                    else:
                                        expected = f" (expected {product_types[0]})"
                                raise Error(f"input {index} for {task.name} has not been assigned" + expected)
                        if input is not None:
                            for file in input.files:
                                if file.file_name:
                                    logger.info(f"assigning input '{file.file_name}' to task '{task.name}'")
                            task.inputs.append(input)
                            input_cache[input.file_type] = input
                            id = input_element.get("id")
                            if id is not None:
                                named_inputs[id] = input

                    for output_element in task_element.findall("List_of_Outputs/Output"):
                        file_type = output_element.findtext("Type")
                        if file_type is None:
                            file_type = output_element.findtext("File_Type")
                        assert file_type is not None
                        file_name_type = output_element.findtext("File_Name_Type")
                        destination = output_element.findtext("Destination")
                        mandatory = output_element.findtext("Mandatory") != "No"
                        filename = ""
                        if file_name_type == "Regexp":
                            if file_type not in job.config.product_types:
                                raise Error(f"product type '{file_type}' not defined in global config")
                            filename = job.config.product_types[file_type].match_expression
                            if filename is None:
                                raise Error(f"match expression for product type '{file_type}' missing in global config")
                        task.outputs.append(Output(file_type, file_name_type, filename, destination, mandatory))
                        if destination in ["DB", "DBPROC"]:
                            if file_type in self.outputs:
                                raise Error(f"output for '{file_type}' included more than once in the task table")
                            if file_type not in job.config.product_types:
                                raise Error(f"product type '{file_type}' not defined in global config")
                            self.outputs[file_type] = task.outputs[-1]
                    self.tasks.append(task)

        if len(predefined_inputs) > 0:
            product_types = list(predefined_inputs)
            if len(product_types) == 1:
                raise Error(f"input for '{product_types[0]}' from configuration could not be assigned to any tasks")
            else:
                product_type_list = ", ".join(f"'{product_type}'" for product_type in product_types)
                raise Error(f"inputs for " + product_type_list + " from configuration could not be assigned to any "
                            "tasks")

    def write_joborder(self, job: joborder.Job, dry_run: bool = False) -> Union[Path, bytes]:
        """
        Create the joborder file.
        If dry_run is True then nothing should be written to disk and the joborder file content should be returned as a
        byte string.
        If dry_run is False then write the joborder file into the job.working_directory directory and return the full
        path to the created joborder file.
        """
        if not dry_run:
            filepath = Path(job.working_directory, f"JobOrder.{job.config.joborder_id}.xml")
            logger.info(f"creating joborder file '{filepath}'")

        joborder = etree.Element("Ipf_Job_Order")

        conf = etree.SubElement(joborder, "Ipf_Conf")
        element = etree.SubElement(conf, "Processor_Name")
        element.text = job.config.processor_name
        element = etree.SubElement(conf, "Version")
        element.text = job.config.processor_version
        if job.config.order_type is not None:
            element = etree.SubElement(conf, "Order_Type")
            element.text = job.config.order_type
        if job.config.variant_split_logging_level:
            element = etree.SubElement(conf, "Stdout_Log_Level")
            element.text = job.config.log_level
            element = etree.SubElement(conf, "Stderr_Log_Level")
            element.text = job.config.log_level
        else:
            element = etree.SubElement(conf, "Logging_Level")
            element.text = job.config.log_level
        element = etree.SubElement(conf, "Test")
        element.text = "true" if self.enable_test else "false"
        if job.config.variant_use_troubleshooting:
            element = etree.SubElement(conf, "Troubleshooting")
            element.text = "true" if job.config.enable_breakpoints else "false"
        if job.config.variant_global_breakpoint_enable:
            element = etree.SubElement(conf, "Breakpoint_Enable")
            element.text = "true" if job.config.enable_breakpoints else "false"
        if job.config.acquisition_station is not None:
            element = etree.SubElement(conf, "Acquisition_Station")
            element.text = job.config.acquisition_station
        element = etree.SubElement(conf, "Processing_Station")
        element.text = job.config.processing_station
        files = etree.SubElement(conf, "Config_Files")
        if not self.variant_private_config_file:
            for config_file in self.config_files:
                element = etree.SubElement(files, "Conf_File_Name")
                element.text = config_file
        for config_space in self.config_spaces:
            element = etree.SubElement(files, config_space)
            element.text = self.config_spaces[config_space]
        if self.include_global_sensing_time:
            element = etree.SubElement(conf, "Sensing_Time")
            start_element = etree.SubElement(element, "Start")
            start_element.text = job.config.format_timestamp(self.sensing_start)
            stop_element = etree.SubElement(element, "Stop")
            stop_element.text = job.config.format_timestamp(self.sensing_stop, round_up=True)

        if self.variant_dynamic_processing_parameters:
            if job.config.variant_alternate_dynamic_processing_parameter_name:
                parameters_element = etree.SubElement(conf, "List_of_Dynamic_Processing_Parameters")
            else:
                parameters_element = etree.SubElement(conf, "Dynamic_Processing_Parameters")
        else:
            parameters_element = etree.SubElement(joborder, "Processing_Parameters")
        num_parameters = 0
        for name, value in self.parameters.items():
            if self.parameters[name] is not None:
                if job.config.variant_alternate_dynamic_processing_parameter_name:
                    parameter_element = etree.SubElement(parameters_element, "Dynamic_Processing_Parameter")
                else:
                    parameter_element = etree.SubElement(parameters_element, "Processing_Parameter")
                name_element = etree.SubElement(parameter_element, "Name")
                name_element.text = name
                value_element = etree.SubElement(parameter_element, "Value")
                value_element.text = value
                num_parameters += 1
        if not self.variant_dynamic_processing_parameters or \
                job.config.variant_alternate_dynamic_processing_parameter_name:
            parameters_element.set("count", str(num_parameters))

        procs = etree.SubElement(joborder, "List_of_Ipf_Procs")
        procs.set("count", str(len(job.tasks)))
        for task in self.tasks:
            proc = etree.SubElement(procs, "Ipf_Proc")
            element = etree.SubElement(proc, "Task_Name")
            element.text = task.name
            element = etree.SubElement(proc, "Task_Version")
            element.text = task.version
            breakpoint = etree.SubElement(proc, job.config.variant_breakpoint_element_name)
            if not job.config.variant_global_breakpoint_enable:
                element = etree.SubElement(breakpoint, "Enable")
                element.text = "OFF"
            element = etree.SubElement(breakpoint, "List_of_Brk_Files")
            element.set("count", "0")
            input_list = etree.SubElement(proc, "List_of_Inputs")
            input_list.set("count", str(len(task.inputs)))
            for input in task.inputs:
                input_element = etree.SubElement(input_list, "Input")
                element = etree.SubElement(input_element, "File_Type")
                element.text = input.file_type
                element = etree.SubElement(input_element, "File_Name_Type")
                element.text = input.file_name_type
                if input.input_source_data is not None:
                    element = etree.SubElement(input_element, "Input_Source_Data")
                    element.text = "true" if input.input_source_data else "false"
                file_list = etree.SubElement(input_element, "List_of_File_Names")
                file_list.set("count", str(len(input.files)))
                for file in input.files:
                    element = etree.SubElement(file_list, "File_Name")
                    element.text = str(Path(job.working_directory, file.file_name))
                    if input.file_name_type == "Directory" and element.text[-1] != "/":
                        element.text += "/"
                    elif input.file_name_type == "Physical" and input.file_type in job.config.product_types:
                        if job.config.product_types[input.file_type].stem_as_physical_dbl:
                            element.text += ".DBL"
                interval_list = etree.SubElement(input_element, "List_of_Time_Intervals")
                num_time_intervals = 0
                for file in input.files:
                    if (file.start_time is not None and file.stop_time is not None) or \
                            job.config.variant_always_include_input_time_interval:
                        interval = etree.SubElement(interval_list, "Time_Interval")
                        file_start = getattr(file, "start_time", datetime.datetime.min)
                        file_stop = getattr(file, "stop_time", datetime.datetime.max)
                        if job.config.variant_clip_input_time_interval_to_sensing_interval and \
                                intersects(self.sensing_start, self.sensing_stop, file_start, file_stop):
                            file_start, file_stop = intersection(self.sensing_start, self.sensing_stop, file_start,
                                                                 file_stop)
                        element = etree.SubElement(interval, "Start")
                        element.text = job.config.format_timestamp(file_start)
                        element = etree.SubElement(interval, "Stop")
                        element.text = job.config.format_timestamp(file_stop)
                        element = etree.SubElement(interval, "File_Name")
                        element.text = str(Path(job.working_directory, file.file_name))
                        if (input.file_name_type == "Directory" or len(file.file_name) == 0) and \
                                element.text[-1] != "/":
                            element.text += "/"
                        elif input.file_name_type == "Physical" and input.file_type in job.config.product_types:
                            if job.config.product_types[input.file_type].stem_as_physical_dbl:
                                element.text += ".DBL"
                        num_time_intervals += 1
                interval_list.set("count", str(num_time_intervals))
            output_list = etree.SubElement(proc, "List_of_Outputs")
            output_list.set("count", str(len(task.outputs)))
            for output in task.outputs:
                output_element = etree.SubElement(output_list, "Output")
                element = etree.SubElement(output_element, "File_Type")
                element.text = output.file_type
                element = etree.SubElement(output_element, "File_Name_Type")
                element.text = output.file_name_type
                element = etree.SubElement(output_element, "File_Name")
                element.text = str(Path(job.working_directory, output.file_name))
                if (output.file_name_type == "Directory" or len(output.file_name) == 0) and element.text[-1] != "/":
                    element.text += "/"

        if self.variant_private_config_file:
            conf = etree.SubElement(joborder, "Processor_Conf")
            if len(self.config_files) > 0:
                element = etree.SubElement(conf, "File_Name")
                element.text = self.config_files[0]

        tree = etree.ElementTree(joborder)

        if job.config.joborder_schema is not None:
            xmlschema = etree.XMLSchema(file=job.config.joborder_schema)
            etree.clear_error_log()
            try:
                xmlschema.assertValid(tree)
                logger.info(f"joborder valid according to schema '{job.config.joborder_schema}'")
            except etree.DocumentInvalid as exc:
                logger.error(f"could not verify joborder against schema '{job.config.joborder_schema}'")
                for error in exc.error_log:  # type: ignore
                    logger.error(f"{error.filename}:{error.line}: {error.message}")
                if not dry_run:
                    raise Error(f"invalid joborder file")

        if dry_run:
            return etree.tostring(tree, pretty_print=True, encoding="UTF-8", xml_declaration=True)

        tree.write(filepath, pretty_print=True, encoding="UTF-8", xml_declaration=True)
        return filepath

    def _scan_list_file_for_output_products(self, job: joborder.Job, path: Path):
        has_errors = False
        with open(path) as list_file:
            for index, line in enumerate(list_file, start=1):
                line = line.strip()
                files = []
                metadata_files = None

                # Find outputs corresponding to the current line from the LIST file.
                if job.config.variant_listfile_contains_stem:
                    # Line from the LIST file contains a stem that could match more than one file.
                    files = [Path(file) for file in glob.glob(f"{line}*")]

                    # Separate metadata (.MTD) file(s).
                    metadata_files = list(filter(lambda file: file.suffix == ".MTD", files))
                    files = [file for file in files if file not in metadata_files]

                    if len(files) == 0:
                        logger.error(f"[processor] {path.name}:{index}: stem does not match any (non-metadata) " +
                                     "files in working directory")
                        has_errors = True
                    if len(metadata_files) > 1:
                        logger.error(f"[processor] {path.name}:{index}: found more than one metadata file " +
                                     "matching stem in working directory")
                        has_errors = True
                    elif len(metadata_files) == 1:
                        metadata_file = metadata_files[0]
                else:
                    # Line from the LIST file refers to a file / directory directly.
                    file = Path(line)
                    if file.exists():
                        files = [file]
                        if Path(file.stem + ".MTD").exists():
                            metadata_file = Path(file.stem + ".MTD")
                    else:
                        logger.error(f"[processor] {path.name}:{index}: file not found in working directory")
                        has_errors = True

                # Update list of output products.
                product_type = None
                for name, product_type_config in job.config.product_types.items():
                    if product_type_config.match_expression is not None:
                        if re.match(product_type_config.match_expression, line) is not None:
                            product_type = name
                            break
                if product_type is None:
                    logger.error(f"[processor] {path.name}:{index}: cannot determine product type")
                    has_errors = True
                elif len(files) > 0:
                    if product_type not in self.outputs:
                        logger.error(f"[processor] {path.name}:{index}: unexpected product type '{product_type}'")
                        has_errors = True
                    else:
                        if product_type in job.outputs:
                            output = job.outputs[product_type]
                        else:
                            output = joborder.Output(product_type)
                            job.outputs[product_type] = output
                        output.products.append(joborder.OutputProduct(files, metadata_file))

        if has_errors:
            raise ProcessorError(f"{path.name}: LIST file contains errors")

    def _scan_workspace_for_output_products(self, job: joborder.Job):
        files = os.listdir(job.working_directory)  # type: List[str]
        for output in self.outputs.values():
            # Find all files that match the product type match expression.
            matched_files = []
            match_expression = job.config.product_types[output.file_type].match_expression
            if match_expression is not None:
                for file in files:
                    if re.match(match_expression, file) is not None and not file.endswith(".MTD"):
                        matched_files.append(file)
            if len(matched_files) > 0:
                if output.file_type in job.outputs:
                    job_output = job.outputs[output.file_type]
                else:
                    job_output = joborder.Output(output.file_type)
                    job.outputs[output.file_type] = job_output
                for file in matched_files:
                    metadata_file = Path(Path(file).stem + ".MTD")
                    if metadata_file.exists():
                        job_output.products.append(joborder.OutputProduct([Path(file)], metadata_file))
                    else:
                        job_output.products.append(joborder.OutputProduct([Path(file)]))

    def locate_and_check_outputs(self, job: joborder.Job) -> None:
        """
        Fully initialize job.outputs
        job.outputs should only have entries where destination is DB or DBPROC
        """
        logger.info("checking output products")

        # Locate output products.
        if job.config.variant_ignore_listfile:
            self._scan_workspace_for_output_products(job)
        else:
            list_files = list(job.working_directory.glob("*.LIST"))
            if len(list_files) > 1:
                raise ProcessorError("multiple LIST files found: " + ", ".join([str(file) for file in list_files]))
            if len(list_files) == 1:
                if job.config.variant_listfile_uses_order_id:
                    list_basename = list_files[0].name
                    expected_list_basename = job.config.joborder_id + ".LIST"
                    if list_basename != expected_list_basename:
                        raise ProcessorError(f"found LIST file with unexpected filename (found '{list_basename}', "
                                             f"expected '{expected_list_basename}')")
                self._scan_list_file_for_output_products(job, list_files[0])
            elif job.config.variant_listfile_mandatory:
                raise ProcessorError("LIST file not found")
            else:
                self._scan_workspace_for_output_products(job)

        # Group output products by stem
        for output in job.outputs.values():
            product_type_config = job.config.product_types.get(output.product_type)
            if product_type_config is not None and product_type_config.stem_expression is not None:
                groups = {}  # type: Dict[str, joborder.OutputProduct]
                for product in output.products:
                    for filepath in product.filepaths:
                        match_obj = re.match(product_type_config.stem_expression, filepath.name)
                        stem = "" if match_obj is None else match_obj.group()
                        if not stem:
                            raise Error(f"stem expression '{product_type_config.stem_expression}' of product type " +
                                        f"'{output.product_type}' returns empty stem when applied to filename " +
                                        f"'{filepath.name}'")
                        if stem in groups:
                            if groups[stem].metadata_filepath != product.metadata_filepath:
                                raise ProcessorError(f"inconsistent metadata file presence when combining outputs " +
                                                     f"for product type '{output.product_type}' with stem '{stem}'")
                            groups[stem].filepaths.append(filepath)
                        else:
                            groups[stem] = joborder.OutputProduct([filepath], product.metadata_filepath)
                output.products = list(groups.values())

        # Check whether metadata file presence is as expected
        for output in job.outputs.values():
            product_type_config = job.config.product_types.get(output.product_type)
            if product_type_config is not None:
                if product_type_config.has_metadata_file:
                    if any([product.metadata_filepath is None for product in output.products]):
                        logger.warning(f"[processor] missing metadata file for output '{output.product_type}'")
                else:
                    if any([product.metadata_filepath is not None for product in output.products]):
                        logger.warning(f"[processor] unexpected metadata file for output '{output.product_type}'")

        # Check outputs against task table.
        has_errors = False
        for product_type in self.outputs:
            if product_type not in job.outputs:
                if self.outputs[product_type].mandatory:
                    logger.error(f"[processor] no outputs for product type '{product_type}' found in working directory")
                    has_errors = True
            else:
                if len(job.outputs[product_type].products) > 1:
                    product_type_config = job.config.product_types[product_type]
                    if product_type_config is None or not product_type_config.has_multi_product_output:
                        logger.warning(f"[processor] product type '{product_type}' appears more than once in " +
                                       f"working directory ({len(job.outputs[product_type].products)} times)")
        if has_errors:
            raise ProcessorError("outputs produced by processor do not match task table")
