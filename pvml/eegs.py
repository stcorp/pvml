from collections import OrderedDict
import datetime
from dataclasses import dataclass, field
import glob
import logging
import os
from pathlib import Path
import re
import socket
from typing import List, Dict, Optional, Union

from lxml import etree

from .exceptions import Error, ProcessorError
from . import joborder

logger = logging.getLogger("pvml")


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
        processor_version = tree.findtext("Processor_Version")
        if processor_name is not None and processor_version is not None:
            if processor_name == config.processor_name and processor_version == config.processor_version:
                if tasktable is not None:
                    raise Error(f"multiple tasktables for {processor_name}/{processor_version}")
                tasktable = tree

    if tasktable is None:
        raise Error(f"no tasktable found for {config.processor_name}/{config.processor_version}")

    return tasktable


@dataclass
class SelectedInput:
    file_type: str
    file_names: List[str] = field(default_factory=list)
    internal_output: bool = False
    virtual: bool = False


@dataclass
class Input:
    input_id: str
    alternative_id: str
    selected_inputs: List[SelectedInput] = field(default_factory=list)


@dataclass
class Output:
    file_type: str
    file_name_pattern: str


@dataclass
class IntermediateOutput:
    output_id: str
    output_file: str


@dataclass
class Task:
    name: str
    version: str
    cpu_cores: int
    ram: int
    disk_space: int
    parameters: OrderedDict[str, str] = field(default_factory=OrderedDict)
    config_files: OrderedDict[str, str] = field(default_factory=OrderedDict)
    inputs: List[Input] = field(default_factory=list)
    outputs: List[Output] = field(default_factory=list)
    intermediate_outputs: List[IntermediateOutput] = field(default_factory=list)


@dataclass
class Backend:
    toi_start: Optional[datetime.datetime] = None
    toi_stop: Optional[datetime.datetime] = None
    outputs: Dict[str, Output] = field(default_factory=dict)
    tasks: List[Task] = field(default_factory=list)

    def initialize_job(self, job: joborder.Job) -> None:
        """
        Fully initialize job.tasks and job.inputs and resolve everything to be able to write the joborder file
        job.inputs should only have entries where origin is External
        This function should not write anything to disk (don't assume that job.working_directory is created yet)
        This function is allowed to use job.archive_module to resolve input references and find unassigned inputs
        """
        assert len(self.tasks) == 0  # don't allow double initialisation

        # read task table
        tree = read_tasktable(job.config)

        self.toi_start = job.config.sensing_start
        self.toi_stop = job.config.sensing_stop
        predefined_inputs = {}  # type: Dict[str, joborder.Input]
        selected_input_cache = {}  # type: Dict[str, SelectedInput]
        with job.archive_module.Archive(job.config) as archive:
            for config_input in job.config.inputs:
                if len(config_input.products) == 0:
                    continue
                input_products = []
                input_start_time = None
                input_stop_time = None
                for config_product in config_input.products:
                    product = archive.resolve_reference(config_product.reference, config_input.product_type)
                    input_product = joborder.InputProduct(product.reference, product.filename)
                    if product.validity_start is not None:
                        if input_start_time is None or product.validity_start < input_start_time:
                            input_start_time = product.validity_start
                    if product.validity_stop is not None:
                        if input_stop_time is None or product.validity_stop > input_stop_time:
                            input_stop_time = product.validity_stop
                    input_products.append(input_product)
                predefined_inputs[config_input.product_type] = joborder.Input(config_input.product_type, input_products)
                if self.toi_start is None and input_start_time is not None:
                    self.toi_start = input_start_time
                if self.toi_stop is None and input_stop_time is not None:
                    self.toi_stop = input_stop_time

            for task_element in tree.findall("List_of_Tasks/Task"):
                name = task_element.findtext("Task_Name")
                assert name is not None
                version = task_element.findtext("Task_Version")
                assert version is not None
                executable = task_element.findtext("Executable")
                assert executable is not None
                value = task_element.findtext("CPU_Cores/Number")
                assert value is not None
                cpu_cores = int(value)
                value = task_element.findtext("RAM/Amount")
                assert value is not None
                ram = int(value)
                value = task_element.findtext("Disk_Space")
                assert value is not None
                disk_space = int(value)
                job_task = joborder.Task(name, version, executable)
                job.tasks.append(job_task)

                task = Task(name, version, cpu_cores, ram, disk_space)

                for element in task_element.findall("List_of_Proc_Parameters/Proc_Parameter"):
                    name = element.findtext("Name")
                    assert name is not None
                    if name in job.config.processing_parameters:
                        task.parameters[name] = job.config.processing_parameters[name]
                    else:
                        default_value = element.findtext("Default_Value")
                        assert default_value is not None
                        task.parameters[name] = default_value

                for element in task_element.findall("List_of_Cfg_Files/Cfg_File"):
                    cfg_id = element.findtext("Cfg_ID")
                    assert cfg_id is not None
                    filename = element.findtext("Cfg_File_Name")
                    assert filename is not None
                    task.config_files[cfg_id] = filename

                for input_element in task_element.findall("List_of_Inputs/Input"):
                    input_id = input_element.findtext("Input_ID")
                    assert input_id is not None
                    value = input_element.findtext("Mandatory")
                    assert value is not None
                    mandatory = value == "Yes"
                    input = None
                    for alternative_element in input_element.findall("List_of_Input_Alternatives/Input_Alternative"):
                        alternative_id = alternative_element.findtext("Alternative_ID")
                        assert alternative_id is not None
                        alternative_inputs = []  # type: List[SelectedInput]
                        alternative_job_inputs = []  # type: List[joborder.Input]
                        complete = True
                        for alternative_type in \
                                alternative_element.findall("List_of_Alternative_Types/Alternative_Type"):
                            file_type = alternative_type.findtext("File_Type")
                            assert file_type is not None
                            origin = alternative_type.findtext("Origin")
                            assert origin is not None
                            instances = alternative_type.findtext("Instances")
                            assert instances is not None
                            if instances != "SINGLE":
                                raise NotImplementedError(f"suport for Instances='{instances}' for task input not " +
                                                          "supported")
                            virtual = False
                            value = alternative_type.findtext("Virtual")
                            if value is not None:
                                virtual = value == "Yes"
                            if origin == "EXTERNAL":
                                if file_type in selected_input_cache:
                                    alternative_inputs.append(selected_input_cache[file_type])
                                    alternative_job_inputs.append(job.inputs[file_type])
                                elif file_type in predefined_inputs:
                                    predefined_input = predefined_inputs[file_type]
                                    selected_input = SelectedInput(file_type)
                                    for input_product in predefined_input.products:
                                        selected_input.file_names.append(input_product.filename)
                                    alternative_inputs.append(selected_input)
                                    alternative_job_inputs.append(predefined_input)
                                else:
                                    # TODO: Implement support for resolving inputs based on Selection_Parameters
                                    complete = False
                                    break
                            else:
                                # origin = "Task_Name/Task_Version"
                                input_task_name, input_task_version = origin.split('/')
                                internal_output = None
                                for input_task in self.tasks:
                                    if input_task.name == input_task_name and input_task.version == input_task_version:
                                        for output in input_task.outputs:
                                            if output.file_type == file_type:
                                                internal_output = output
                                                break
                                if internal_output is None:
                                    logger.warning(f"could not find output of type '{file_type}' from task {origin} " +
                                                   f"to be used as internal input '{input_id}' for task '{task.name}'")
                                    complete = False
                                    break
                                selected_input = SelectedInput(file_type)
                                selected_input.file_names.append(internal_output.file_name_pattern)
                                selected_input.internal_output = True
                                if virtual:
                                    selected_input.virtual = True
                                alternative_inputs.append(selected_input)
                        if complete:
                            logger.info(f"selecting alternative '{alternative_id}' for input '{input_id}' of task " +
                                        f"'{task.name}'")
                            for job_input in alternative_job_inputs:
                                if job_input.product_type not in job.inputs:
                                    job.inputs[job_input.product_type] = job_input
                                if job_input.product_type in predefined_inputs:
                                    del predefined_inputs[job_input.product_type]
                            input = Input(input_id, alternative_id)
                            for selected_input in alternative_inputs:
                                input.selected_inputs.append(selected_input)
                                if selected_input.file_type not in selected_input_cache:
                                    selected_input_cache[selected_input.file_type] = selected_input
                                if selected_input.virtual and selected_input.file_type in job.inputs:
                                    # remove virtual inputs from the joborder to prevent them from getting retrieved
                                    del job.inputs[selected_input.file_type]
                            break
                    if input is None:
                        if mandatory:
                            raise Error(f"could not find input alternative for input '{input_id}'")
                    else:
                        for selected_input in input.selected_inputs:
                            if not selected_input.internal_output:
                                for file_name in selected_input.file_names:
                                    logger.info(f"assigning input file '{file_name}' to input '{input_id}' of task " +
                                                f"'{task.name}'")
                        task.inputs.append(input)

                for output_element in task_element.findall("List_of_Outputs/Output"):
                    file_type = output_element.findtext("File_Type")
                    assert file_type is not None
                    file_name_pattern = output_element.findtext("File_Name_Pattern")
                    task.outputs.append(Output(file_type, file_name_pattern))
                    destination = output_element.findtext("Destination")
                    if destination == "External":
                        if file_type in self.outputs:
                            raise Error(f"output for '{file_type}' included more than once in the task table")
                        if file_type not in job.config.product_types:
                            raise Error(f"product type '{file_type}' not defined in global config")
                        self.outputs[file_type] = task.outputs[-1]

                for output_element in task_element.findall("List_of_Intermediate_Outputs/Intermediate_Output"):
                    output_id = output_element.findtext("Intermediate_Output_ID")
                    assert output_id is not None
                    output_file = output_element.findtext("Intermediate_Output_File")
                    assert output_file is not None
                    task.intermediate_outputs.append(IntermediateOutput(output_id, output_file))

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
            logger.info(f"creating JobOrder file '{filepath}'")

        attributes = {
            "schemaName": job.config.joborder_schema_name,
            "schemaVersion": job.config.joborder_schema_version,
        }
        joborder = etree.Element("Job_Order", attrib=attributes)

        conf = etree.SubElement(joborder, "Processor_Configuration")
        element = etree.SubElement(conf, "File_Class")
        element.text = job.config.file_class
        element = etree.SubElement(conf, "Processor_Name")
        element.text = job.config.processor_name
        element = etree.SubElement(conf, "Processor_Version")
        element.text = job.config.processor_version
        element = etree.SubElement(conf, "Processing_Node")
        if job.config.processing_node is not None:
            element.text = job.config.processing_node
        else:
            element.text = socket.gethostname()
        stdout_levels = etree.SubElement(conf, "List_of_Stdout_Log_Levels")
        stderr_levels = etree.SubElement(conf, "List_of_Stderr_Log_Levels")
        log_levels = ["DEBUG", "INFO", "PROGRESS", "WARNING", "ERROR"]
        for log_level in log_levels[log_levels.index(job.config.log_level):]:
            element = etree.SubElement(stdout_levels, "Stdout_Log_Level")
            element.text = log_level
            element = etree.SubElement(stderr_levels, "Stderr_Log_Level")
            element.text = log_level
        element = etree.SubElement(conf, "Intermediate_Output_Enable")
        element.text = "true" if job.config.enable_breakpoints else "false"
        element = etree.SubElement(conf, "Processing_Station")
        element.text = job.config.processing_station
        request = etree.SubElement(conf, "Request")
        if self.toi_start is not None and self.toi_stop is not None:
            toi = etree.SubElement(request, "TOI")
            element = etree.SubElement(toi, "Start")
            element.text = self.toi_start.strftime("%Y-%m-%dT%H:%M:%S.%f")
            element = etree.SubElement(toi, "Stop")
            element.text = self.toi_stop.strftime("%Y-%m-%dT%H:%M:%S.%f")
        tasks_element = etree.SubElement(joborder, "List_of_Tasks")
        for task in self.tasks:
            task_element = etree.SubElement(tasks_element, "Task")
            element = etree.SubElement(task_element, "Task_Name")
            element.text = task.name
            element = etree.SubElement(task_element, "Task_Version")
            element.text = task.version
            element = etree.SubElement(task_element, "Number_of_CPU_Cores")
            element.text = str(task.cpu_cores)
            element = etree.SubElement(task_element, "Amount_of_RAM")
            element.text = str(task.ram)
            element = etree.SubElement(task_element, "Disk_Space")
            element.text = str(task.disk_space)
            parameters_element = etree.SubElement(task_element, "List_of_Proc_Parameters")
            for name, value in task.parameters.items():
                parameter_element = etree.SubElement(parameters_element, "Proc_Parameter")
                name_element = etree.SubElement(parameter_element, "Name")
                name_element.text = name
                value_element = etree.SubElement(parameter_element, "Value")
                value_element.text = value
            cfg_files = etree.SubElement(task_element, "List_of_Cfg_Files")
            for config_id, config_file_name in task.config_files.items():
                cfg_element = etree.SubElement(cfg_files, "Cfg_File")
                element = etree.SubElement(cfg_element, "Cfg_ID")
                element.text = config_id
                element = etree.SubElement(cfg_element, "Cfg_File_Name")
                element.text = config_file_name
            input_list = etree.SubElement(task_element, "List_of_Inputs")
            for input in task.inputs:
                input_element = etree.SubElement(input_list, "Input")
                element = etree.SubElement(input_element, "Input_ID")
                element.text = input.input_id
                element = etree.SubElement(input_element, "Alternative_ID")
                element.text = input.alternative_id
                selected_input_list = etree.SubElement(input_element, "List_of_Selected_Inputs")
                for selected_input in input.selected_inputs:
                    selected_input_element = etree.SubElement(selected_input_list, "Selected_Input")
                    element = etree.SubElement(selected_input_element, "File_Type")
                    element.text = selected_input.file_type
                    file_list = etree.SubElement(selected_input_element, "List_of_File_Names")
                    for file_name in selected_input.file_names:
                        element = etree.SubElement(file_list, "File_Name")
                        element.text = str(Path(job.working_directory, file_name))
            output_list = etree.SubElement(task_element, "List_of_Outputs")
            for output in task.outputs:
                output_element = etree.SubElement(output_list, "Output")
                element = etree.SubElement(output_element, "File_Type")
                element.text = output.file_type
                element = etree.SubElement(output_element, "File_Name_Pattern")
                element.text = output.file_name_pattern
                element = etree.SubElement(output_element, "File_Dir")
                element.text = str(job.working_directory)
                element = etree.SubElement(output_element, "Baseline")
                if output.file_type in job.config.product_types:
                    element.text = job.config.product_types[output.file_type].baseline
            intermediate_output_list = etree.SubElement(task_element, "List_of_Intermediate_Outputs")
            for intermediate_output in task.intermediate_outputs:
                intermediate_output_element = etree.SubElement(intermediate_output_list, "Intermediate_Output")
                element = etree.SubElement(output_element, "Intermediate_Output_ID")
                element.text = intermediate_output.output_id
                element = etree.SubElement(output_element, "Intermediate_Output_File")
                element.text = intermediate_output.output_file

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

    def locate_and_check_outputs(self, job: joborder.Job) -> None:
        """
        Fully initialize job.outputs
        job.outputs should only have entries where destination is External
        """
        logger.info("checking output products")

        # Locate output products.
        has_errors = False
        if job.config.variant_regex_output_pattern:
            files = os.listdir(job.working_directory)  # type: List[str]
        for file_type, output in self.outputs.items():
            if job.config.variant_regex_output_pattern:
                matched_files = []
                for file in files:
                    if re.match(output.file_name_pattern, file) is not None:
                        matched_files.append(file)
            else:
                matched_files = glob.glob(os.path.join(job.working_directory, output.file_name_pattern))
            if len(matched_files) == 0:
                logger.error(f"[processor] no outputs for product type '{file_type}' found in working directory")
                has_errors = True
            else:
                assert file_type not in job.outputs
                job_output = joborder.Output(file_type)
                for file in matched_files:
                    job_output.products.append(joborder.OutputProduct([Path(file)]))
                job.outputs[file_type] = job_output
        if has_errors:
            raise ProcessorError("outputs produced by processor do not match task table")
