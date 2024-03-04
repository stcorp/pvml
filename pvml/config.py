import datetime
from dataclasses import dataclass, field
from importlib import resources
import logging
import math
from pathlib import Path
import sys
from urllib.error import URLError
from urllib.request import urlopen
from urllib.parse import urlparse
from typing import List, Optional, Union, Dict, Any

from lxml import etree

from .exceptions import Error

CONFIG_FILE_DATETIME_FORMATS = ["YYYY-MM-DDThh:mm:ss.uuuuuu", "YYYY-MM-DDThh:mm:ss"]

logger = logging.getLogger("pvml")


@dataclass
class ProductTypeConfig:
    match_expression: Optional[str] = None

    # MMFI specific
    start_time_expression: Optional[str] = None
    start_time_format: Optional[str] = None
    stop_time_expression: Optional[str] = None
    stop_time_format: Optional[str] = None
    stem_expression: Optional[str] = None
    stem_as_physical_dbl: bool = False
    has_metadata_file: bool = False
    has_multi_product_output: bool = False

    # EEGS specific
    baseline: str = "01"


@dataclass
class InputProduct:
    reference: str  # product reference specific to the archive backend

    # MMFI specific
    start_time: Optional[datetime.datetime] = None
    stop_time: Optional[datetime.datetime] = None


@dataclass
class Input:
    product_type: str
    products: List[InputProduct] = field(default_factory=list)


@dataclass
class Config:
    # Global (common)
    global_config_file: Optional[Path] = None
    interface_backend: str = "MMFI"
    archive_backend: str = "pvml.local"
    archive_options: Any = None
    tasktable_path: List[Path] = field(default_factory=list)
    tasktable_schema: Optional[Path] = None
    joborder_schema: Optional[Path] = None
    workspace_directory: Path = Path(".")  # root directory in which working directories will be created
    task_wrapper: Optional[str] = None
    product_types: Dict[str, ProductTypeConfig] = field(default_factory=dict)  # maps product type to its config

    # Global (MFFI)
    acquisition_station: Optional[str] = None
    processing_station: str = ""
    config_spaces: Dict[str, str] = field(default_factory=dict)  # maps config space name to path
    variant_use_numerical_order_id: bool = True
    variant_split_logging_level: bool = True
    variant_global_breakpoint_enable: bool = True
    variant_sensing_time_flag: bool = False
    variant_breakpoint_element_name: str = "BreakPoint"
    variant_alternate_dynamic_processing_parameter_name: bool = False
    variant_always_include_input_time_interval: bool = False
    variant_clip_input_time_interval_to_sensing_interval: bool = False
    variant_use_troubleshooting: bool = False
    variant_joborder_timeformat: str = "YYYYMMDD_hhmmssuuuuuu"
    variant_min_time_pattern: Optional[str] = None
    variant_max_time_pattern: Optional[str] = None
    variant_ignore_listfile: bool = False
    variant_listfile_mandatory: bool = False
    variant_listfile_uses_order_id: bool = True
    variant_listfile_contains_stem: bool = False

    # Global (EEGS)
    joborder_schema_name: str = "JobOrder"
    joborder_schema_version: str = "1.0"
    file_class: str = ""
    variant_regex_output_pattern: bool = False

    # Job specific (common)
    job_config_file: Optional[Path] = None
    joborder_id: str = "0"
    processor_name: Optional[str] = None
    processor_version: Optional[str] = None
    tasktable_url: Optional[str] = None
    working_directory: Optional[Path] = None  # override of location of the working directory for this job
    log_level: str = "INFO"
    enable_breakpoints: bool = False
    sensing_start: Optional[datetime.datetime] = None
    sensing_stop: Optional[datetime.datetime] = None
    processing_parameters: Dict[str, str] = field(default_factory=dict)  # maps parameter name to its value
    exit_codes: Dict[str, List[int]] = field(default_factory=dict)  # maps task name to list of allowed exit codes
    inputs: List[Input] = field(default_factory=list)

    # Job specific (MMFI)
    mode: Optional[str] = None
    test: bool = False
    order_type: Optional[str] = None

    # Job specific (EEGS)
    processing_node: Optional[str] = None

    def parse_timestamp(self, timestamp, formats):
        # Turn a time string into a datetime value using any of the given formats for parsing
        # Supported formats are: "YYYY-MM-DDThh:mm:ss.uuuuuu", "YYYY-MM-DDThh:mm:ss", "YYYYMMDDThhmmss",
        #     "YYYYMMDD_hhmmssuuuuuu", "YYYYMMDD_hhmmssuuu", "YYYYMMDD_hhmmss.uuu", "YYYYMMDD_hhmmss"
        # minpattern/maxpattern are values in "YYYY-MM-DDThh:mm:ss.uuu" format that will map to datetime.min and
        # and datetime.max respectively.
        for format in formats:
            if self.variant_min_time_pattern is not None:
                p = self.variant_min_time_pattern
                min_value = format.replace("YYYY", p[0:4]).replace("MM", p[5:7]).replace("DD", p[8:10])
                min_value = min_value.replace("hh", p[11:13]).replace("mm", p[14:16])
                min_value = min_value.replace("ss", p[17:19]).replace("uuu", p[20:23])
                if timestamp == min_value:
                    return datetime.datetime.min
            if self.variant_max_time_pattern is not None:
                p = self.variant_max_time_pattern
                max_value = format.replace("YYYY", p[0:4]).replace("MM", p[5:7]).replace("DD", p[8:10])
                max_value = max_value.replace("hh", p[11:13]).replace("mm", p[14:16])
                max_value = max_value.replace("ss", p[17:19]).replace("uuu", p[20:23])
                if timestamp == max_value:
                    return datetime.datetime.max
            try:
                if format == "YYYY-MM-DDThh:mm:ss.uuuuuu":
                    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                if format == "YYYY-MM-DDThh:mm:ss":
                    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
                if format == "YYYYMMDDThhmmss":
                    return datetime.datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
                if format == "YYYYMMDD_hhmmssuuuuuu":
                    if len(timestamp) != 21:
                        raise ValueError()
                    return datetime.datetime.strptime(timestamp, "%Y%m%d_%H%M%S%f")
                if format == "YYYYMMDD_hhmmssuuu":
                    if len(timestamp) != 18:
                        raise ValueError()
                    return datetime.datetime.strptime(timestamp, "%Y%m%d_%H%M%S%f")
                if format == "YYYYMMDD_hhmmss.uuu":
                    if len(timestamp) != 19:
                        raise ValueError()
                    return datetime.datetime.strptime(timestamp, "%Y%m%d_%H%M%S.%f")
                if format == "YYYYMMDD_hhmmss":
                    if len(timestamp) != 15:
                        raise ValueError()
                    return datetime.datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
            except Exception:
                pass
            else:
                raise ValueError("unsupported datetime format '%s'" % (format,))
        raise ValueError("datetime value '%s' does not match format '%s'" % (timestamp, "|".join(formats)))

    def format_timestamp(self, timestamp, round_up=False):
        # Turn a datetime value into a string using the given format
        # If the format does not use microsecond resolution then by default values are rounded down.
        # However, if round_up is set to True, values are rounded up (which is useful for e.g. end times of ranges)
        # Supported formats are: "YYYY-MM-DDThh:mm:ss.uuuuuu", "YYYY-MM-DDThh:mm:ss", "YYYYMMDDThhmmss",
        #     "YYYYMMDD_hhmmssuuuuuu", "YYYYMMDD_hhmmssuuu", "YYYYMMDD_hhmmss.uuu", "YYYYMMDD_hhmmss"
        format = self.variant_joborder_timeformat
        minpattern = self.variant_min_time_pattern
        maxpattern = self.variant_max_time_pattern
        if format == "YYYY-MM-DDThh:mm:ss.uuuuuu":
            if minpattern and timestamp == datetime.datetime.min:
                return minpattern + minpattern[20:23]
            if maxpattern and timestamp == datetime.datetime.max:
                return maxpattern + maxpattern[20:23]
            return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")
        if format == "YYYY-MM-DDThh:mm:ss":
            if minpattern and timestamp == datetime.datetime.min:
                return minpattern[0:19]
            if maxpattern and timestamp == datetime.datetime.max:
                return maxpattern[0:19]
            return timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        if format == "YYYYMMDD_hhmmssuuuuuu":
            if minpattern and timestamp == datetime.datetime.min:
                p = minpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19] + p[20:23] + p[20:23]
            if maxpattern and timestamp == datetime.datetime.max:
                p = maxpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19] + p[20:23] + p[20:23]
            return timestamp.strftime("%Y%m%d_%H%M%S%f")
        if format == "YYYYMMDD_hhmmssuuu":
            if minpattern and timestamp == datetime.datetime.min:
                p = minpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19]
            if maxpattern and timestamp == datetime.datetime.max:
                p = maxpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19]
            if round_up:
                milliseconds = math.ceil(timestamp.microsecond / 1e3)
            else:
                milliseconds = math.floor(timestamp.microsecond / 1e3)
            timestamp = timestamp.replace(microsecond=milliseconds * 1e3)
            return timestamp.strftime("%Y%m%d_%H%M%S%f")[:-3]
        if format == "YYYYMMDD_hhmmss.uuu":
            if minpattern and timestamp == datetime.datetime.min:
                p = minpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19] + '.' + p[20:23]
            if maxpattern and timestamp == datetime.datetime.max:
                p = maxpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19] + '.' + p[20:23]
            if round_up:
                milliseconds = math.ceil(timestamp.microsecond / 1e3)
            else:
                milliseconds = math.floor(timestamp.microsecond / 1e3)
            timestamp = timestamp.replace(microsecond=int(milliseconds * 1e3))
            return timestamp.strftime("%Y%m%d_%H%M%S.%f")[:-3]
        if format == "YYYYMMDD_hhmmss":
            if minpattern and timestamp == datetime.datetime.min:
                p = minpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19]
            if maxpattern and timestamp == datetime.datetime.max:
                p = maxpattern
                return p[0:4] + p[5:7] + p[8:10] + '_' + p[11:13] + p[14:16] + p[17:19]
            return timestamp.strftime("%Y%m%d_%H%M%S")
        raise ValueError("unsupported datetime format '%s'" % format)

    def read_global_config(self, config_file: Union[Path, str]):
        tree = None
        if isinstance(config_file, str):
            if urlparse(config_file).scheme != '':
                try:
                    with urlopen(config_file) as response:
                        tree = etree.fromstring(response.read())
                except URLError as e:
                    raise Error(f"failed to retrieve global config file {config_file} ({str(e)})")
            else:
                config_file = Path(config_file)
        if tree is None:
            assert type(config_file) == Path
            config_file = config_file.resolve()
            tree = etree.parse(config_file)
            self.job_config_file = config_file

        with resources.as_file(resources.files(__package__).joinpath("xsd/global_config.xsd")) as resource:
            schema = resource.read_text()
        xmlschema = etree.XMLSchema(etree.fromstring(schema.encode("utf8")))
        etree.clear_error_log()
        try:
            xmlschema.assertValid(tree)
        except etree.DocumentInvalid as exc:
            logger.error(f"could not verify '{config_file}' against schema")
            for error in exc.error_log:  # type: ignore
                logger.error(f"{error.filename}:{error.line}: {error.message}")
            raise Error("invalid PVML global configuration file")
        logger.debug(f"file '{config_file}' valid according to internal schema")

        value = tree.findtext("interfaceBackend")
        if value is not None:
            if value not in ["MMFI", "EEGS"]:
                raise Error(f"configuration error (invalid interface backend '{value}')")
            self.interface_backend = value
        value = tree.findtext("taskTablePath")
        if value is not None:
            self.tasktable_path = []
            for component in value.split(":"):
                path = Path(component)
                if not path.is_absolute():
                    if self.job_config_file is None:
                        raise Error(f"tasktable path '{component}' relative to '{str(config_file)}' not supported")
                    path = Path(self.job_config_file.parent, path)
                self.tasktable_path.append(path)
        value = tree.findtext("workspaceDirectory")
        if value is not None:
            self.workspace_directory = Path(value)
            if not self.workspace_directory.is_absolute():
                if self.job_config_file is None:
                    raise Error(f"workspace directory '{value}' relative to '{str(config_file)}' not supported")
                self.workspace_directory = Path(self.job_config_file.parent, self.workspace_directory)
        value = tree.findtext("taskTableSchema")
        if value is not None:
            self.tasktable_schema = Path(value)
            if not self.tasktable_schema.is_absolute():
                if self.job_config_file is None:
                    raise Error(f"tasktable schema '{value}' relative to '{str(config_file)}' not supported")
                self.tasktable_schema = Path(self.job_config_file.parent, self.tasktable_schema)
        value = tree.findtext("jobOrderSchema")
        if value is not None:
            self.joborder_schema = Path(value)
            if not self.joborder_schema.is_absolute():
                if self.job_config_file is None:
                    raise Error(f"joborder schema '{value}' relative to '{str(config_file)}' not supported")
                self.joborder_schema = Path(self.job_config_file.parent, self.joborder_schema)
        value = tree.findtext("taskWrapper")
        if value is not None:
            self.task_wrapper = value
        value = tree.findtext("acquisitionStation")
        if value is not None:
            self.acquisition_station = value
        value = tree.findtext("processingStation")
        if value is not None:
            self.processing_station = value
        for product_type_element in tree.findall("productTypes/productType"):
            product_type = ProductTypeConfig()
            name = product_type_element.get("name")
            assert name is not None
            product_type.match_expression = product_type_element.findtext("matchExpression")
            element = product_type_element.find("startTimeExpression")
            if element is not None:
                product_type.start_time_expression = element.text
                product_type.start_time_format = element.get("format")
            element = product_type_element.find("stopTimeExpression")
            if element is not None:
                product_type.stop_time_expression = element.text
                product_type.stop_time_format = element.get("format")
            product_type.stem_expression = product_type_element.findtext("stemExpression")
            value = product_type_element.findtext("stemAsPhysicalDBL")
            if element is not None:
                product_type.stem_as_physical_dbl = value in ["true", "1"]
            value = product_type_element.findtext("hasMetadataFile")
            if value is not None:
                product_type.has_metadata_file = value in ["true", "1"]
            value = product_type_element.findtext("hasMultiProductOutput")
            if value is not None:
                product_type.has_multi_product_output = value in ["true", "1"]
            value = product_type_element.findtext("baseline")
            if value is not None:
                product_type.baseline = value
            self.product_types[name] = product_type

        # MMFI specific
        value = tree.findtext("splitLoggingLevel")
        if value in ["false", "0"]:
            self.variant_split_logging_level = False
        value = tree.findtext("globalBreakpointEnable")
        if value in ["false", "0"]:
            self.variant_global_breakpoint_enable = False
        value = tree.findtext("sensingTimeFlag")
        if value in ["true", "1"]:
            self.variant_sensing_time_flag = True
        value = tree.findtext("alternateBreakpointElementName")
        if value in ["true", "1"]:
            self.variant_breakpoint_element_name = "Breakpoint"
        value = tree.findtext("alternateDynamicProcessingParameterName")
        if value in ["true", "1"]:
            self.variant_alternate_dynamic_processing_parameter_name = True
        value = tree.findtext("alwaysIncludeInputTimeInterval")
        if value in ["true", "1"]:
            self.variant_always_include_input_time_interval = True
        value = tree.findtext("clipInputTimeIntervalToSensingInterval")
        if value in ["true", "1"]:
            self.variant_clip_input_time_interval_to_sensing_interval = True
        value = tree.findtext("useTroubleshooting")
        if value in ["true", "1"]:
            self.variant_use_troubleshooting = True
        value = tree.findtext("jobOrderTimeFormat")
        if value is not None:
            self.variant_joborder_timeformat = value
        value = tree.findtext("minTimeValue")
        if value is not None:
            self.variant_min_time_pattern = value
        value = tree.findtext("maxTimeValue")
        if value is not None:
            self.variant_max_time_pattern = value
        value = tree.findtext("numericalOrderId")
        if value in ["false", "0"]:
            self.variant_use_numerical_order_id = False
        value = tree.findtext("ignoreListFile")
        if value in ["true", "1"]:
            self.variant_ignore_listfile = True
        value = tree.findtext("listFileMandatory")
        if value in ["true", "1"]:
            self.variant_listfile_mandatory = True
        value = tree.findtext("listFilenameUsesOrderId")
        if value in ["false", "0"]:
            self.variant_listfile_uses_order_id = False
        value = tree.findtext("listFileContainsStem")
        if value in ["true", "1"]:
            self.variant_listfile_contains_stem = True

        # EEGS specific
        value = tree.findtext("jobOrderSchemaName")
        if value is not None:
            self.joborder_schema_name = value
        value = tree.findtext("jobOrderSchemaVersion")
        if value is not None:
            self.joborder_schema_version = value
        value = tree.findtext("fileClass")
        if value is not None:
            self.file_class = value
        value = tree.findtext("useRegexOutputPattern")
        if value in ["true", "1"]:
            self.variant_regex_output_pattern = True

        value = tree.findtext("archiveBackend")
        if value is not None:
            self.archive_backend = value
        element = tree.find("archiveOptions")
        if element is not None:
            try:
                __import__(self.archive_backend)
                archive = sys.modules[self.archive_backend]
            except ImportError:
                raise Error(f"import of extension module '{self.archive_backend}' failed")
            self.archive_options = archive.parse_config(self, element)

    def read_job_config(self, config_file: Union[Path, str]):
        tree = None
        if isinstance(config_file, str):
            if urlparse(config_file).scheme != '':
                try:
                    with urlopen(config_file) as response:
                        tree = etree.fromstring(response.read())
                except URLError as e:
                    raise Error(f"failed to retrieve job config file {config_file} ({str(e)})")
            else:
                config_file = Path(config_file)
        if tree is None:
            assert type(config_file) == Path
            config_file = config_file.resolve()
            tree = etree.parse(config_file)
            self.job_config_file = config_file

        with resources.as_file(resources.files(__package__).joinpath("xsd/job_config.xsd")) as resource:
            schema = resource.read_text()
        xmlschema = etree.XMLSchema(etree.fromstring(schema.encode("utf8")))
        etree.clear_error_log()
        try:
            xmlschema.assertValid(tree)
        except etree.DocumentInvalid as exc:
            logger.error(f"could not verify '{config_file}' against schema")
            for error in exc.error_log:  # type: ignore
                logger.error(f"{error.filename}:{error.line}: {error.message}")
            raise Error("invalid PVML job configuration file")
        logger.debug(f"file '{config_file}' valid according to internal schema")

        self.processor_name = tree.findtext("processorName")
        self.processor_version = tree.findtext("processorVersion")
        self.tasktable_url = tree.findtext("taskTableUrl")
        if self.tasktable_url is None:
            if self.processor_name is None and self.processor_version is None:
                raise Error("job config file should contain processorName/processorVersion and/or taskTableUrl")
        if (self.processor_name is None and self.processor_version is not None):
            raise Error("processorName should be provided if processorVersion is present")
        if (self.processor_version is None and self.processor_name is not None):
            raise Error("processorVersion should be provided if processorName is present")
        value = tree.findtext("jobOrderId")
        assert value is not None
        self.joborder_id = value
        value = tree.findtext("workingDirectory")
        if value is not None:
            self.working_directory = Path(value)
        value = tree.findtext("processingNode")
        if value is not None:
            self.processing_node = value
        value = tree.findtext("mode")
        if value is not None:
            self.mode = value
        value = tree.findtext("fileClass")
        if value is not None:
            self.file_class = value
        value = tree.findtext("loggingLevel")
        if value is not None:
            self.log_level = value
        value = tree.findtext("enableBreakpoints")
        if value is not None:
            self.enable_breakpoints = value in ["true", "1"]
        value = tree.findtext("test")
        if value is not None:
            self.test = value in ["true", "1"]
        value = tree.findtext("acquisitionStation")
        if value is not None:
            self.acquisition_station = value
        value = tree.findtext("processingStation")
        if value is not None:
            self.processing_station = value
        value = tree.findtext("orderType")
        if value is not None:
            self.order_type = value
        value = tree.findtext("sensingStart")
        if value is not None:
            self.sensing_start = self.parse_timestamp(value, CONFIG_FILE_DATETIME_FORMATS)
        value = tree.findtext("sensingStop")
        if value is not None:
            self.sensing_stop = self.parse_timestamp(value, CONFIG_FILE_DATETIME_FORMATS)

        element = tree.find("processingParameters")
        if element is not None:
            elements = element.findall("parameter")
            for element in elements:
                name = element.get("name")
                assert name is not None and element.text is not None
                self.processing_parameters[name] = element.text

        element = tree.find("configSpaces")
        if element is not None:
            elements = element.findall("configSpace")
            for element in elements:
                name = element.get("name")
                assert name is not None and element.text is not None
                self.config_spaces[name] = element.text

        for input_element in tree.findall("inputs/input"):
            product_type = input_element.get("product_type")
            assert product_type is not None
            input = Input(product_type)
            for product_element in input_element.findall("product"):
                assert product_element.text is not None
                input_product = InputProduct(product_element.text)
                start_time = product_element.get("start")
                if start_time is not None:
                    input_product.start_time = self.parse_timestamp(start_time, CONFIG_FILE_DATETIME_FORMATS)
                stop_time = product_element.get("stop")
                if stop_time is not None:
                    input_product.stop_time = self.parse_timestamp(stop_time, CONFIG_FILE_DATETIME_FORMATS)
                input.products.append(input_product)
            self.inputs.append(input)

        for element in tree.findall("exitCodes"):
            task = element.get("task")
            assert task is not None
            if element.text is None or not element.text.strip():
                raise Error(f"list of expected exit codes is empty for task {task}")
            self.exit_codes[task] = [int(exit_code) for exit_code in element.text.split()]

        element = tree.find("archiveOptions")
        if element is not None:
            try:
                __import__(self.archive_backend)
                archive = sys.modules[self.archive_backend]
            except ImportError:
                raise Error(f"import of extension module '{self.archive_backend}' failed")
            self.archive_options = archive.parse_config(self, element)

    def update(self, new: dict):
        def update_dict(old: Any, new: dict):
            for key, value in new.items():
                if isinstance(value, dict):
                    if isinstance(old, dict):
                        update_dict(old[key], value)
                    else:
                        update_dict(old.__dict__[key], value)
                else:
                    setattr(old, key, value)
        update_dict(self, new)
