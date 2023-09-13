PVML User Manual
================

:Version: 4.0


This document contains installation instructions, and a brief user manual for
the Processor Verification Management Layer (PVML).

PVML is an implementation of a Management Layer as used within the ESA Ground
Segment for executing data processors. It supports the interface as defined by
the Generic IPF Interface Guidelines (MMFI-GSEG-EOPG-TN-07-0003) as well as
the more recent Earth Explorer specific Generic Processor ICD
(ESA-EOPG-EEGS-ID-0083).
Using the tasktable files from a processor, a global PVML configuration file,
and a PVML job configuration file, PVML is able to create a joborder file and
run the task(s) of a processor.

Additionally, PVML will verify the inputs and outputs to check for compliance
with the applicable ICD. The aim is to ensure that a processor will operate
correctly within an ESA ground segment.



Installation Instructions
=========================
PVML is implemented as a python package that can be retrieved from
https://github.com/stcorp/pvml.

To install pvml, download the .tar.gz package and use: ::

  $ tar zxf pvml-4.0.tar.gz
  $ cd pvml-4.0
  $ pip install .

This will install the ``pvml`` python package, which includes the ``pvml``
command line tool.

Running PVML
============
To run PVML you will have to provide it a reference to the PVML global
configuration file and a reference to the PVML job configuration file (both are
explained below). The global configuration file can be provided as command line
parameter or as environment setting, but the job configuration file always has
to be provided as command line parameter. There are no specific restrictions on
where you place the global and job configuration files on your file system.

For example, if your global PVML configuration file is::

    /home/pvml/dummy/pvml-config.xml

and your job configuration file is::

    /home/pvml/dummy/pvml-job-12345.xml

then you can either use::

    $ pvml /home/pvml/dummy/pvml-config.xml /home/pvml/dummy/pvml-job-12345.xml

or, using the ``PVML_CONFIG`` setting ::

    $ export PVML_CONFIG=/home/pvml/dummy/pvml-config.xml
    $ pvml /home/pvml/dummy/pvml-job-12345.xml

All options necessary for PVML to run the processor and verify its inputs and
outputs, such as location of tasktable files, location of workspace directory,
location of XML Schema files, etc. are in these two configuration files.

Note that you can get an overview of the command line parameters of PVML by
providing the ``-h`` or ``--help`` option to the ``pvml`` script.

The execution flow of PVML is as follows:

- read the global PVML configuration file and verify its contents
- read the PVML job configuration file and verify its contents
- locate tasktable files and verify their contents
- find appropriate tasktable file for PVML job
- inside the workspace directory (as indicated by the global PVML config file)
  create a working directory with name equal to the job id
- copy all required input files (as indicated in the PVML job config file) to
  the working directory
- create a joborder file inside the working directory
- change the current directory to the working directory
- execute each task of the processor with a full path to the joborder file as
  single argument. Output of the processor is directed both to the console and
  a ``LOG.<order_id>`` file. Verify exit code of each task. If the exit code
  does not correspond with the expected exit code, PVML will produce an error
  and terminate
- after all tasks have been executed search for output products and verify
  whether there are entries for each expected output

Log messages from PVML itself will be written to the console and to a
``pvml.log`` file inside the working directory.



The global PVML config file
===========================
The global PVML configuration file contains options that are common for a
single processor or for a set of processors (e.g. all processors related to a
single mission or to a single instrument for a mission).

The global configuration file is an XML file with main element `pvmlConfig`.
Inside this element there should be the following sub elements:

name : mandatory
  The name of the configuration (usually the name of the mission, instrument,
  or processor).

taskTablePath : mandatory
  A ``:`` separated search path for tasktable files.
  Individual path items may be references to directories (in which case all
  items in the directory should be task table files), references to individual
  tasktable files, or globbing-style pattern matches
  (e.g. ``/dir/TaskTable*.xml``).

workspaceDirectory : mandatory
  The directory where PVML can create subdirectories for each job (each
  subdirectory will have the order id as directory name). Make sure that the
  pvml script has write access to this directory. The workspace directory
  should preferably be on a local file system (for performance reasons) with
  sufficient free disk space.

taskTableSchema : optional
  Full path to an xml schema file for the tasktable files. If this option is
  provided, PVML will perform a schema check on all tasktable files in the
  tasktable path. The tasktable schema should be as restrictive as
  possible to reflect the tailoring of the Management Layer interface for this
  specific configuration. This option should generally always be provided.

jobOrderSchema : optional
  Full path to a schema file for the joborder files. If this option is
  provided, PVML will perform a schema check on all joborder files that it
  creates itself. This option is normally not necessary but can be useful to
  verify the behavior of PVML. Just as the tasktable schema file, the joborder
  schema file should be as restrictive as possible to reflect the tailoring of
  the Management Layer interface for this specific configuration.

jobOrderSchemaName : optional, EEGS backend only
  Value to use for the schemaName attribute in the generated joborder file.
  If not set, the value ``JobOrder`` will be used.

jobOrderSchemaVersion : optional, EEGS backend only
  Value to use for the schemaVersion attribute in the generated joborder file.
  If not set, the value ``1.0`` will be used.

interfaceBackend : optional
  The type of Management Layer interface to use. This can be ``MMFI`` (the
  default) for the Generic IPF Interface Guidelines interface or ``EEGS`` for
  the more recent Earth Explorer Generic Processor ICD interface.

archiveBackend : optional
  The backend used to find and retrieve input products and store output
  products. This should be a reference to a python module that can be imported
  and that implements the PVML Archive Backend Interface.
  PVML itself comes with a simple local filesystem backend that is available
  as ``pvml.local``. This is the default backend if this option is not set.

archiveOptions : optional
  This is a free-form xml block whose content is further specific by the
  chosen archive backend. The options for the PVML local archive backend are
  specified further down in this document.

taskWrapper : optional
  Name or full path to an executable that will be called for each task in the
  job order. If a name is provided, the full path will be determined based on
  the PATH environment variable.
  Instead of executing the task itself, PVML will call the wrapper executable
  for each task. The wrapper executable should take two arguments. The first
  argument is the full path to the task executable as defined in the tasktable
  file and the second argument is the name of the joborder file.
  The wrapper executable will be called with the current directory set to the
  working directory (similar to how the original task would have been called).
  The wrapper task is responsible for producing log messages to stdout/stderr
  and producing a proper exit code as if it was the original task executable.

acquisitionStation : optional, MFFI backend only
  This value will be used to populate the corresponding entry in the joborder
  file. Its presence in the global PVML config file determines whether the
  ``Acquisition_Station`` field will be included in the joborder file.

processingStation : mandatory
  This value will be used to populate the corresponding entry in the joborder
  file.

fileClass : optional, EEGS backend only
  Name of the file class that will be stored in the joborder file as
  File_Class. If not specified, an empty string will be used.

splitLoggingLevel : optional, true/false/0/1, MFFI backend only
   If split (the default) then  separate entries for ``Stdout_Log_Level`` and
   ``Stderr_Log_Level`` will be generated in the joborder file, otherwise a
   single ``Logging_Level`` entry will be used.
   Currently PVML is limited to having the same value for ``Stdout_Log_Level``
   and ``Stderr_Log_Level`` (they will both have the value of ``loggingLevel``
   as included in the PVML job config file).

globalBreakpointEnable : optional, true/false/0/1, MFFI backend only
  If enabled (the default) a single ``Breakpoint_Enable`` element will be
  included in the joborder file, otherwise each ``BreakPoint`` section in the
  joborder file will get an ``Enable`` element. Note that it is currently not
  possible with PVML to enable/disable breakpoints. Breakpoints will always be
  disabled.

sensingTimeFlag : optional, true/false/0/1, MFFI backend only
  Whether the joborder file is allowed to have a ``Sensing_Time`` element.
  By default the flag is set to false.

alternateBreakpointElementName : optional, true/false/0/1, MFFI backend only
  By default the ``BreakPoint`` element inside ``Ipf_Proc`` should be named
  ``BreakPoint``. But by enabling this option the element will be named
  ``Breakpoint`` (with small ``p``).

alternateDynamicProcessingParameterName : optional, true/false/0/1, MFFI backend only
  By default, in case the task table contains the element named
  ``List_of_Dyn_ProcParam``, the Job Order contains the element
  ``Dynamic_Processing_Parameters`` with sub elements ``Processing_Parameter``.
  However, if this option is enabled the Job Order will use the element name
  ``List_of_Dynamic_Processing_Parameters`` (with ``count`` attribute) and with
  sub elements named ``Dynamic_Processing_Parameter``.

alwaysIncludeInputTimeInterval : optional, true/false/0/1, MFFI backend only
  By default the joborder file only contains ``Time_Interval`` elements if a
  validity start/stop is known for an input. When this option is enabled a
  ``Time_Interval`` element will always be created (using min/max dates for
  datetime values that have not been set).

clipInputTimeIntervalToSensingInterval : optional, true/false/0/1, MFFI backend only
  If enabled, the ``Time_Interval`` elements contained in the joborder file
  will be clipped to the overall sensing interval. If a ``Time_Interval``
  overlaps the overall sensing interval, it will be shortened to the overlap.
  Otherwise, it won't be changed. By default, this option is disabled.

useTroubleshooting : optional, true/false/0/1, MFFI backend only
  If enabled PVML will include a ``Troubleshooting`` element in the joborder
  file (its value is currently not configurable and will always be set to
  ``false``). By default this option is set to ``false``.

jobOrderTimeFormat : optional, MFFI backend only
  This describes the format to be used for time values in the joborder file.
  Only three values are allowed: ``YYYYMMDD_hhmmssuuuuuu`` (the default),
  ``YYYYMMDD_hhmmssuuu`` and ``YYYYMMDD_hhmmss.uuu``

minTimeValue : optional, MFFI backend only
  This sets the value that is used for parsing and writing of time values that
  are to indicate 'infinite in the past'. The value should be provided in
  ``YYYY-MM-DDThh:mm:ss.uuu`` format. The ``uuu`` part will be extended to
  ``uuuuuu`` by means of replication. Example: ``0000-00-00T00:00:00.000``.
  By default no special handling of a minimum time value is performed.

maxTimeValue : optional, MFFI backend only
  This sets the value that is used for parsing and writing of time values that
  are to indicate 'infinite in the future'. The value should be provided in
  ``YYYY-MM-DDThh:mm:ss.uuu`` format. The ``uuu`` part will be extended to
  ``uuuuuu`` by means of replication. Example: ``9999-99-99T99:99:99.999``.
  By default no special handling of a maximum time value is performed.

numericalOrderId : optional, MFFI backend only
 if set to true (the default) then the Job Order Id needs to be a numerical
 value. If set to false, the Job Order Id can be any string value.

ignoreListFile : optional, true/false/0/1, MFFI backend only
  If enabled, ignore the presence of a ``.LIST`` file and scan the working
  directory for output products directly. This is useful for cases where
  the ``.LIST`` file is not compliant with the specifications.
  This option overrides the `listFileMandatory` option.
  By default, ``.LIST`` files are not ignored.

listFileMandatory : optional, true/false/0/1, MFFI backend only
  If enabled, the processor is expected to produce a ``.LIST`` file and it is
  an error if this file cannot be found. If disabled (the default), then the
  presence of a ``.LIST`` file is optional and the working directory will be
  scanned for output products in case a ``.LIST`` file cannot be found.

listFilenameUsesOrderId : optional, true/false/0/1, MFFI backend only
  If enabled (the default), then the ``.LIST`` file as produced by the
  processor will have to be named ``<order_id>.LIST`` (with ``<order_id>``
  being the order id as included in the joborder filename). If disabled, then
  the processor can use any name for the ``.LIST`` file as long as it has the
  ``.LIST`` extension and as long as there is only one such file in the
  working directory.

listFileContainsStem : optional, true/false/0/1, MFFI backend only
  If enabled, the ``.LIST`` file is expected to contain a `stem` of filenames
  of generated products (e.g. just the product name; the filename without
  extension). In such cases any file or directory that starts with this stem
  prefix is considered to be part of the generated product. If disabled (the
  default), the ``.LIST`` file is expected to contain full filenames.

useRegexOutputPattern : optional, true/false/0/1, EEGS backend only
  Whether the ``File_Name_Pattern`` for outputs in the tasktables should be
  interpreted as regular expressions. By default the patterns will be
  interpreted as globbing patterns.

configSpaces/configSpace : optional, multiple, MFFI backend only
  Provide a default value (i.e. path to a configuration file) for specific
  config spaces. The ``name`` attribute should contain the name of the config
  space. Example::

    <configSpaces>
      <configSpace name="Geophysical_Constants">/path/to/Geophysical_Constants.xml</configSpace>
    </configSpaces>

productTypes/productType : optional, multiple
  When using the MMFI backend, then for each product type that is included in
  a tasktable where ``Destination=DB``, there should be an entry in the PVML
  global config file with a regular expression to be able to derive a product
  type for each entry in a ``.LIST`` file. The ``name`` attribute and
  ``matchExpression`` child element are mandatory. Example::

    <productTypes>
      <productType name="MY_TYPE">
        <matchExpression><![CDATA[.*MY_TYPE.*]]></matchExpression>
      </productType>
    </productTypes>

  You can also add ``productType`` entries for inputs in order to pass options
  on how PVML should treat these inputs:

  startTimeExpression : optional
    A regular expression that can be used to extract the start time from the
    product name. This attribute is applicable if the ``archiveBackend`` is
    set to ``pmvl.local``.
    The mandatory attribute 'format' describes the format of the extracted
    time. Supported formats are: ``YYYY-MM-DDThh:mm:ss.uuuuuu``,
    ``YYYY-MM-DDThh:mm:ss``, ``YYYYMMDDThhmmss``, ``YYYYMMDD_hhmmssuuuuuu``,
    ``YYYYMMDD_hhmmssuuu``, ``YYYYMMDD_hhmmss.uuu``, ``YYYYMMDD_hhmmss``.
    Example::

      <startTimeExpression format="YYYYMMDDThhmmss">
        <![CDATA[.{19}(.{15})]]>
      </startTimeExpression>

  stopTimeExpression : optional
    Similar to `startTimeExpression` (see description above).

  stemExpression : optional, MFFI backend only
    A regular expression used to group a list of files that match the
    ``matchExpression`` associated with this product type into products. For
    example, when no ``.LIST`` file is produced by a processor, subsequent
    scanning of the working directory may produce a list of multiple ``.HDR``
    / ``.DBL`` pairs. A 'stemExpression' such as ``[^.]*`` could then be used
    to group this list into products (each ``.HDR`` / ``.DBL`` pair
    constitutes a product).

  stemAsPhysicalDBL : optional, true/false/0/1, MFFI backend only
    If set to true, then for products that are composed of a ``.HDR`` and
    ``.DBL`` file PVML will include a full path to the ``.DBL`` file in case
    the tasktable uses ``Physical`` for ``File_Name_Type``.
    The filename reference in the PVML job config file still needs to be
    provided as ``Stem`` for inputs of this product type (i.e. the product
    name without ``.HDR`` or ``.DBL`` extension).

  hasMetadataFile : optional, true/false/0/1, MFFI backend only
    If set to true, the processor is expected to produce a metadata file for
    products of this product type. The filename of the metadata file consists
    of the product name followed by the extension ``.MTD``. It is an error if
    no file with this filename can be found in the working directory.

  hasMultiProductOutput : optional, true/false/0/1, MFFI backend only
    If set to true, it is allowed for the processor to produce multiple
    products of this product type. If set to false (the default) PVML will
    produce a warning if the processor generates more than one product of this
    type.

  baseline : optional, true/false/0/1, EEGS backend only
    This is the baseline value that will be stored in the joborder file.
    If this value is not provided for a product type it will use a default
    value of ``"01"``.



The PVML job config file
========================
The PVML job configuration file contains options that are specific for a single
run of a processor. When verifying a processor, each PVML job can be considered
a `test case`; this is a specific run of the processor with a predefined set of
inputs.

The job configuration file is an XML file with main element `pvmlJob`. Inside
this element there should be the following items (in order):

jobOrderId : mandatory
  The order id of the job. This id will be used as name for the subdirectory in
  the workspace directory and will be included in the name of the joborder file
  that gets provided to the processor.

processorName : mandatory
  This is the name of the processor. It is used, together with
  `processorVersion`, to find the appropriate tasktable file for the job.

processorVersion : mandatory
  This is the version of the processor. It is used, together with the
  `processorName`, to find the appropriate tasktable file for the job.

mode : optional, MFFI backend only
  Run the processor using the specified mode. This determines which inputs are
  chosen from the tasktable. Only inputs that have Mode set to the specified
  value will be included or inputs that have ``Mode=ALWAYS``. If no mode
  parameter is provided, then only inputs with ``Mode=ALWAYS`` will be
  included.

fileClass : optional, EEGS backend only
  Name of the file class that will be stored in the joborder file. It will
  override the same option from the global PVML config file.

workingDirectory : optional
  Instead of using a subdirectory with the name of the ``jobOrderId`` in the
  PVML workspace directory, use this specific directory as working directory
  for the job. If the value is a relative path it will be used relative to the
  workspace directory as specified in the PVML config file. Note that if the
  directory already exists, PVML will first remove all contents in the
  directory before starting the job.

processingNode : optional, EEGS backend only
  Name of the processing node that will be stored in the joborder file as
  Processing_Node. This name will then be used in the log messages generated
  by the processor. If not specified, the hostname of the system will be used.

loggingLevel : optional
  This defines the logging level as included in the joborder file. Values can
  be ``DEBUG``, ``INFO``, ``PROGRESS``, ``WARNING``, or ``ERROR``. The default
  value is ``INFO``.

enableBreakpoints : optional, true/false/0/1
  Whether to enable breakpoints / troubleshooting in the generated joborder
  file. The default is false.

test : optional, true/false/0/1
  If enabled, the ``Test`` field in the joborder file will be set to true (the
  default is false).

acquisitionStation : optional, MFFI backend only
  This value will be used to populate the corresponding entry in the joborder
  file. It will override the same option from the global PVML config file.

processingStation :  optional
  This value will be used to populate the corresponding entry in the joborder
  file. It will override the same option from the global PVML config file.

orderType : optional
  Only appropriate for processors where an ``Order_Type`` is requested to be
  present in the joborder file. With this option you provide the value that
  should be included in the joborder file.

sensingStart : optional
  Overall sensing start time. This value will be included in the joborder
  ``Sensing_Time`` section. The format should be ISO8601 without time zone
  indicator. If this parameter is not provided and the first input in the
  job config file has an associated start time, then that start time will
  be used for the overall sensing start time.

sensingStop : optional
  Overall sensing stop time. This value will be included in the joborder
  ``Sensing_Time`` section. The format should be ISO8601 without time zone
  indicator. If this parameter is not provided and the first input in the
  job config file has an associated stop time, then that stop time will be
  used for the overall sensing stop time.

processingParameters/parameter : optional, multiple
  Assign a value to each processing parameters. Whether a processing parameter
  is mandatory or optional depends on the contents of the tasktable file.
  Example::

    <processingParameters>
      <parameter name="File_Counter">0123</parameter>
    </processingParameters>

configSpaces/configSpace : optional, multiple, MFFI backend only
  Provide the configuration file path for each config space that is referenced
  in tasktable file. This element is mandatory in case no default value for the
  config space was defined in the global PVML config file. The ``name``
  attribute should contain the name of the config space. Example::

    <configSpaces>
      <configSpace name="Geophysical_Constants">/path/to/Geophysical_Constants.xml</configSpace>
    </configSpaces>

inputs/input : optional, multiple
  Only inputs for which ``Origin=DB`` in the tasktable file should be
  described. Whether an input reference is optional or mandatory depends on
  whether the input is defined as mandatory in the tasktable file.

  product : mandatory, multiple
    An input of a specific product type can consist of one or more products.
    For each product a reference to the products should be provided.
    The format of the reference depends on the chosen archive backend.
    For ``pvml.local`` this should be a full path to the product.
    PVML will copy the file to the working directory before executing the
    processor.
    When a file needs to have an accompanying entry in the
    ``List_of_Time_Intervals`` section (each file will have its own time
    interval), then you should provide start and stop attributes containing
    ISO8601 time references (without time zone indicator) to provide the
    start/stop times. Example::

      <inputs>
        <input product_type="MY_TYPE">
          <file start="2010-01-01T00:00:00" stop="2011-01-01T00:00:00">/data/MY_TYPE_2010_2011.dat</file>
        </input>
      </inputs>

    The ``start`` and ``stop`` attributes are optional. If the attributes are
    not set and the PVML global config file contains start/stop time
    expressions for this product type, then these expressions will be used to
    determine the start/stop time values. Otherwise, no start/stop time will
    be associated to the input file.

exitCodes : optional, multiple
  A space-separated list of expected exit codes for a task. If not provided
  PVML will expect the task to exit with exit code 0. The ``task``
  attribute should contain the name of the task.

archiveOptions : optional
  This is a free-form xml block whose content is further specific by the
  chosen archive backend. The options for the PVML local archive backend are
  specified further down in this document.



Local Archive Backend options
=============================
These are the backend options when the archive backend is set to
``pvml.local``.

useSymlinks : optional, true/false/0/1
  If enabled, PVML will create symbolic links to the input files in the
  working directory instead of physically copying them. Using symbolic links
  is disabled by default.
