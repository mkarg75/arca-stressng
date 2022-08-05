#!/usr/bin/env python3


# from datetime import _IsoCalendarDate
# import re
# import string
from imaplib import IMAP4_stream
from os import system
from stat import FILE_ATTRIBUTE_NO_SCRUB_DATA
import sys
import typing

import tempfile
from urllib.parse import _NetlocResultMixinBytes
from numpy import int0
import yaml
import json
import subprocess
import dataclasses

# import fileinput
# import os
# import shutil
# import csv
from dataclasses import dataclass


# from typing import List
from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import schema
from arcaflow_plugin_sdk import annotations


@dataclass
class cpuStressorParams:
    """
    The parameters for the CPU stressor
    """
    stressor: str
    cpu_count: int
    cpu_method: typing.Optional[str] = "all"

    def to_jobfile(self) -> str:
        result = "cpu {}\n".format(self.cpu_count)
        if self.cpu_method is not None:
            result = result + "cpu-method {}\n".format(self.cpu_method)
        return result 


@dataclass
class cpu_cacheStressorParams:
    """
    The parameters for the cpu cache stressor, 0 for the following options means 1 worker
    doing the respective type of work:

    bsearch: binary search worker
    cache: random wide spread memory read and write
    heapsort: sort 32 bit integers using BSD heapsort
    hsearch: search a 80$ full has table using hsearch
    icache: stress the instruction cache by forcing reloads
    lockbus: rapidly lock and increment 64 bytes of randomly chosen memory
    lsearch: linear search of unsorted array 
    malloc: continuously call malloc, callc, realloc and free
    matrix: perform various matrix operations
    membarrier: exercise the membarrir system call (Linux only)
    memcpy: copy 2MB of data from a shared region to a buffer
    mergesort: sort 32 bit integers using BSD mergesort
    qsort: sort 32 bit integers using qsort
    str: exercise various libc string functions on random strings
    stream: exercise memory bandwith stressor
    tsearch: insert, search and delete 32 bit integers on a binary tree
    vecmath: perform various unsinged integer math operations on 128 bit vectors
    wcs: exercise various libc wide character string functions on random strings
    zlib: compress and decompress randon data using zlig
    """
    stressor: str
    bsearch: int
    cache: int
    heapsort: int
    hsearch: int
    icache: int
    lockbus: int
    lsearch: int
    malloc: int
    matrix: int
    membarrier: int
    memcpy: int
    mergesort: int
    qsort: int
    str: int
    stream: int
    tsearch: int
    vecmath: int
    wcs: int
    zlib: int



@dataclass
class vmStressorParams:
    """
    The parameters for the vm (virtual memory) stressor
    vm: number of virtual-memory stressors
    vm_bytes: amount of vm stressor memory
    """
    stressor: str
    vm: int
    vm_bytes: str
    mmap: typing.Optional[str] = None
    mmap_bytes: typing.Optional[str] = None

    def to_jobfile(self) -> str:
        vm = "vm {}\n".format(self.vm)
        vm_bytes = "vm-bytes {}\n".format(self.vm_bytes)
        result = vm + vm_bytes
        if self.mmap is not None:
            result = result + "mmap {}\n".format(self.mmap)
        if self.mmap_bytes is not None:
            result = result + "mmap-bytes {}\n".format(mmap)
        return result
        
           

@dataclass
class StressNGParams:
    """
    The parameters in this schema will be passed through to the stressng
    command unchanged
    """

    # generic options
    timeout: str
    items: typing.List[
        typing.Annotated[
            typing.Union[
                typing.Annotated[cpuStressorParams, annotations.discriminator_value("cpu")],
                typing.Annotated[vmStressorParams, annotations.discriminator_value("vm")]
            ],
            annotations.discriminator("stressor")
        ]
    ]
    verbose: typing.Optional[str] = None
    metrics_brief: typing.Optional[str] = None

    def to_jobfile(self) -> str:
        result = "timeout {}\n".format(self.timeout)
        if self.verbose is not None:
            result = result + "verbose {}\n".format(self.verbose)
        if self.metrics_brief is not None:
            result = result + "metrics-brief {}\n".format(self.metrics_brief)
        return result


@dataclass
class WorkloadParams:
    """
    This is the data structure for the input parameters of the step
    defined below
    """
    "str",
    # samples: int
    StressNGParams: StressNGParams
    cleanup: typing.Optional[str] = "True"


@dataclass
class SystemInfoOutput:
    """
    This is the data structure that holds the generic info for the
    tested system
    """

    stress_ng_version: str = dataclasses.field(metadata={"id": "stress-ng-version"})
    run_by: str = dataclasses.field(metadata={"id": "run-by"})
    date: str = dataclasses.field(metadata={"id": "date-yyyy-mm-dd"})
    time: str = dataclasses.field(metadata={"id": "time-hh-mm-ss"})
    epoch: int = dataclasses.field(metadata={"id": "epoch-secs"})
    hostname: str
    sysname: str
    nodename: str
    release: str
    version: str
    machine: str
    uptime: int
    totalram: int
    freeram: int
    sharedram: int
    bufferram: int
    totalswap: int
    freeswap: int
    pagesize: int
    cpus: int
    cpus_online: int = dataclasses.field(metadata={"id": "cpus-online"})
    ticks_per_second: int = dataclasses.field(metadata={"id": "ticks-per-second"})

system_info_output_schema = plugin.build_object_schema(SystemInfoOutput)

@dataclass
class VMOutput:
    """
    This is the data structure that holds the results for the VM stressor
    """

    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: float = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-usr-sys-time"}
    )
    bogo_ops_per_second_real_time: float = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-real-time"}
    )
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(
        metadata={"id": "cpu-usage-per-instance"}
    )

vm_output_schema = plugin.build_object_schema(VMOutput)

@dataclass
class CPUOutput:
    """
    This is the data structure that holds the results for the CPU stressor
    """

    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: float = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-usr-sys-time"}
    )
    bogo_ops_per_second_real_time: float = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-real-time"}
    )
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(
        metadata={"id": "cpu-usage-per-instance"}
    )

cpu_output_schema = plugin.build_object_schema(CPUOutput)

@dataclass
class CPUCacheOutput:
    """
    This is the data structure that holds the results for the CPU Cache stressor
    """

    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: float = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-usr-sys-time"}
    )
    bogo_ops_per_second_real_time: float = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-real-time"}
    )
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(
        metadata={"id": "cpu-usage-per-instance"}
    )

cpucache_output_schema = plugin.build_object_schema(CPUCacheOutput)



@dataclass
class WorkloadResults:
    """
    This is the output data structure for the success case
    """

    systeminfo: SystemInfoOutput
    vminfo: typing.Optional[VMOutput] = None
    cpuinfo: typing.Optional[CPUOutput] = None
    #cpucacheinfo: typing.Optional[CPUCacheOutput] = None


@dataclass
class WorkloadError:
    """
    This is the output data structure for the failure case
    """

    error: str


# The following is a decorator (starting with @). We add this in front of our function to define the medadata for our step.
@plugin.step(
    id="workload",
    name="stress-ng workload",
    description="Run the stress-ng workload with the given parameters",
    outputs={"success": WorkloadResults, "error": WorkloadError},
)
def stressng_run(params: WorkloadParams) -> typing.Tuple[str, typing.Union[WorkloadResults, WorkloadError], ]:
#def stressng_run(params: WorkloadParams) -> str:
    """
    This function is implementing the step. It needs the decorator to turn it into a step. The type hints for the params are required.

    :param params

    :return: the string identifying which output it is, as well as the output structure
    """

    print("==>> Generating temporary jobfile...")
    # generic parameters are in the StressNGParams class (e.g. the timeout)
    result = params.StressNGParams.to_jobfile()
    # now we need to iterate of the list of items
    for item in params.StressNGParams.items:
        result = result + item.to_jobfile()
    
    stressng_jobfile = tempfile.mkstemp()
    stressng_outfile = tempfile.mkstemp()

    # write the temporary jobfile
    with open(stressng_jobfile[1], 'w') as jobfile:
        jobfile.write(result)
    stressng_command = ["/usr/bin/stress-ng", "-j", stressng_jobfile[1], "--metrics", "-Y", stressng_outfile[1]]

    print("==>> Running stress-ng with the temporary jobfile...")  
    try:
        print(subprocess.check_output(stressng_command, cwd="/tmp", text=True, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError as error:
        return "error", WorkloadError(f"{error.cmd[0]} failed with return code {error.returncode}:\n{error.output}")    
    
    with open(stressng_outfile[1], 'r') as output:
        try:
            stressng_yaml = yaml.safe_load(output)
        except yaml.YAMLError as e:
            print(e)
 
    system_info = (stressng_yaml['system-info'])
    metrics = (stressng_yaml['metrics'])
    
    cpuinfo = {}
    vminfo = {}
    cpucacheinfo = {}
    
    for metric in metrics:
        print("Current metric: ", metric)
        print("stressor: ", metric['stressor'])
        if metric['stressor'] == "cpu":
            cpuinfo = metric
            print("Returning cpuinfo object!")
        if metric['stressor'] == "vm":
            vminfo = metric
            print("Returning vminfo object!")
        #if metric['stressor'] == "cpu-cache":
        #    cpucacheinfo = metric
    
    print("==>> Workload run complete!")

    return "success", WorkloadResults(system_info_output_schema.unserialize(system_info), vm_output_schema.unserialize(vminfo), 
    cpu_output_schema.unserialize(cpuinfo))    
    

if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                stressng_run,
            )
        )
    )
