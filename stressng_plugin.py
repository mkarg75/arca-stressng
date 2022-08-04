#!/usr/bin/env python3


# from datetime import _IsoCalendarDate
# import re
# import string
from imaplib import IMAP4_stream
import sys
import typing

# import tempfile
import yaml
# import json
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

    @classmethod
    def to_jobfile(self) -> str:
        result = "cpu {}\n".format(self.cpu_count)
        if self.cpu_method is not None:
            result = result + "cpu-method {}\n".format(self.cpu_method)
            print(result)
        return result 



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

    @classmethod
    def to_jobfile(self) -> str:
        vm = "vm {}\n".format(self.vm)
        vm_bytes = "vm-bytes {}\n".format(self.vm_bytes)
        result = vm + vm_bytes
        if self.mmap is not None:
            result = result + "mmap {}\n".format(self.mmap)
        if self.mmap_bytes is not None:
            result = result + "mmap-bytes {}\n".format(mmap)
        print(result)
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

    @classmethod
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

system_output_results_schema = plugin.build_object_schema(SystemInfoOutput)

@dataclass
class VMOutput:
    """
    This is the data structure that holds the results for the VM stressor
    """

    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: int = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-usr-sys-time"}
    )
    bogo_ops_per_second_real_time: int = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-real-time"}
    )
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(
        metadata={"id": "cpu-usage-per-instance"}
    )


@dataclass
class CPUOutput:
    """
    This is the data structure that holds the results for the CPU stressor
    """

    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: int = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-usr-sys-time"}
    )
    bogo_ops_per_second_real_time: int = dataclasses.field(
        metadata={"id": "bogo-ops-per-second-real-time"}
    )
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(
        metadata={"id": "cpu-usage-per-instance"}
    )


@dataclass
class WorkloadResults:
    """
    This is the output data structure for the success case
    """
    discriminator: str

    systeminfo: SystemInfoOutput
    vminfo: VMOutput
    cpuinfo: CPUOutput


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
#def stressng_run(params: WorkloadParams) -> typing.Tuple[str, typing.Union[WorkloadResults, WorkloadError], ]:
def stressng_run(params: WorkloadParams) -> str:
    """
    This function is implementing the step. It needs the decorator to turn it into a step. The type hints for the params are required.

    :param params

    :return: the string identifying which output it is, as well as the output structure
    """

    print("==>> Importing workload parameters...")
    result = params.StressNGParams.items[0]
    data = result.to_jobfile()
    print(data)
    #result = result + params.StressNGParams.items[1].to_jobfile
    #print(result)
     
    #print("==>> Building the commandfile for stress-ng")
    # looping over the stressng_example yaml

    #print("==>> Building the commandline to run")
    #print(str(params.StressNGParams.items[0].stressor))
    #stressng_command = ["/usr/bin/stress-ng", "--cpu", str(params.StressNGParams.items[0]cpu_count), "--vm", str(params.vm), "--vm-bytes", str(params.vm_bytes), "--timeout", str(params.timeout), "--metrics -Y /tmp/blabla.yml"]


    # run the stressng workload
    #print("==>> Running the stressng workload")
    #try:
    #    print(
    #        subprocess.check_output(
    #            #stressng_command, cwd="/tmp", text=True, stderr=subprocess.STDOUT
    #        )
    #    )
    #except subprocess.CalledProcessError as error:
    #    # temp_cleanup("blabla.yml")
    #    return "error", WorkloadError(
    #        f"{error.cmd[0]} failed with return code {error.returncode}:\n{error.output} "
    #    )
    return "success"

if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                stressng_run,
            )
        )
    )
