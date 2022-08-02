#!/usr/bin/env python3
"""
Copyright 2022 Marko Karg

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#from datetime import _IsoCalendarDate
import re
import string
import sys
import typing
import tempfile
import yaml
import json
import subprocess
import dataclasses
import fileinput
import os
import shutil
import csv
from dataclasses import dataclass
from typing import List
from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import schema



@dataclass
class cpuStressorParams:
    """
    The parameters for the CPU stressor 
    """
    cpu_count: int
    cpu_method: typing.Optional[str] = "all"


@dataclass
class vmStressorParams:
    """
    The parameters for the vm (virtual memory) stressor
    vm: number of virtual-memory stressors
    vm_bytes: amount of vm stressor memory
    """
    vm: int
    vm_bytes: str
    mmap: typing.Optional[str] = None
    mmap_bytes: typing.Optional[str] = None


@dataclass
class StressNGParams:
   """
   The parameters in this schema will be passed through to the stressng command unchanged
   """
   # generic options
   timeout: str 
   items: typing.List[typing.Union[cpuStressorParams, vmStressorParams]]
   verbose: typing.Optional[str] = None
   metrics_brief: typing.Optional[str] = None



@dataclass
class WorkloadParams:
  """
  This is the data structure for the input parameters of the step defined below
  """
  samples: int
  StressNGParams: StressNGParams
  cleanup: typing.Optional[str] = "True"


@dataclass
class SystemInfoOutput:
    """
    This is the data structure that holds the generic info for the tested system
    """
    stress_ng_version: str = dataclasses.field(metadata={"id": "stress-ng-version"})
    run_by: str  = dataclasses.field(metadata={"id": "run-by"})
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

@dataclass
class VMOutput:
    """
    This is the data structure that holds the results for the VM stressor
    """
    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: int = dataclasses.field(metadata={"id": "bogo-ops-per-second-usr-sys-time"})
    bogo_ops_per_second_real_time: int = dataclaseses.field(metadata={"id": "bogo-ops-per-second-real-time"})
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(metadata={"id": "cpu-usage-per-instance"})


@dataclass
class CPUOutput:
    """
    This is the data structure that holds the results for the CPU stressor
    """
    stressor: str
    bogo_ops: int = dataclasses.field(metadata={"id": "bogo-ops"})
    bogo_ops_per_second_usr_sys_time: int = dataclasses.field(metadata={"id": "bogo-ops-per-second-usr-sys-time"})
    bogo_ops_per_second_real_time: int = dataclaseses.field(metadata={"id": "bogo-ops-per-second-real-time"})
    wall_clock_time: float = dataclasses.field(metadata={"id": "wall-clock-time"})
    user_time: float = dataclasses.field(metadata={"id": "user-time"})
    system_time: float = dataclasses.field(metadata={"id": "system-time"})
    cpu_usage_per_instance: float = dataclasses.field(metadata={"id": "cpu-usage-per-instance"})

  

@dataclass
class WorkloadResults:
    """
    This is the output data structure for the success case
    """
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

def stressng_run(params: WorkloadParams) -> typing.Typle[str, typing.Union[WorkloadResults, WorkloadError]]:
    """
    This function is implementing the step. It needs the decorator to turn it into a step. The type hints for the params are required.
    
    :param params

    :return: the string identifying which output it is, as well as the output structure
    """

    print("==>> Building the commandline to run")
    stressng_command = [
        f"stressng --cpu {params.cpu_count} --vm {params.vm} --vm-bytes {params.vm_bytes} --timeout {params.timeout} --metrics -Y /tmp/blabla.yml"
    ]

    # run the stressng workload
    print("==>> Running the stressng workload")
    try:
        print(subprocess.check_output(stressng_command, cwd="/tmp", text=True, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError as error:
        temp_cleanup("blabla.yml")
        return "error", WorkloadError(f"{error.cmd[0]} failed with return code {error.returncode}:\n{error.output} ")


if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        stressng_run,
    )))
        