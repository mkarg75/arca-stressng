#!/usr/bin/env python3

import re
import tempfile
import unittest
import stressng_plugin
from arcaflow_plugin_sdk import plugin


class StressNGTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            stressng_plugin.cpuStressorParams(
                stressor="cpu",
                cpu_count=2
            )
        )

        plugin.test_object_serialization(
            stressng_plugin.vmStressorParams(
                stressor="vm",
                vm=2,
                vm_bytes="2G"
            )
        )

        plugin.test_object_serialization(
            stressng_plugin.matrixStressorParams(
                stressor="matrix",
                matrix=2
            )
        )
        plugin.test_object_serialization(
            stressng_plugin.mqStressorParams(
                stressor="mq",
                mq=2
            )
        )
   
        
    def test_functional_cpu(self):
        # idea is to run a small cpu bound benchmark and compare its output with a known-good output
        # this is clearly not perfect, as we're limited to the field names and can't do a direct 
        # comparison of the returned values

        cpu = stressng_plugin.cpuStressorParams(
            stressor="cpu",
            cpu_count=2,
            cpu_method="all"
        )

        stress = stressng_plugin.StressNGParams(
            timeout="1m",
            cleanup="False",
            items=[cpu]
        )

        input = stressng_plugin.WorkloadParams(
            StressNGParams=stress,
            cleanup="False"
        )

        stressng_jobfile = tempfile.mkstemp()
        reference_jobfile = "./test_cpu.yaml"

        result = stressng_plugin.StressNGParams.to_jobfile()
        for item in stressng_plugin.StressNGParams.items:
            result = result + item.to_jobfile()
            # write the temporary jobfile
        
        with open(stressng_jobfile[1], 'w') as jobfile:
            jobfile.write(result)

        # next - compare the jobfile with the reference file



        
if __name__ == '__main__':
    unittest.main()