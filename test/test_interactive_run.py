# Copyright 2018 Uber Technologies, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess
import time
import torch
import traceback
import unittest
import warnings

import horovod.torch as hvd

from horovod.run.run import run


class InteractiveRunTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(InteractiveRunTests, self).__init__(*args, **kwargs)
        warnings.simplefilter('module')

    def happy_run(self):

        def fn(a, b, c, d):
            hvd.init()
            rank = hvd.rank()
            v = a + b + c + d
            res = hvd.allgather(torch.tensor([rank, v])).tolist()
            if rank == 0:
                return res

            for use_gloo in [False, True]:
                res1 = run(fn, (1, 20), {"c": 300, "d": 4000}, np=1, use_gloo=use_gloo)
                self.assertListEqual([0, 4321], res1)
                res1 = run(fn, (1, 20), {"c": 300, "d": 4000}, np=2, use_gloo=use_gloo)
                self.assertListEqual([0, 4321, 1, 4321], res1)

