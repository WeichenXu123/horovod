# Copyright 2019 Uber Technologies, Inc. All Rights Reserved.
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

from __future__ import print_function
import argparse
import hashlib
import os
import sys
import traceback
import six
import uuid
import cloudpickle
import textwrap
import socket
try:
    from shlex import quote
except ImportError:
    from pipes import quote
import horovod

from horovod.run.common.util import codec, env as env_util, safe_shell_exec, \
    timeout, secret
from horovod.run.common.util import settings as hvd_settings
from horovod.run.driver import driver_service
from horovod.run.task import task_service
from horovod.run.util import cache, threads, network

# Cached information of horovodrun functions be stored in this directory
CACHE_FOLDER = os.path.join(os.path.expanduser('~'), '.horovod')

# Cache entries will be stale if they are older than this number of minutes
CACHE_STALENESS_THRESHOLD_MINUTES = 60

# Number of retries for sshing into the hosts
SSH_RETRIES = 5

_MPI_ROOT_WDIR = "/tmp"
_PICKLED_PROC_FN_FILENAME = "proc_fn.pkl"
_LOCAL_PICKLED_RESULT_FILENAME = "local_result.pkl"
_PICKLED_RESULT_FILENAME = "result.pkl"
_PROC_LAUNCHER_FILENAME = "launch.sh"

@cache.use_cache()
def _check_all_hosts_ssh_successful(host_addresses, ssh_port=None):
    """
    checks if ssh can successfully be performed to all the hosts.
    :param host_addresses: list of addresses to ssh into. for example,
        ['worker-0','worker-1']
        ['10.11.11.11', '10.11.11.12']
    :type host_addresses: list(strings)
    :return: Returns True if all ssh was successful into all the addresses.
    """

    def exec_command(command):
        exit_code = 1
        output_msg = ""

        # Try ssh 5 times
        for i in range(SSH_RETRIES):
            output = six.StringIO()
            try:
                exit_code = safe_shell_exec.execute(command,
                                                    stdout=output,
                                                    stderr=output)
                if exit_code == 0:
                    break
                else:
                    output_msg = output.getvalue()
            finally:
                output.close()
        return exit_code, output_msg

    if ssh_port:
        ssh_port_arg = "-p {ssh_port}".format(ssh_port=ssh_port)
    else:
        ssh_port_arg = ""

    ssh_command_format = 'ssh -o StrictHostKeyChecking=no {host} {ssh_port_arg} date'

    args_list = [[ssh_command_format.format(host=host_address,
                                            ssh_port_arg=ssh_port_arg)]
                 for host_address in host_addresses]
    ssh_exit_codes = \
        threads.execute_function_multithreaded(exec_command,
                                               args_list)

    ssh_successful_to_all_hosts = True
    for index, ssh_status in six.iteritems(ssh_exit_codes):
        exit_code, output_msg = ssh_status[0], ssh_status[1]
        if exit_code != 0:
            print("ssh not successful for host {host}:\n{msg_output}".format(
                host=host_addresses[index],
                msg_output=output_msg
            ))

            ssh_successful_to_all_hosts = False
    if not ssh_successful_to_all_hosts:
        exit(1)
    return True


def _launch_task_servers(all_host_names, local_host_names, driver_addresses,
                         settings):
    """
    executes the task server and service client task for registration on the
    hosts.
    :param all_host_names: list of addresses. for example,
        ['worker-0','worker-1']
        ['10.11.11.11', '10.11.11.12']
    :type all_host_names: list(string)
    :param local_host_names: names that are resolved to one of the addresses
    of local hosts interfaces. For example,
        set(['localhost', '127.0.0.1'])
    :type local_host_names: set
    :param driver_addresses: map of interfaces and their address and port for
    the service. For example:
        {
            'lo': [('127.0.0.1', 34588)],
            'docker0': [('172.122.10.1', 34588)],
            'eth0': [('11.111.33.73', 34588)]
        }
    :type driver_addresses: map
    :param settings: the object that contains the setting for running horovod
    :type settings: Horovod.run.common.util.settings.Settings
    :return:
    :rtype:
    """

    def _exec_command(command):
        host_output = six.StringIO()
        try:
            exit_code = safe_shell_exec.execute(command,
                                                stdout=host_output,
                                                stderr=host_output)
            if exit_code != 0:
                print(
                    "Launching horovodrun task function was not "
                    "successful:\n{host_output}"
                        .format(host_output=host_output.getvalue()))
                os._exit(exit_code)
        finally:
            host_output.close()
        return exit_code

    if settings.ssh_port:
        ssh_port_arg = "-p {ssh_port}".format(ssh_port=settings.ssh_port)
    else:
        ssh_port_arg = ""
    args_list = []
    for index in range(len(all_host_names)):
        host_name = all_host_names[index]
        if host_name in local_host_names:
            command = \
                '{python} -m horovod.run.task_fn {index} ' \
                '{driver_addresses} {settings}'.format(
                    python=sys.executable,
                    index=codec.dumps_base64(index),
                    driver_addresses=codec.dumps_base64(driver_addresses),
                    settings=codec.dumps_base64(settings)
                )
        else:
            command = \
                'ssh -o StrictHostKeyChecking=no {host} {ssh_port_arg} ' \
                '\'{python} -m horovod.run.task_fn {index} {driver_addresses}' \
                ' {settings}\''.format(
                    host=host_name,
                    ssh_port_arg=ssh_port_arg,
                    python=sys.executable,
                    index=codec.dumps_base64(index),
                    driver_addresses=codec.dumps_base64(driver_addresses),
                    settings=codec.dumps_base64(settings)
                )
        args_list.append([command])
    # Each thread will use ssh command to launch the server on one task. If an
    # error occurs in one thread, entire process will be terminated. Otherwise,
    # threads will keep running and ssh session -- and the the task server --
    # will be bound to the thread. In case, the horovodrun process dies, all
    # the ssh sessions and all the task servers will die as well.
    threads.execute_function_multithreaded(_exec_command,
                                           args_list,
                                           block_until_all_done=False)


@cache.use_cache()
def _driver_fn(all_host_names, local_host_names, settings):
    """
    launches the service service, launches the task service on each worker and
    have them register with the service service. Each worker probes all the
    interfaces of the worker index + 1 (in a ring manner) and only keeps the
    routed interfaces. Function returns the intersection of the set of all the
    routed interfaces on all the workers.
    :param key:
    :type key: string
    :param all_host_names: list of addresses. for example,
        ['worker-0','worker-1']
        ['10.11.11.11', '10.11.11.12']
    :type all_host_names: list(string)
    :param local_host_names: host names that resolve into a local addresses.
    :type local_host_names: set
    :param settings: the object that contains the setting for running horovod
    :type settings: Horovod.run.common.util.settings.Settings
    :return: example: ['eth0', 'eth1']
    :rtype: list[string]
    """
    # Launch a TCP server called service service on the host running horovodrun.
    driver = driver_service.HorovodRunDriverService(settings.num_hosts, settings.key)
    if settings.verbose >= 2:
        print("Launched horovodrun server.")
    # Have all the workers register themselves with the service service.
    _launch_task_servers(all_host_names, local_host_names,
                         driver.addresses(), settings)
    if settings.verbose >= 2:
        print("Attempted to launch horovod task servers.")
    try:
        # wait for all the hosts to register with the service service.
        if settings.verbose >= 2:
            print("Waiting for the hosts to acknowledge.")
        driver.wait_for_initial_registration(settings.timeout)
        tasks = [task_service.HorovodRunTaskClient(index,
                                                   driver.task_addresses_for_driver(
                                                       index),
                                                   settings.key,
                                                   settings.verbose)
                 for index in range(settings.num_hosts)]
        # Notify all the drivers that the initial registration is complete.
        for task in tasks:
            task.notify_initial_registration_complete()
        if settings.verbose >= 2:
            print("Notified all the hosts that the registration is complete.")
        # Each worker should probe the interfaces of the next worker in a ring
        # manner and filter only the routed ones -- it should filter out
        # interfaces that are not really connected to any external networks
        # such as lo0 with address 127.0.0.1.
        if settings.verbose >= 2:
            print("Waiting for hosts to perform host-to-host "
                  "interface checking.")
        driver.wait_for_task_to_task_address_updates(settings.timeout)
        if settings.verbose >= 2:
            print("Host-to-host interface checking successful.")
        # Determine a set of common interfaces for task-to-task communication.
        common_intfs = set(driver.task_addresses_for_tasks(0).keys())
        for index in range(1, settings.num_hosts):
            common_intfs.intersection_update(
                driver.task_addresses_for_tasks(index).keys())
        if not common_intfs:
            raise Exception(
                'Unable to find a set of common task-to-task communication '
                'interfaces: %s'
                % [(index, driver.task_addresses_for_tasks(index))
                   for index in range(settings.num_hosts)])
        return common_intfs
    finally:
        driver.shutdown()


def _is_open_mpi_installed():
    output = six.StringIO()
    command = 'mpirun --version'
    try:
        exit_code = safe_shell_exec.execute(command, stdout=output,
                                            stderr=output)
        output_msg = output.getvalue()
    except Exception:
        print(traceback.format_exc(), file=sys.stderr)
        return False
    finally:
        output.close()

    if exit_code == 0:
        if 'Open MPI' not in output_msg:
            print('Open MPI not found in output of mpirun --version.',
                  file=sys.stderr)
            return False
        else:
            return True
    else:
        print("Was not able to run %s:\n%s" % (command, output_msg),
              file=sys.stderr)
        return False


def parse_args():
    parser = argparse.ArgumentParser(description='Horovod Runner')

    parser.add_argument('-v', '--version', action="store_true", dest="version",
                        help="Shows horovod version.")

    parser.add_argument('-np', '--num-proc', action="store", dest="np",
                        type=int, help="Total number of training processes.")

    parser.add_argument('-p', '--ssh-port', action="store", dest="ssh_port",
                        type=int, help="SSH port on all the hosts.")

    parser.add_argument('-H', '--host', action="store", dest="host",
                        help="To specify the list of host names as well as the "
                             "number of available slots on each host for "
                             "training processes using the following format: "
                             "<hostname>:<number of slots>,... . "
                             "E.g., host1:2,host2:4,host3:1 "
                             "indicates that 2 processes can run on "
                             "host1, 4 processes on host2, and 1 process "
                             "on host3.")

    parser.add_argument('--disable-cache', action="store_true",
                        dest="disable_cache",
                        help="If the flag is not set, horovodrun will perform "
                             "the initialization checks only once every 60 "
                             "minutes -- if the checks successfully pass. "
                             "Otherwise, all the checks will run every time "
                             "horovodrun is called.")

    parser.add_argument('--start-timeout', action="store",
                        dest="start_timeout", type=int,
                        help="Horovodrun has to perform all the checks and "
                             "start the processes before the specified "
                             "timeout. The default value is 30 seconds. "
                             "Alternatively, The environment variable "
                             "HOROVOD_START_TIMEOUT can also be used to "
                             "specify the initialization timeout.")

    parser.add_argument('--verbose', action="store_true",
                        dest="verbose",
                        help="If this flag is set, extra messages will "
                             "printed.")

    parser.add_argument('command', nargs=argparse.REMAINDER,
                        help="Command to be executed.")

    parsed_args = parser.parse_args()

    if parsed_args.verbose is None:
        # This happens if the user does "horovodrun --verbose" without
        # specifying any value to verbose. For the sake of consistency, we set
        # the verbosity here to the default value of 1.
        parsed_args.verbose = 1

    if not parsed_args.version and not parsed_args.np:
        parser.error('argument -np/--num-proc is required')

    return parsed_args


def run_from_main():
    args = parse_args()

    if args.version:
        print(horovod.__version__)
        exit(0)

    _run(
        mode=0,
        np=args.np,
        ssh_port=args.ssh_port,
        host=args.host,
        disable_cache=args.disable_cache,
        start_timeout=args.start_timeout,
        verbose=args.verbose,
        command=args.command,
        wdir=None)


def run_from_notebook(
        proc_fn,
        proc_fn_args,
        num_proc,
        ssh_port,
        host,
        disable_cache,
        start_timeout,
        verbose):

    wdir = os.path.join(_MPI_ROOT_WDIR, 'hvd_run_' + str(uuid.uuid4()))
    os.makedirs(wdir)
    if verbose:
        print("mpirun working dir is " + wdir)

    # Invokes proc_fn with args. So we don't need to pickle them separately.
    def wrapped_proc_fn(rank=0):
        return_value = proc_fn(*proc_fn_args)
        if rank == 0:
            with open(_PICKLED_RESULT_FILENAME, 'wb') as f:
                try:
                    cloudpickle.dump(return_value, f)
                except Exception as e:
                    raise RuntimeError("Caught an excpetion while pickling "
                                       "return value: {}".format(repr(e)))

    pickled_proc_fn_str = cloudpickle.dumps(wrapped_proc_fn)
    pickled_proc_fn_path = os.path.join(wdir, _PICKLED_PROC_FN_FILENAME)
    with open(pickled_proc_fn_path, 'wb') as f:
        f.write(pickled_proc_fn_str)

    localhostname = socket.gethostname()
    launcher_path = os.pash.join(wdir, _PROC_LAUNCHER_FILENAME)
    with open(launcher_path, 'w') as f:
        f.write(textwrap.dedent("""
        set -e
        cd {wdir}
        rank=${{OMPI_COMM_WORLD_RANK:-0}}
        {exec} -c "import cloudpickle; cloudpickle.load(open('{fn_file}', 'rb'))(rank=$rank)"
        if [[ "$rank" -eq 0 ]]; then
        scp -q -p {ssh_port} -o StrictHostKeyChecking=no \
        {wdir}/{local_result} {local}:{wdir}/{result}
        fi
        """.format(wdir=wdir,
                   exec=sys.executable,
                   fn_file=_PICKLED_PROC_FN_FILENAME,
                   local=localhostname,
                   local_result=_LOCAL_PICKLED_RESULT_FILENAME,
                   result=_PICKLED_RESULT_FILENAME)))

    os.chmod(launcher_path, '0o777')

    _run(
        mode=1,
        np=num_proc,
        ssh_port=ssh_port,
        host=host,
        disable_cache=disable_cache,
        start_timeout=start_timeout,
        verbose=verbose,
        command=None,
        wdir=wdir)

    result_path = os.path.join(wdir, _PICKLED_RESULT_FILENAME)
    with open(result_path, 'rb') as f:
        result_bytes = f.read()

    return cloudpickle.loads(result_bytes)


def _run(mode, np, ssh_port, host, disable_cache, start_timeout, verbose, command, wdir):

    if host:
        all_host_names = [x for x in
                          [y.split(':')[0] for y in host.split(',')]]
    else:
        all_host_names = []

    # horovodrun has to finish all the checks before this timeout runs out.
    if not start_timeout:
        # Lookup default timeout from the environment variable.
        start_timeout = int(os.getenv('HOROVOD_START_TIMEOUT', '30'))

    tmout = timeout.Timeout(start_timeout,
                            message='Timed out waiting for {activity}. Please '
                                    'check connectivity between servers. You '
                                    'may need to increase the --start-timeout '
                                    'parameter if you have too many servers.')
    settings = hvd_settings.Settings(verbose=2 if verbose else 0,
                                     ssh_port=ssh_port,
                                     key=secret.make_secret_key(),
                                     timeout=tmout,
                                     num_hosts=len(all_host_names),
                                     num_proc=np)

    # This cache stores the results of checks performed by horovodrun
    # during the initialization step. It can be disabled by setting
    # --disable-cache flag.
    fn_cache = None
    if not disable_cache:
        params = ''
        if np:
            params += str(np) + ' '
        if host:
            params += str(host) + ' '
        if ssh_port:
            params += str(ssh_port)
        parameters_hash = hashlib.md5(params.encode('utf-8')).hexdigest()
        fn_cache = cache.Cache(CACHE_FOLDER, CACHE_STALENESS_THRESHOLD_MINUTES,
                               parameters_hash)

    remote_host_names = []
    if host:
        if settings.verbose >= 2:
            print("Filtering local host names.")
        remote_host_names = network.filter_local_addresses(all_host_names)

        if len(remote_host_names) > 0:
            if settings.verbose >= 2:
                print("Checking ssh on all remote hosts.")
            # Check if we can ssh into all remote hosts successfully.
            _check_all_hosts_ssh_successful(remote_host_names, ssh_port,
                                            fn_cache=fn_cache)
            if settings.verbose >= 2:
                print("SSH was successful into all the remote hosts.")

            if mode == 1:
                # Copy working dir to all remote machines
                for remote_host in set(remote_host_names):
                    scp_cmd = "scp -r -q -p {ssh_port} -o StrictHostKeyChecking=no " \
                              "{wdir} {remote_host}:{root_wdir}" \
                                .format(ssh_port=ssh_port, wdir=wdir,
                                        remote_host=remote_host, root_wdir=_MPI_ROOT_WDIR)
                    output = six.StringIO()
                    exit_code = safe_shell_exec.execute(scp_cmd, stdout=output, stderr=output)
                    if exit_code != 0:
                        output_msg = output.getvalue()
                        raise RuntimeError("Copy working dir to remote host {remote_host} failed."
                                           "Error message is {msg}".format(
                                            remote_host=remote_host, msg=output_msg))

        hosts_arg = "-H {hosts}".format(hosts=host)
    else:
        # if user does not specify any hosts, mpirun by default uses local host.
        # There is no need to specify localhost.
        hosts_arg = ""

    if host and len(remote_host_names) > 0:
        if settings.verbose >= 2:
            print("Testing interfaces on all the hosts.")

        local_host_names = set(all_host_names) - set(remote_host_names)
        # Find the set of common, routed interfaces on all the hosts (remote
        # and local) and specify it in the args to be used by NCCL. It is
        # expected that the following function will find at least one interface
        # otherwise, it will raise an exception.
        common_intfs = _driver_fn(all_host_names, local_host_names,
                                  settings, fn_cache=fn_cache)

        tcp_intf_arg = "-mca btl_tcp_if_include {common_intfs}".format(
            common_intfs=','.join(common_intfs))
        nccl_socket_intf_arg = "-x NCCL_SOCKET_IFNAME={common_intfs}".format(
            common_intfs=','.join(common_intfs))

        if settings.verbose >= 2:
            print("Interfaces on all the hosts were successfully checked.")
    else:
        # If all the given hosts are local, no need to specify the interfaces
        # because MPI does not use network for local execution.
        tcp_intf_arg = ""
        nccl_socket_intf_arg = ""

    # Pass all the env variables to the mpirun command.
    env = os.environ.copy()

    # Pass secret key through the environment variables.
    env[secret.HOROVOD_SECRET_KEY] = codec.dumps_base64(settings.key)

    if not _is_open_mpi_installed():
        raise Exception(
            'horovodrun convenience script currently only supports '
            'Open MPI.\n\n'
            'Choose one of:\n'
            '1. Install Open MPI 4.0.0+ and re-install Horovod '
            '(use --no-cache-dir pip option).\n'
            '2. Run distributed '
            'training script using the standard way provided by your'
            ' MPI distribution (usually mpirun, srun, or jsrun).')

    if ssh_port:
        ssh_port_arg = "-mca plm_rsh_args \"-p {ssh_port}\"".format(
            ssh_port=ssh_port)
    else:
        ssh_port_arg = ""

    if mode == 0:
        wdir_arg = ""
    else:
        wdir_arg = "-wdir {wdir}".format(wdir=wdir)
        command = _PROC_LAUNCHER_FILENAME

    mpirun_command = (
        'mpirun --allow-run-as-root --tag-output '
        '-np {num_proc} {hosts_arg} '
        '-bind-to none -map-by slot '
        '-mca pml ob1 -mca btl ^openib '
        '{ssh_port_arg} '
        '{tcp_intf_arg} '
        '-x NCCL_DEBUG=INFO '
        '{nccl_socket_intf_arg} '
        '{wdir_arg} '
        '{env} {command}'  # expect a lot of environment variables
            .format(num_proc=settings.num_proc,
                    hosts_arg=hosts_arg,
                    tcp_intf_arg=tcp_intf_arg,
                    nccl_socket_intf_arg=nccl_socket_intf_arg,
                    ssh_port_arg=ssh_port_arg,
                    wdir_arg=wdir_arg,
                    env=' '.join('-x %s' % key for key in env.keys()
                                 if env_util.is_exportable(key)),
                    command=' '.join(quote(par) for par in command))
    )

    if settings.verbose >= 2:
        print(mpirun_command)

    if mode == 0:
        # Execute the mpirun command.
        os.execve('/bin/sh', ['/bin/sh', '-c', mpirun_command], env)
    else:
        # Execute the mpirun command in subprocess and get result back from rank-0 process.
        output = six.StringIO()
        exit_code = safe_shell_exec.execute(mpirun_command, stdout=output, stderr=output)
        if exit_code != 0:
            output_msg = output.getvalue()
            raise RuntimeError("mpi run failed. Exit code is {exit_code}"
                               "Output message is {output}"
                               .format(exit_code=exit_code, output=output_msg))


if __name__ == "__main__":
    run_from_main()
