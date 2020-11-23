import process_utils
from LocalQueueClient import LocalQueueClient
from LocalQueueClientMessage import LocalQueueClientMessage
from PoolPolicy import FixedPoolPolicy

import os
import sys
import json

SINGLE_JVM_FOR_FUNCTIONS = True


class PoolBusyException(Exception):
    pass


class FunctionWorkerPool:
    def __init__(self, topic, worker_params, state, queue, logger, initial_target, pool_policy):
        self._workers = {}
        self._workers_in_use = {}
        self._logger = logger
        self._worker_params = worker_params
        self._state = state
        self._queue = queue
        self._topic = topic
        self._policy = pool_policy(self)
        self._local_queue_client = LocalQueueClient(connect=self._queue)
        for _ in range(initial_target):
            self.add_worker()

    def add_worker(self):
        new_worker = FunctionWorkerHandle(
            self._worker_params, self._state, self._queue, self._logger)
        if not new_worker.failed():
            self._workers[new_worker.getpid()] = new_worker

    def allocate_worker(self):
        if len(self._workers) > 0:
            for pid, worker_handle in self._workers.items():
                self._workers.pop(pid)
                self._workers_in_use[pid] = worker_handle
                self._policy.on_pool_allocate()
                return pid, worker_handle
        else:
            realloc = self._policy.on_pool_busy()
            if realloc:
                return self.allocate_worker()
            else:
                raise PoolBusyException

    def free_worker(self, pid):
        if pid in self._workers_in_use.keys():
            handle = self._workers_in_use.pop(pid)
            self._workers[pid] = handle
            self._policy.on_pool_free()

    def update_workers(self, update):
        for w in self._workers.values():
            w.update(json.dumps(update))

    def tick(self):
        self._policy.on_pool_tick()

    def _exit(self):
        for w in self._workers.values():
            w._exit()


class FunctionWorkerHandle:
    def __init__(self, worker_params, state, queue, logger):
        self._queue = queue
        self._logger = logger
        self._python_version = sys.version_info
        self._pid = None
        self._failed = False

        error, pyprocess, jvprocess = self._start_function_worker(
            worker_params, state["resource_runtime"], state["resource_env_var_list"])
        self._jvprocess = jvprocess
        if error is None and pyprocess is not None:
            self._process = pyprocess
            self._pid = self._process.pid
            self._topic = "{}-{}".format(worker_params["ftopic"], self._pid)
            self._local_queue_client = LocalQueueClient(connect=self._queue)
            self._local_queue_client.addTopic(self._topic)
        else:
            self._logger.error(error)
            self._failed = True

    def failed(self):
        return self._failed

    def getpid(self):
        return self._pid

    def _start_python_function_worker(self, worker_params, env_var_list):
        error = None
        process = None
        function_name = worker_params["fname"]
        state_name = worker_params["functionstatename"]
        custom_env = os.environ.copy()
        old_ld_library_path = ""
        if "LD_LIBRARY_PATH" in custom_env:
            old_ld_library_path = custom_env["LD_LIBRARY_PATH"]
        custom_env["LD_LIBRARY_PATH"] = "/opt/mfn/workflow/states/" + state_name + "/" + \
            function_name + ":/opt/mfn/workflow/states/" + \
            state_name + "/" + function_name + "/lib"

        if old_ld_library_path != "":
            custom_env["LD_LIBRARY_PATH"] = custom_env["LD_LIBRARY_PATH"] + \
                ":" + old_ld_library_path

        #custom_env["PYTHONPATH"] = "/opt/mfn/workflow/states/" + state_name + "/" + function_name

        for env_var in env_var_list:
            idx = env_var.find("=")
            if idx == -1:
                continue
            env_var_key = env_var[0:idx]
            env_var_value = env_var[idx+1:]
            custom_env[env_var_key] = env_var_value

        #self._logger.info("environment variables (after user env vars): %s", str(custom_env))

        if self._python_version >= (3, ):
            cmd = "python3 "
        else:
            cmd = "python "
        cmd = cmd + "/opt/mfn/FunctionWorker/python/FunctionWorker.py"
        # state_name can contain whitespace
        cmd = cmd + " " + '\"/opt/mfn/workflow/states/%s/worker_params.json\"' % state_name

        filename = '/opt/mfn/logs/function_' + state_name + '.log'
        log_handle = open(filename, 'a')

        # store command arguments for when/if we need to restart the process if it fails
        command_args_map = {}
        command_args_map["command"] = cmd
        command_args_map["custom_env"] = custom_env
        command_args_map["log_filename"] = filename

        #self._logger.info("Starting function worker: " + state_name + "  with stdout/stderr redirected to: " + filename)
        error, process = process_utils.run_command(
            cmd, self._logger, custom_env=custom_env, process_log_handle=log_handle)
        if error is None:
            self._logger.info("Started function worker: %s, pid: %s, with stdout/stderr redirected to: %s",
                              state_name, str(process.pid), filename)
        return error, process

    def _start_function_worker(self, worker_params, runtime, env_var_list):
        error = None
        pyprocess = None
        jvprocess = None

        if runtime.find("python") != -1:
            error, pyprocess = self._start_python_function_worker(
                worker_params, env_var_list)
        elif runtime.find("java") != -1:
            # TODO: environment/JVM variables need to be utilized by the java request handler, not by the function worker

            if SINGLE_JVM_FOR_FUNCTIONS:
                # _XXX_: we'll launch the single JVM handling all java functions later
                error, pyprocess = self._start_python_function_worker(
                    worker_params, env_var_list)
            else:
                # if jar, the contents have already been extracted as if it was a zip archive
                # start the java request handler if self._function_runtime == "java"
                # we wrote the parameters to json file at the state directory
                self._logger.info(
                    "Launching JavaRequestHandler for state: %s", worker_params["functionstatename"])
                cmdjavahandler = "java -jar /opt/mfn/JavaRequestHandler/target/javaworker.jar "
                cmdjavahandler += "/opt/mfn/workflow/states/" + \
                    worker_params["functionstatename"] + \
                    "/java_worker_params.json"

                error, jvprocess = process_utils.run_command(
                    cmdjavahandler, self._logger, wait_until="Waiting for requests on:")
                if error is not None:
                    error = "Could not launch JavaRequestHandler: " + \
                        worker_params["fname"] + " " + error
                    self._logger.error(error)
                else:
                    error, pyprocess = self._start_python_function_worker(
                        worker_params, env_var_list)
        else:
            error = "Unsupported function runtime: " + runtime

        return error, pyprocess, jvprocess

    def _wait_for_child_processes(self):
        output, error = process_utils.run_command_return_output(
            'pgrep -P ' + str(os.getpid()), self._logger)
        if error is not None:
            self._logger.error(
                "[FunctionWorkerPool] wait_for_child_processes: Failed to get children process ids: %s", str(error))
            return

        children_pids = set(output.split())
        self._logger.info("[FunctionWorkerPool] wait_for_child_processes: Parent pid: %s, Children pids: %s", str(
            os.getpid()), str(children_pids))

        if self._jvprocess is not None:
            if str(self._jvprocess.pid) in children_pids:
                children_pids.remove(str(self._jvprocess.pid))
                self._logger.info("[FunctionWorkerPool] wait_for_child_processes: Not waiting on JavaRequestHandler pid: %s", str(
                    self._jvprocess.pid))

        if not children_pids:
            self._logger.info(
                "[FunctionWorkerPool] wait_for_child_processes: No remaining pids to wait for")
            return

        while True:
            try:
                cpid, status = os.waitpid(-1, 0)
                self._logger.info(
                    "[FunctionWorkerPool] wait_for_child_processes: Status changed for pid: %s, Status: %s", str(cpid), str(status))
                if str(cpid) not in children_pids:
                    #print('wait_for_child_processes: ' + str(cpid) + "Not found in children_pids")
                    continue
                children_pids.remove(str(cpid))
                if not children_pids:
                    self._logger.info(
                        "[FunctionWorkerPool] wait_for_child_processes: No remaining pids to wait for")
                    break
            except Exception as exc:
                self._logger.error(
                    '[FunctionWorkerPool] wait_for_child_processes: %s', str(exc))

    def _exit(self):
        shutdown_message = {}
        shutdown_message["action"] = "stop"

        lqcm_shutdown = LocalQueueClientMessage(
            key="0l", value=json.dumps(shutdown_message))
        ack = self._local_queue_client.addMessage(
            self._topic, lqcm_shutdown, True)
        while not ack:
            ack = self._local_queue_client.addMessage(
                self._topic, lqcm_shutdown, True)

        self._logger.info("Waiting for function workers to shutdown")
        self._wait_for_child_processes()

        if self._jvprocess is not None:
            process_utils.terminate_and_wait_child(
                self._jvprocess, "JavaRequestHandler", 5, self._logger)

        self._local_queue_client.shutdown()
        pass

    def update(self, value):
        lqcm = LocalQueueClientMessage(key="0l", value=value)
        ack = self._local_queue_client.addMessage(self._topic, lqcm, True)
        while not ack:
            ack = self._local_queue_client.addMessage(self._topic, lqcm, True)

    def execute(self, key, encapsulated_value):
        lqcm = LocalQueueClientMessage(key=key, value=encapsulated_value)
        ack = self._local_queue_client.addMessage(self._topic, lqcm, True)
        while not ack:
            ack = self._local_queue_client.addMessage(self._topic, lqcm, True)
        self._logger.info("Execution request for %s to worker %s", key, self._topic)

    def swapout(self):
        pass

    def swapin(self):
        pass

    def inswap_ready(self):
        pass
