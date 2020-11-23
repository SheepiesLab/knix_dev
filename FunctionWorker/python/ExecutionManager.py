from LocalQueueClient import LocalQueueClient, LocalQueueMessage
from LocalQueueClientMessage import LocalQueueClientMessage
from FunctionWorkerPool import FunctionWorkerPool
from MicroFunctionsLogWriter import MicroFunctionsLogWriter
from PublicationUtils import PublicationUtils
from StateUtils import StateUtils
from ResourceAllocationPolicy import PreAllocateResourcePolicy
from PoolPolicy import FixedPoolPolicy

import py3utils

import json
import sys
import logging
import socket
import time
import queue

LOGGER_HOSTNAME = 'hostname-unset'
LOGGER_CONTAINERNAME = 'containername-unset'
LOGGER_UUID = '0l'
LOGGER_USERID = 'userid-unset'
LOGGER_WORKFLOWNAME = 'workflow-name-unset'
LOGGER_WORKFLOWID = 'workflow-id-unset'


class LoggingFilter(logging.Filter):
    def filter(self, record):
        global LOGGER_HOSTNAME
        global LOGGER_CONTAINERNAME
        global LOGGER_UUID
        global LOGGER_USERID
        global LOGGER_WORKFLOWNAME
        global LOGGER_WORKFLOWID
        record.timestamp = time.time()*1000000
        record.hostname = LOGGER_HOSTNAME
        record.containername = LOGGER_CONTAINERNAME
        record.uuid = LOGGER_UUID
        record.userid = LOGGER_USERID
        record.workflowname = LOGGER_WORKFLOWNAME
        record.workflowid = LOGGER_WORKFLOWID
        return True


class ExecutionManager:
    def __init__(self, eman_args):

        self._POLL_TIMEOUT = py3utils.ensure_long(10000)

        self._queue = eman_args["queue"]
        self._sandboxid = eman_args["sandboxid"]
        self._workflowid = eman_args["workflowid"]
        self._wf_exit = eman_args["workflowexit"]
        self._wf_entry = eman_args["workflowentry"]
        self._hostname = eman_args["hostname"]
        self._userid = eman_args["userid"]
        self._workflowname = eman_args["workflowname"]
        self._worker_params = eman_args["workerparams"]
        self._worker_states = eman_args["workerstates"]
        self._new_execution_queue = queue.Queue(0) # TODO configure max queue size

        self._topic = self._sandboxid + "-" + self._workflowid + "-MFNExecutionManager"
        self._setup_loggers()

        self._executions = {}
        self._prefix = self._sandboxid + "-" + self._workflowid + "-"

        self._pools = {}
        for topic in self._worker_params.keys():
            self._pools[topic] = FunctionWorkerPool(
                topic, self._worker_params[topic], self._worker_states[topic], self._queue, self._logger,  5, FixedPoolPolicy)

        self._local_queue_client = LocalQueueClient(connect=self._queue)
        self._exit_topic = self._prefix + self._wf_exit
        self._exit_listen_topic = self._exit_topic + '-em'
        self._entry_listen_topic = self._wf_entry
        self._local_queue_client.addTopic(self._exit_listen_topic)
        self._listen_topics = list(self._worker_params.keys())
        self._listen_topics.remove(self._entry_listen_topic)
        


    def _setup_loggers(self):
        global LOGGER_HOSTNAME
        global LOGGER_CONTAINERNAME
        global LOGGER_USERID
        global LOGGER_WORKFLOWNAME
        global LOGGER_WORKFLOWID

        LOGGER_HOSTNAME = self._hostname
        LOGGER_CONTAINERNAME = socket.gethostname()
        LOGGER_USERID = self._userid
        LOGGER_WORKFLOWNAME = self._workflowname
        LOGGER_WORKFLOWID = self._workflowid
        self._logger = logging.getLogger(self._topic)
        self._logger.setLevel(logging.INFO)
        self._logger.addFilter(LoggingFilter())

        formatter = logging.Formatter(
            "[%(timestamp)d] [%(levelname)s] [%(hostname)s] [%(containername)s] [%(uuid)s] [%(userid)s] [%(workflowname)s] [%(workflowid)s] [%(name)s] [%(asctime)s.%(msecs)03d] %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        logfile = '/opt/mfn/logs/execution_manager.log'

        hdlr = logging.FileHandler(logfile)
        hdlr.setLevel(logging.INFO)
        hdlr.setFormatter(formatter)
        self._logger.addHandler(hdlr)

        global print
        print = self._logger.info
        sys.stdout = MicroFunctionsLogWriter(self._logger, logging.INFO)
        sys.stderr = MicroFunctionsLogWriter(self._logger, logging.ERROR)

    def _add_new_execution(self, key, value):
        try:
            self._executions[key] = Execution(key, value, self._entry_listen_topic, self._worker_params, self._pools, PreAllocateResourcePolicy)
        except NoResourceAvailableException as e:
            try:
                self._new_execution_queue.put_nowait((key, value))
            except queue.Full:
                pass
            raise e

    def _process_update(self, value):
        try:
            update = json.loads(value)
            action = update["action"]

            #self._logger.debug("New update: %s", update)

            if action == "stop":
                self._exit()
            elif action == "update-local-functions":
                for p in self._pools.values():
                    p.update_workers(update)
        except Exception as exc:
            self._logger.error(
                "Could not parse update message: %s; ignored...", str(exc))

    def _handle_entry_message(self, lqm):
        try:
            lqcm = LocalQueueClientMessage(lqm=lqm)
            key = lqcm.get_key()
            value = lqcm.get_value()
            if key == "0l":
                self._process_update(value)
            else:
                try:
                    self._add_new_execution(key, value)
                    self._logger.info("New Execution %s created", key)
                except NoResourceAvailableException:
                    self._logger.info("New Execution %s in queue", key)

        except Exception as exc:
            self._logger.exception("Exception in handling: %s", str(exc))
            sys.stdout.flush()

    def _handle_exit_message(self, lqm):
        try:
            lqcm = LocalQueueClientMessage(lqm=lqm)
            key = lqcm.get_key()
            value = lqcm.get_value()
            self._executions[key].on_exit_return(value)
            self._executions.pop(key)
            self._logger.info("Execution exit: %s", key)
        except Exception as exc:
            self._logger.exception("Exception in handling: %s", str(exc))
            sys.stdout.flush()

    def _handle_function_progress_message(self, lqm):
        try:
            lqcm = LocalQueueClientMessage(lqm=lqm)
            key = lqcm.get_key()
            value = lqcm.get_value()
            if key == "0l":
                self._process_update(value)
                self._logger.info("Process update")
            else:
                self._executions[key].on_worker_progress(value)
                self._logger.info("Execution progress: %s", key)
        except Exception as exc:
            self._logger.exception("Exception in handling: %s", str(exc))
            sys.stdout.flush()

    def _get_message(self, topic):
        return self._local_queue_client.getMessage(topic, self._POLL_TIMEOUT)

    def _loop(self):
        # Try kill the queue
        while True:
            key, value = (None, None)
            try:
                key, value = self._new_execution_queue.get_nowait()
            except queue.Empty:
                break

            try:
                self._add_new_execution(key, value)
                self._logger.info("New Execution %s created", key)
            except NoResourceAvailableException:
                self._logger.info("New Execution %s in queue", key)
                break

        # Read EntryTopic, create new Execution for requests
        lqm = self._get_message(self._entry_listen_topic)
        if lqm is not None:
            self._logger.info("New Entry Message")
            self._handle_entry_message(lqm)

        # Read ExecutionManagerTopic, update executions accordingly
        for t in self._listen_topics:
            lqm = self._get_message(t)
            if lqm is not None:
                self._logger.info("New Progress Message")
                self._handle_function_progress_message(lqm)

        # Read ExitTopic
        lqm = self._get_message(self._exit_listen_topic)
        if lqm is not None:
            self._logger.info("New Exit Message")
            self._handle_exit_message(lqm)
        
        for e in self._executions.values():
            e.tick()
        

    def _exit(self):
        for topic, pool in self._pools.items():
            try:
                pool._exit()
            except Exception as exc:
                self._logger.exception(
                    "Exception in shutting down pool %s: %s", topic, str(exc))
                sys.stdout.flush()

        self._local_queue_client.shutdown()
        self._running = False

    def run(self):
        self._running = True
        while self._running:
            self._loop()

class NoResourceAvailableException(Exception):
    pass

class Execution:
    def __init__(self, key, encapsulated_value, entry_topic, worker_params, pools, resource_policy):
        self._key = key
        self._policy = resource_policy(entry_topic, worker_params, pools)
        self._resource_map = self._policy.on_execution_init()
        self._exec_queue = []
        self.on_worker_progress([{
            "next": entry_topic,
            "value": encapsulated_value
        }])

    def on_worker_progress(self, value):
        remaining = []
        for output in value:
            next_topic = output["next"]
            encapsulated_value = output["value"]
            self._resource_map = self._policy.on_execution_progress(next_topic)
            if next_topic in self._resource_map:
                self._resource_map[next_topic][1].execute(self._key, encapsulated_value)
            else:
                remaining.append(output)
        if len(remaining) > 0:
            self._exec_queue.append(remaining)

    def on_exit_return(self, value):
        self._policy.on_execution_exit()

    def tick(self):
        self._resource_map = self._policy.on_execution_tick()
        q = self._exec_queue
        self._exec_queue = []
        for v in q:
            self.on_worker_progress(v)



def main():
    params_filename = sys.argv[1]
    with open(params_filename, "r") as paramsf:
        params = json.load(paramsf)

    # create a thread with local queue consumer and subscription
    try:
        em = ExecutionManager(params)
        em.run()
    except Exception as exc:
        raise

if __name__ == '__main__':
    main()

