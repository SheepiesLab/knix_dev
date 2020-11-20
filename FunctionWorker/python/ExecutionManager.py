from LocalQueueClient import LocalQueueClient, LocalQueueMessage
from LocalQueueClientMessage import LocalQueueClientMessage
from FunctionWorkerPool import FunctionWorkerPool
from MicroFunctionsLogWriter import MicroFunctionsLogWriter

import py3utils

import json
import sys
import logging
import socket
import time

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
        self._workflow_topics = eman_args["workflowtopics"]

        self._topic = self._sandboxid + "-" + self._workflowid + "-MFNExecutionManager"
        self._setup_loggers()

        self._executions = {}

        self._pools = {}
        for topic in self._workflow_topics:
            self._pools[topic] = FunctionWorkerPool(
                self._worker_params[topic], self._worker_states[topic], self._queue, self._logger,  5)

        self._local_queue_client = LocalQueueClient(connect=self._queue)

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
``
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
        self._executions[key] = Execution(key, value)

    def _process_update(self, value):
        try:
            update = json.loads(value)
            action = update["action"]

            #self._logger.debug("New update: %s", update)

            if action == "stop":
                self._exit()
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
                self._add_new_execution(key, value)
                self._logger.info("New Execution %s", key)
        except Exception as exc:
            self._logger.exception("Exception in handling: %s", str(exc))
            sys.stdout.flush()

    def _handle_self_message(self, lqm):
        try:
            lqcm = LocalQueueClientMessage(lqm=lqm)
            key = lqcm.get_key()
            value = lqcm.get_value()
            self._executions[key].on_worker_return(value)
        except Exception as exc:
            self._logger.exception("Exception in handling: %s", str(exc))
            sys.stdout.flush()

    def _get_message(self, topic):
        return self._local_queue_client.getMessage(topic, self._POLL_TIMEOUT)

    def _loop(self):
        # Read EntryTopic, create new Execution for requests
        lqm = self._get_message(self._wf_entry)
        if lqm is not None:
            self._handle_entry_message(lqm)
        # Read ExecutionManagerTopic, update executions accordingly
        lqm = self._get_message(self._topic)
        if lqm is not None:
            self._handle_self_message(lqm)

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


class Execution:
    def __init__(self, key, encapsulated_value):
        pass

    def on_worker_return(self, value):
        for output in value:
            next_topic = output["next"]
            encapsulated_value = output["value"]
        # Determine resource requests
        # Determine next execution
        pass


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

