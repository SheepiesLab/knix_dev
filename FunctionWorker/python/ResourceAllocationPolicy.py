from FunctionWorkerPool import PoolBusyException

class BaseResourceAllocationPolicy:
    def __init__(self, entry_topic, worker_params, pools):
        self._entry_topic = entry_topic
        self._current_topics = [entry_topic]
        self._worker_params = worker_params
        self._pools = pools
        self._resource_map = {}

    def on_execution_init(self):
        raise NotImplementedError

    def on_execution_tick(self):
        raise NotImplementedError

    def on_execution_progress(self):
        raise NotImplementedError

    def on_execution_exit(self):
        raise NotImplementedError

class PreAllocateResourcePolicy(BaseResourceAllocationPolicy):
    def __init__(self, entry_topic, worker_params, pools):
        super().__init__(entry_topic, worker_params, pools)

    def on_execution_init(self):
        for topic in self._worker_params.keys():
            while True:
                try:
                    self._resource_map[topic] = self._pools[topic].allocate_worker()
                    break
                except PoolBusyException:
                    pass
        return self._resource_map

    def on_execution_tick(self):
        return self._resource_map

    def on_execution_progress(self, next_topic):
        return self._resource_map

    def on_execution_exit(self):
        for topic in self._worker_params.keys():
            self._pools[topic].free_worker(self._resource_map[topic][0])