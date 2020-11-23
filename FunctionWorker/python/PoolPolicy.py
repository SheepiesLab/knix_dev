class BasePoolPolicy:
    def __init__(self, pool):
        self._pool = pool

    def on_pool_tick(self):
        raise NotImplementedError

    def on_pool_allocate(self):
        raise NotImplementedError

    def on_pool_free(self):
        raise NotImplementedError

    def on_pool_busy(self):
        '''
        Return a boolean to indicate restart allocation or else
        '''
        raise NotImplementedError

class FixedPoolPolicy(BasePoolPolicy):
    def __init__(self, pool):
        super().__init__(pool)

    def on_pool_tick(self):
        pass

    def on_pool_allocate(self):
        pass

    def on_pool_free(self):
        pass

    def on_pool_busy(self):
        return False