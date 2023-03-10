from multiprocessing import Queue
from threading import Thread

class ThreadedTask(Thread):

    def __init__(self, queue, app, task, args=None):
        Thread.__init__(self)
        self.queue = queue
        self.task = task
        self.args = args
        self.app = app

    def run(self):
        res = self.task(*self.args)
        return self.queue.put(res)