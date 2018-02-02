import threading
import queue

class QIQOWorker(threading.Thread):
    """Gets a value from an input Queue, pass the value to the worker_fun
    and add the returned function to the output Queue. If the worker_fun
    returns False, that element is not added to the output queue.

    The thread is stopped when the stop_event is set.
    """
    def __init__(self, worker_fun, queue_in, queue_out, stop_event, *args, **kwargs):
        threading.Thread.__init__(self)
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.worker_fun = worker_fun
        self.args = args
        self.kwargs = kwargs
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            # get a proxy from queue
            try:
                raw_product = self.queue_in.get(timeout=1)
            except queue.Empty:  # if queue is empty, return to the start of the loop
                continue

            # do work on the raw product
            refined_product = self.worker_fun(raw_product, *self.args, **self.kwargs)

            # if the proxy is fast, add it to the output queue
            if refined_product is not False:
                self.out_queue.put(refined_product)

            # all is done
            self.queue.task_done()
