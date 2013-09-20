from celery import AsyncResult
from tastypie.resources import Resource


class CeleryTaskObject(object):
    def __init__(self, uuid=None):
        self.uuid = uuid
        self.obj = self.get_obj()

    def get_obj(self):
        if self.uuid is None:
            return None
        return AsyncResult(self.uuid)

    @property
    def status(self):
        return self.obj.status

    @property
    def result(self):
        if self.obj.ready():
            return self.obj.result
        else:
            return None

    @property
    def ready(self):
        return self.obj.ready()

    @property
    def successful(self):
        return self.obj.successful()

    @property
    def failed(self):
        return self.obj.failed()

    def forget(self):
        self.obj.forget()


class AsyncTaskResource(Resource):
    def dispatch(self, request_type, request, **kwargs):
        # Check to see if the request is allowed, and if it is, if there's a
        # task defined for that type of method. If there is no task, just
        # return as normal; if there is, execute that task instead.
        allowed_methods = getattr(
            self._meta, '{}_allowed_methods'.format(request_type), None)
        request_method = self.method_check(request, allowed=allowed_methods)
        request_method_name = '{}_{}'.format(request_method, request_type)

        task = self.get_task(request_method_name)
        if task is None:
            return super(AsyncTaskResource, self).dispatch(
                request_type, request, **kwargs)

        self.is_authenticated(request)
        self.throttle_check(request)
        self.log_throttled_access(request)

        result_id = task.delay(request, **kwargs).id
        result = CeleryTaskObject(result_id)

        # If the task has completed, everything should be in a state where the
        # regular Tastypie methods should return the right result.
        if result.ready:
            return super(AsyncTaskResource, self).dispatch(
                request_type, request, **kwargs)

        # If the task is still executing, return a simple status response.
        response = self.create_response(request, {
            'uuid': result.uuid,
            'status': result.status,
            'result': result.result,
            'ready': result.ready,
            'successful': result.successful,
            'failed': result.failed,
        })
        return response

    def get_task(self, request_method_name):
        # Get and return the appropriate task if it's defined, otherwise None.
        task_name = '{}_task'.format(request_method_name)
        task = getattr(self._meta, task_name, getattr(self._meta, 'task', None))
        return task
