from collections import namedtuple

from huey.exceptions import HueyException


Message = namedtuple('Message', ('id', 'name', 'eta', 'retries', 'retry_delay',
                                 'args', 'kwargs', 'on_complete', 'on_error'))


class Registry(object):
    def __init__(self):
        self._registry = {}
        self._periodic_tasks = []

    def task_to_string(self, task_class):
        return '%s.%s' % (task_class.__module__, task_class.__name__)

    def register(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str in self._registry:
            raise ValueError('Attempting to register a task with the same '
                             'identifier as existing task. Specify a different'
                             ' name= to register this task. "%s"' % task_str)

        self._registry[task_str] = task_class
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        return True

    def unregister(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str not in self._registry:
            return False

        del self._registry[task_str]
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks = [t for t in self._periodic_tasks
                                    if t is not task_class]
        return True

    def string_to_task(self, task_str):
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)
        return self._registry[task_str]

    def create_message(self, task):
        task_str = self.task_to_string(type(task))
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)

        # Remove the "task" instance from any arguments before serializing.
        if task.kwargs and 'task' in task.kwargs:
            task.kwargs.pop('task')

        on_complete = None
        if task.on_complete is not None:
            on_complete = self.create_message(task.on_complete)

        on_error = None
        if task.on_error is not None:
            on_error = self.create_message(task.on_error)

        return Message(
            task.id,
            task_str,
            task.eta,
            task.retries,
            task.retry_delay,
            task.args,
            task.kwargs,
            on_complete,
            on_error)

    def create_task(self, message):
        TaskClass = self.string_to_task(message.name)

        on_complete = None
        if message.on_complete is not None:
            on_complete = self.create_task(message.on_complete)

        on_error = None
        if message.on_error is not None:
            on_error = self.create_task(message.on_error)

        return TaskClass(
            message.args,
            message.kwargs,
            message.id,
            message.eta,
            message.retries,
            message.retry_delay,
            on_complete,
            on_error)

    @property
    def periodic_tasks(self):
        return [task_class() for task_class in self._periodic_tasks]
