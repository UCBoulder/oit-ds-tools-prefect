"""Allows autodocumenting Prefect tasks"""

from sphinx.domains.python import PyFunction
from sphinx.ext.autodoc import FunctionDocumenter
from docutils import nodes

from prefect import Task


class TaskDocumenter(FunctionDocumenter):
    """Document task definitions."""

    objtype = "task"
    member_order = 11

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        return isinstance(member, Task)


class TaskDirective(PyFunction):
    """Sphinx task directive."""

    def get_signature_prefix(self, sig):
        prefix = self.env.config.task_prefix
        return [nodes.Text(prefix + " ")]


def autodoc_skip_member_handler(app, what, name, obj, skip, options):
    """Handler for autodoc-skip-member event."""

    if isinstance(obj, Task):
        if skip:
            return False
    return None


def setup(app):
    """Sphinx extension setup function"""

    app.add_autodocumenter(TaskDocumenter)
    app.add_directive_to_domain("py", "task", TaskDirective)
    app.connect("autodoc-skip-member", autodoc_skip_member_handler)
    app.add_config_value("task_prefix", "task", "env")

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
