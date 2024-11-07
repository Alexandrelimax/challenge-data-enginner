from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.dataform import DataformHook

class DataformWorkflowTaskGroup(TaskGroup):
    def __init__(self, group_id, project_id, region, repository_id, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        hook = DataformHook()
        @task(task_group=self)
        def create_compilation_result():
            compilation_result = {}
            result = hook.create_compilation_result(
                project_id=project_id,
                region=region,
                repository_id=repository_id,
                compilation_result=compilation_result
            )
            return result.name

        @task(task_group=self)
        def start_workflow_invocation(compilation_result_name):
            workflow_invocation = {
                'compilation_result': compilation_result_name,
            }
            invocation = hook.create_workflow_invocation(
                project_id=project_id,
                region=region,
                repository_id=repository_id,
                workflow_invocation=workflow_invocation
            )
            return invocation.name

        @task(task_group=self)
        def monitor_workflow_invocation(workflow_invocation_name):
            hook.wait_for_workflow_invocation(
                workflow_invocation_id=workflow_invocation_name,
                repository_id=repository_id,
                project_id=project_id,
                region=region,
                wait_time=10,
                timeout=600
            )
            return f"Workflow {workflow_invocation_name} completed successfully."

        compilation_result_name = create_compilation_result()
        workflow_invocation_name = start_workflow_invocation(compilation_result_name)
        self.monitor_task = monitor_workflow_invocation(workflow_invocation_name)
