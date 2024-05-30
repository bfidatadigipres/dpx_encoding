from dagster import resource


@resource
def dpx_path_resource(init_context):
    return init_context.resource_config["dpx_path"]