import importlib.util
import os

from app.core.celery_app import celery_app


@celery_app.task(bind=True)
def run_computation_task(self, script_filename: str, params: dict):
    try:
        # Script path - script_filename expected to include extension or not?
        # The API removes extension. Let's assume input implies .py if missing or reconstruct it.
        # However, the API logic was: module_name = filename[:-3].
        # So script_filename here is NO extension.

        file_path = os.path.join("app/computations", f"{script_filename}.py")

        if not os.path.exists(file_path):
            return {"error": f"Script file {file_path} not found."}

        # Dynamically load module from file path
        spec = importlib.util.spec_from_file_location(script_filename, file_path)
        if spec is None or spec.loader is None:
            return {"error": f"Could not load spec for {script_filename}"}

        module = importlib.util.module_from_spec(spec)
        # Did not register in sys.modules to avoid pollution in multi-threaded environment
        spec.loader.exec_module(module)

        if not hasattr(module, "run"):
            return {
                "error": f"Script {script_filename} does not have a 'run' function."
            }

        result = module.run(params)
        return result
    except Exception as e:
        return {"error": f"Execution error: {str(e)}"}
