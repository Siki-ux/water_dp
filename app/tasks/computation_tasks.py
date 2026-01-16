import importlib.util
import os

from app.core.celery_app import celery_app


@celery_app.task(bind=True)
def run_computation_task(
    self, script_filename: str, params: dict, script_id: str = None
):
    import uuid

    from app.computations.context import ComputationContext
    from app.core.database import SessionLocal
    from app.models.computations import ComputationScript

    db = SessionLocal()
    try:
        # Script path
        file_path = os.path.join("app/computations", f"{script_filename}.py")
        if not os.path.exists(file_path):
            return {"error": f"Script file {file_path} not found."}

        # Dynamically load module
        spec = importlib.util.spec_from_file_location(script_filename, file_path)
        if spec is None or spec.loader is None:
            return {"error": f"Could not load spec for {script_filename}"}

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "run"):
            return {
                "error": f"Script {script_filename} does not have a 'run' function."
            }

        # Setup Context
        # Resolving Script ID: Use explicit argument if provided (preferred), else lookup by filename
        resolved_script_id = None
        if script_id:
            try:
                resolved_script_id = uuid.UUID(str(script_id))
            except ValueError:
                # script_id is not a valid UUID, fallback to DB lookup
                pass

        if not resolved_script_id:
            # Fallback to DB lookup
            script = (
                db.query(ComputationScript)
                .filter(ComputationScript.filename == f"{script_filename}.py")
                .first()
            )
            resolved_script_id = script.id if script else uuid.uuid4()

        ctx = ComputationContext(db, self.request.id, resolved_script_id, params)

        # Execute with Context
        # Check if run accepts 1 arg (old) or ctx (new)
        # We can try/except or inspect signature.
        # For backward compatibility, let's inspect.
        import inspect

        sig = inspect.signature(module.run)
        if len(sig.parameters) == 1:
            # Check param name? 'ctx' vs 'params'
            param_name = list(sig.parameters.keys())[0]
            if param_name == "ctx":
                result = module.run(ctx)
            else:
                # Legacy mode
                result = module.run(params)
        else:
            # Assume new mode or failure
            result = module.run(ctx)

        # Post-Execution: Passive Alert Evaluation
        if isinstance(result, dict):
            from app.services.alert_evaluator import AlertEvaluator

            evaluator = AlertEvaluator(db)
            evaluator.evaluate_result(self.request.id, resolved_script_id, result)

        return result
    except Exception as e:
        return {"error": f"Execution error: {str(e)}"}
    finally:
        db.close()
