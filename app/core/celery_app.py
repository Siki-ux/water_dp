from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "water_dp_worker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "app.tasks.import_tasks",
        "app.tasks.computation_tasks",
        "app.tasks.simulation_tasks",
    ],
)

celery_app.conf.task_routes = {
    "app.tasks.import_tasks.*": {"queue": "imports"},
    "app.tasks.computation_tasks.*": {"queue": "computations"},
    "app.tasks.simulation_tasks.*": {"queue": "celery"},
}

celery_app.conf.beat_schedule = {
    "run-simulation-step-every-10s": {
        "task": "app.tasks.simulation_tasks.run_simulation_step",
        "schedule": 10.0,
    },
}
