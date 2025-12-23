"""API route modules."""

from minimodal.server.routes.health import router as health_router, init_health_router
from minimodal.server.routes.functions import router as functions_router, init_functions_router
from minimodal.server.routes.workers import router as workers_router, init_workers_router
from minimodal.server.routes.volumes import router as volumes_router, init_volumes_router
from minimodal.server.routes.secrets import router as secrets_router, init_secrets_router
from minimodal.server.routes.users import router as users_router, init_users_router
from minimodal.server.routes.web_endpoints import router as web_endpoints_router, init_web_endpoints_router
from minimodal.server.routes.schedules import router as schedules_router, init_schedules_router
from minimodal.server.routes.dashboard import router as dashboard_router, init_dashboard_router

__all__ = [
    # Routers
    "health_router",
    "functions_router",
    "workers_router",
    "volumes_router",
    "secrets_router",
    "users_router",
    "web_endpoints_router",
    "schedules_router",
    "dashboard_router",
    # Init functions
    "init_health_router",
    "init_functions_router",
    "init_workers_router",
    "init_volumes_router",
    "init_secrets_router",
    "init_users_router",
    "init_web_endpoints_router",
    "init_schedules_router",
    "init_dashboard_router",
]
