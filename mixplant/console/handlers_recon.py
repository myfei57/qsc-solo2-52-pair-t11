"""Routes for the reconciliation watermark and the readable ledgers."""

from __future__ import annotations

from typing import Any

from ..errors import ValidationError
from .router import Request, Response, Router


def register(router: Router, runtime: Any, pages: Any) -> None:
    """Register the reconciliation routes."""

    router.add("GET", "/api/recon", lambda _: Response.json({"recon": runtime.recon.status()}))
    router.add("GET", "/api/recon/verify", lambda _: Response.json(runtime.recon_verify()))
    router.add("GET", "/api/ledger/external", _external(runtime))
    router.add("GET", "/api/ledger/internal", lambda _: Response.json(runtime.internal_ledger()))
    router.add("GET", "/api/ledger/history", _history(runtime))
    router.add("GET", "/api/conclusions/{ref}", _conclusion(runtime))

    for path, command in (
        ("/api/recon/reconcile", "recon.reconcile"),
        ("/api/recon/settle", "recon.settle"),
        ("/api/recon/through", "recon.reconcile_through"),
        ("/api/recon/void", "recon.void"),
        ("/api/recon/publish", "recon.publish"),
    ):
        router.add("POST", path, _command(runtime, command))


def _command(runtime: Any, command: str):
    def handler(request: Request) -> Response:
        return Response.json(runtime.dispatch(command, dict(request.body)))

    return handler


def _external(runtime: Any):
    def handler(request: Request) -> Response:
        return Response.json(runtime.external_ledger(dict(request.query)))

    return handler


def _history(runtime: Any):
    def handler(request: Request) -> Response:
        return Response.json(runtime.recon_history(dict(request.query)))

    return handler


def _conclusion(runtime: Any):
    def handler(request: Request) -> Response:
        ref = request.params.get("ref", "")
        if not ref:
            raise ValidationError("conclusion ref is required")
        return Response.json(runtime.recon.conclusion(ref))

    return handler
