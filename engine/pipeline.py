import logging
from typing import Any, Dict

logger = logging.getLogger("digital_pulpit")


def _wrap_result(run_type: str, result: Any) -> Dict[str, Any]:
    """
    Normalize pipeline return values into a UI-safe dict.

    Supported underlying returns:
    - int run_id
    - dict containing at least run_id, optionally ok/status/notes/etc.
    """
    if isinstance(result, dict):
        # Respect underlying 'ok' if present; otherwise assume ok.
        ok = bool(result.get("ok", True))
        out = {"ok": ok, "run_type": run_type}
        out.update(result)
        # Ensure run_type is consistent
        out["run_type"] = run_type
        return out

    # Common legacy behavior: just return run_id
    if isinstance(result, int):
        return {"ok": True, "run_type": run_type, "run_id": result}

    # Unexpected return type
    return {
        "ok":
        False,
        "run_type":
        run_type,
        "run_id":
        None,
        "error":
        f"Unexpected return type from {run_type}: {type(result).__name__}",
    }


def run_vacuum():
    """
    UI-safe wrapper for the Vacuum pipeline.
    """
    try:
        from engine.vacuum import run_vacuum as _run_vacuum
        result = _run_vacuum()
        return _wrap_result("vacuum", result)
    except Exception as e:
        logger.exception("Vacuum failed")
        return {
            "ok": False,
            "run_type": "vacuum",
            "run_id": None,
            "error": str(e)
        }


def run_brain():
    """
    UI-safe wrapper for the Brain pipeline.
    """
    try:
        from engine.brain import run_brain as _run_brain
        result = _run_brain()
        return _wrap_result("brain", result)
    except Exception as e:
        logger.exception("Brain failed")
        return {
            "ok": False,
            "run_type": "brain",
            "run_id": None,
            "error": str(e)
        }


def run_assembly():
    """
    UI-safe wrapper for the Assembly pipeline.
    """
    try:
        from engine.assembly import run_assembly as _run_assembly
        result = _run_assembly()
        return _wrap_result("assembly", result)
    except Exception as e:
        logger.exception("Assembly failed")
        return {
            "ok": False,
            "run_type": "assembly",
            "run_id": None,
            "error": str(e)
        }


def run_all():
    """
    Runs Vacuum -> Brain -> Assembly in sequence.
    Returns a dict containing each sub-result.
    """
    results: Dict[str, Any] = {"ok": True, "run_type": "all"}

    v = run_vacuum()
    results["vacuum"] = v
    if not v.get("ok"):
        results["ok"] = False
        return results

    b = run_brain()
    results["brain"] = b
    if not b.get("ok"):
        results["ok"] = False
        return results

    a = run_assembly()
    results["assembly"] = a
    if not a.get("ok"):
        results["ok"] = False
        return results

    return results
