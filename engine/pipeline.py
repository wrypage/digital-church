import logging

logger = logging.getLogger("digital_pulpit")


def run_vacuum():
    """
    UI-safe wrapper for the Vacuum pipeline.
    Returns a small dict the dashboard can display.
    """
    from engine.vacuum import run_vacuum as _run_vacuum

    run_id = _run_vacuum()
    return {"ok": True, "run_type": "vacuum", "run_id": run_id}


def run_brain():
    """
    UI-safe wrapper for the Brain pipeline.
    """
    from engine.brain import run_brain as _run_brain

    run_id = _run_brain()
    return {"ok": True, "run_type": "brain", "run_id": run_id}


def run_assembly():
    """
    UI-safe wrapper for the Assembly pipeline.
    """
    from engine.assembly import run_assembly as _run_assembly

    run_id = _run_assembly()
    return {"ok": True, "run_type": "assembly", "run_id": run_id}


def run_all():
    """
    Runs Vacuum -> Brain -> Assembly in sequence.
    Returns a dict containing each run id.
    """
    results = {"ok": True, "run_type": "all"}

    try:
        v = run_vacuum()
        results["vacuum"] = v
    except Exception as e:
        logger.exception("Vacuum failed in run_all")
        results["ok"] = False
        results["vacuum_error"] = str(e)
        return results

    try:
        b = run_brain()
        results["brain"] = b
    except Exception as e:
        logger.exception("Brain failed in run_all")
        results["ok"] = False
        results["brain_error"] = str(e)
        return results

    try:
        a = run_assembly()
        results["assembly"] = a
    except Exception as e:
        logger.exception("Assembly failed in run_all")
        results["ok"] = False
        results["assembly_error"] = str(e)
        return results

    return results

