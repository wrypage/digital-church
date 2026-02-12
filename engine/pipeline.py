from engine.vacuum import run_vacuum
from engine.brain import run_brain
from engine.assembly import run_assembly

def run_all():
    results = {}

    try:
        results["vacuum"] = run_vacuum()
    except Exception as e:
        results["vacuum_error"] = str(e)

    try:
        results["brain"] = run_brain()
    except Exception as e:
        results["brain_error"] = str(e)

    try:
        results["assembly"] = run_assembly()
    except Exception as e:
        results["assembly_error"] = str(e)

    return results
