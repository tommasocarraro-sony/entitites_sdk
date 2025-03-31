from entities.utils.run_monitor import HttpRunMonitor

class EventsInterface:
    """
    Exposes Pydantic validation classes, retaining their original naming.

    This interface allows consumers to access the various schemas like:
        - ValidationInterface.FileUploadRequest
        - ValidationInterface.ActionCreate
        - etc.
    """

    # Actions schemas
    HttpRunMonitor = HttpRunMonitor