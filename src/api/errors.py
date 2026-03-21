from fastapi import HTTPException


def api_error(status_code: int, code: str, message: str, details: object | None = None) -> HTTPException:
    payload = {
        "code": code,
        "message": message,
        "details": details,
    }
    return HTTPException(status_code=status_code, detail=payload)


def validation_error(message: str, details: object | None = None) -> HTTPException:
    return api_error(400, "validation_error", message, details)


def execution_error(message: str, details: object | None = None) -> HTTPException:
    return api_error(502, "execution_error", message, details)
