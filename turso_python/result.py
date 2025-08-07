from typing import Any, Dict, List, Optional


class Result:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload or {}

    def raw(self) -> Dict[str, Any]:
        return self._payload

    def rows(self) -> List[Any]:
        try:
            return (
                self._payload.get("results", [])[0]
                .get("response", {})
                .get("result", {})
                .get("rows", [])
            )
        except Exception:
            return []

    def first_value(self, default: Optional[Any] = None) -> Any:
        rows = self.rows()
        if not rows:
            return default
        r0 = rows[0]
        if isinstance(r0, dict) and "values" in r0:
            cells = r0["values"]
            if not cells:
                return default
            c0 = cells[0]
            if isinstance(c0, dict):
                return c0.get("value", default)
            return c0
        if isinstance(r0, (list, tuple)):
            return r0[0] if r0 else default
        return r0 or default

