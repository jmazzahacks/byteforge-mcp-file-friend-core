"""Internal tusd termination adapter; never forwards public-client credentials."""

from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from byteforge_mcp_file_friend_models import validate_id


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str
    ) -> None:
        return None


class Tusd:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.opener = build_opener(NoRedirects())

    def terminate(self, file_id: str) -> None:
        url = self.base_url + validate_id(file_id)
        request = Request(url, method="DELETE", headers={"Tus-Resumable": "1.0.0"})
        try:
            with self.opener.open(request, timeout=30) as response:
                if response.status != 204:
                    raise RuntimeError("Unexpected tusd termination response")
        except HTTPError as error:
            error.close()
            if error.code not in (404, 410):
                raise

    def expire(self, file_id: str) -> bool:
        request = Request(
            self.base_url + validate_id(file_id),
            method="DELETE",
            headers={
                "Tus-Resumable": "1.0.0",
                "X-FF-Expiry": "1",
            },
        )
        try:
            with self.opener.open(request, timeout=30) as response:
                if response.status != 204:
                    raise RuntimeError("Unexpected tusd expiry response")
                return True
        except HTTPError as error:
            error.close()
            if error.code in (404, 410):
                return True
            if error.code == 409:
                return False
            raise
