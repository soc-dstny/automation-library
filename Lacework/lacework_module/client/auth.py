import datetime
from requests_ratelimiter import LimiterAdapter
import requests
from requests.auth import AuthBase


class LaceworkCredentials:
    token: str
    expiresAt: datetime

    @property
    def authorization(self) -> str:
        return f"Bearer {self.token}"

class LaceworkAuthentication(AuthBase):
    """
    Implements a Requests's authentification for Lacework API
    """

    def __init__(
        self,
        lacework_url: str,
        access_key: str,
        secret_key: str,
        default_headers: dict[str, str] | None = None,
        ratelimit_per_hour: int = 480,
    ):
        self.__lacework_url = lacework_url
        self.__access_key = access_key
        self.__secret_key = secret_key
        self.__api_credentials: LaceworkCredentials | None = None
        self.__http_session = requests.Session()

        if default_headers:
            self.__http_session.headers.update(default_headers)

        self.__http_session.mount(
            "https://",
            LimiterAdapter(
                per_hour=ratelimit_per_hour,
            ),
        )

    def get_credentials(self) -> LaceworkCredentials:
        """
        Return Lacework Credentials for the API
        """
        current_dt = datetime.datetime.utcnow()

        if self.__api_credentials is None or current_dt + datetime.timedelta(seconds=3600) >= self.__api_credentials.expiresAt:
            response = self.__http_session.post(
                url=f"https://{self.__lacework_url}.lacework.net/api/v2/access/tokens",
                headers={
                    "X-LW-UAKS": self.__secret_key,
                    "Content-Type": "application/json"
                },
                json={
                    "keyId": self.__access_key,
                    "expiryTime": 3600
                }
            )

            response.raise_for_status()

            credentials = LaceworkCredentials()

            api_credentials: dict = response.json()
            credentials.token = api_credentials["token"]
            credentials.expiresAt = datetime.datetime.strptime(api_credentials["expiresAt"], "%Y-%m-%d %H:%M:%S.%f")
            self.__api_credentials = credentials

        return self.__api_credentials

    def __call__(self, request):
        request.headers["Authorization"] = self.get_credentials().authorization
        request.headers["Content-Type"] = "application/json"
        return request
