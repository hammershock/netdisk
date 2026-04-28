import warnings

warnings.filterwarnings(
    'ignore',
    message=r'urllib3 v2 only supports OpenSSL 1\.1\.1\+.*',
)

from .cli import main  # noqa: E402, F401
