# Generated by devtools/yamaker (pypi).

PY3_LIBRARY()

VERSION(1.4.6)

LICENSE(MIT-0)

PEERDIR(
    contrib/python/asn1crypto
)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    scramp/__init__.py
    scramp/core.py
    scramp/utils.py
)

RESOURCE_FILES(
    PREFIX contrib/python/scramp/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
