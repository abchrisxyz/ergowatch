"""
Repository of register values and their rendered value.
"""

from collections import namedtuple

Register = namedtuple("Register", ["raw", "type", "rendered"])

_registers = [
    Register(
        raw="0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
        type="SGroupElement",
        rendered="03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
    ),
    Register(
        raw="0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
        type="Coll[SByte]",
        rendered="98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
    ),
    Register(
        raw="05a4c3edd9998877",
        type="SLong",
        rendered="261824656027858",
    ),
]


class _RegisterCatalogue:
    """
    Provides a dict maping actual register values to rendered values.
    """

    def __init__(self):
        self._raw2reg = {r.raw: r for r in _registers}
        self._ren2reg = {r.rendered: r for r in _registers}

    def from_raw(self, raw_value: str) -> Register:
        return self._raw2reg[raw_value]

    def from_rendered(self, rendered_value: str) -> Register:
        return self._ren2reg[rendered_value]


RegisterCatalogue = _RegisterCatalogue()
