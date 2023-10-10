import pytest

from streamingdataframes.models.types import SlottedClass


class TestSlottedClass:
    class Woo(SlottedClass):
        __slots__ = (
            "field_a",
            "field_b",
            "field_c",
            "field_d",
            "field_e",
        )

        def __init__(self, field_a: str, field_b: int = 2, field_c: dict = None):
            self.field_a = field_a
            self.field_b = field_b
            self.field_c = field_c or {}
            self.field_d = "woo"

    def test_woo_fields_basic_slot_functionality(self):
        woo = self.Woo("a", field_c={"woop": "pie"})
        assert woo.field_a == "a"
        assert woo.field_b == 2
        assert woo.field_c == {"woop": "pie"}
        assert woo.field_d == "woo"
        with pytest.raises(AttributeError):
            assert woo.field_e
        woo.field_e = "derp"
        assert woo.field_e == "derp"
        with pytest.raises(AttributeError):
            woo.field_f = "derp"  # cannot assign non-slotted entries

    def test_woo_equals(self):
        woo_1 = self.Woo("a")
        woo_2 = self.Woo("b", field_b=3)
        assert woo_1 != woo_2

        woo_2.field_a = "a"
        woo_2.field_b = 2
        assert woo_1 == woo_2
        woo_2.field_e = "uh_oh"  # ensure never-assigned field fails equals check
        assert woo_1 != woo_2
