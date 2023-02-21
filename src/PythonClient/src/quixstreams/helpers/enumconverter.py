import enum


class EnumConverter:

    @staticmethod
    def enum_to_another(enuma, enumb: enum):
        if isinstance(enuma, int):
            for name, member in enumb.__members__.items():
                if type(member.value) == int:
                    if member.value == enuma:
                        return member
                elif member.value[0] == enuma:
                    return member
            raise Exception("Can't convert {} to desired enum".format(enuma))

        for name, member in enumb.__members__.items():
            if name == enuma.name:
                return member

        for name, member in enumb.__members__.items():
            if member == enuma[0]:
                return member
        raise Exception("Can't convert to desired enum")
