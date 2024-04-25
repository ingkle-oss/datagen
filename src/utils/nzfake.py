import random
import string

from faker import Faker


class NZFaker:
    fake = None
    fields = []
    str_choice: list[str]

    int_count: int
    float_count: int
    word_count: int
    text_count: int
    name_count: int
    str_count: int
    str_length: int
    str_cardinality: int

    def __init__(
        self,
        int_count=0,
        float_count=0,
        word_count=0,
        text_count=0,
        name_count=0,
        str_count=0,
        str_length=0,
        str_cardinality=0,
    ):
        self.fake = Faker(use_weighting=False)

        self.fields = (
            [f"int_{i}" for i in range(int_count)]
            + [f"float_{i}" for i in range(float_count)]
            + [f"word_{i}" for i in range(word_count)]
            + [f"text_{i}" for i in range(text_count)]
            + [f"name_{i}" for i in range(name_count)]
            + [f"str_{i}" for i in range(str_count)]
        )

        self.int_count = int_count if int_count and int_count > 0 else 0
        self.float_count = float_count if float_count and float_count > 0 else 0
        self.word_count = word_count if word_count and word_count > 0 else 0
        self.text_count = text_count if text_count and text_count > 0 else 0
        self.name_count = name_count if name_count and name_count > 0 else 0
        self.str_count = str_count if str_count and str_count > 0 else 0
        self.str_length = str_length if str_length and str_length > 0 else 0
        self.str_cardinality = (
            str_cardinality if str_cardinality and str_cardinality > 0 else 0
        )

        if self.str_cardinality > 0:
            self.str_choice = [
                self.fake.unique.pystr(max_chars=str_length)
                for _ in range(str_cardinality)
            ]

    def values(self):
        values = (
            [random.randint(-(2**31), (2**31) - 1) for _ in range(self.int_count)]
            + [random.uniform(-(2**31), (2**31) - 1) for _ in range(self.float_count)]
            + self.fake.words(self.word_count)
            + self.fake.texts(self.text_count)
            + [self.fake.name() for _ in range(self.name_count)]
        )
        if self.str_cardinality > 0:
            values += [random.choice(self.str_choice) for _ in range(self.str_count)]
        else:
            values += [
                "".join(
                    random.choice(string.ascii_letters + string.digits)
                    for _ in range(self.str_length)
                )
                for _ in range(self.str_count)
            ]
        return values
