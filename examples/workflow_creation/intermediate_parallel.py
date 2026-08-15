"""Intermediate: run independent modules in parallel and join their outputs."""

from __future__ import annotations

from maida_workflows import ExecutionContext, Module, RuntimeValue, Workflow, parallel


class CountWords(Module[str, int]):
    input_type = str
    output_type = int

    async def execute(self, value: str, ctx: ExecutionContext) -> int:
        return len(value.split())


class CountCharacters(Module[str, int]):
    input_type = str
    output_type = int

    async def execute(self, value: str, ctx: ExecutionContext) -> int:
        return len(value)


class Uppercase(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


class RenderStatistics(Module[tuple[int, int, str], dict[str, object]]):
    input_type = tuple[int, int, str]
    output_type = dict[str, object]

    async def execute(
        self,
        value: tuple[int, int, str],
        ctx: ExecutionContext,
    ) -> dict[str, object]:
        words, characters, uppercase = value
        return {
            "characters": characters,
            "uppercase": uppercase,
            "words": words,
        }


class TextStatisticsWorkflow(Workflow[str, dict[str, object]]):
    workflow_id = "onboarding-parallel"
    input_type = str
    output_type = dict[str, object]

    def __init__(self) -> None:
        self.words = CountWords()
        self.characters = CountCharacters()
        self.uppercase = Uppercase()
        self.render = RenderStatistics()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[dict[str, object]]:
        statistics = parallel(
            self.words(value),
            self.characters(value),
            self.uppercase(value),
        )
        return self.render(statistics)


workflow = TextStatisticsWorkflow()
EXAMPLE_INPUT = "Maida workflows"
EXPECTED_OUTPUT = {
    "characters": 15,
    "uppercase": "MAIDA WORKFLOWS",
    "words": 2,
}
