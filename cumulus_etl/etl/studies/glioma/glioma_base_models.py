from pydantic import BaseModel, Field


class SpanAugmentedMention(BaseModel):
    has_mention: bool = Field(
        False, description="Whether there is any mention of this variable in the text."
    )
    spans: list[str] = Field(
        default_factory=list, description="The text spans where this variable is mentioned."
    )
