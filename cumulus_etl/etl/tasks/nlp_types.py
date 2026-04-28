import dataclasses


@dataclasses.dataclass
class NoteStats:
    seen: int = 0  # on disk, available to us
    considered: int = 0  # and passed select-by / status filters
    with_text: int = 0  # and had text
    with_results: int = 0  # and had a response from the NLP


@dataclasses.dataclass(kw_only=True)
class NoteDetails:
    note_ref: str
    encounter_id: str
    subject_ref: str

    note_text: str
    note: dict

    orig_note_ref: str
    orig_note_text: str
    orig_note: dict
