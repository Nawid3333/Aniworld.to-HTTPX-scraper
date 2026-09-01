"""Manual demo for the custom genre picker used in option 8.

Run directly to see the picker in action without scraping:

    python tests/manual_picker_demo.py

Type part of a genre name to filter, press Tab to cycle completions,
press Enter to accept, press Esc to clear, or type 0 and Enter to go back.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main  # noqa: E402

CHOICES = {
    "all": "All genres / no filter",
    "action": "Action",
    "adventure": "Adventure",
    "comedy": "Comedy",
    "drama": "Drama",
    "fantasy": "Fantasy",
    "ger-sub": "GerSub",
    "isekai": "Isekai",
    "romance": "Romance",
    "sci-fi": "Science Fiction",
    "slice-of-life": "Slice of Life",
    "supernatural": "Supernatural",
    "thriller": "Thriller",
}

if __name__ == "__main__":
    selected = main._prompt_genre_choice(CHOICES)
    print(f"\nSelected key: {selected}")
