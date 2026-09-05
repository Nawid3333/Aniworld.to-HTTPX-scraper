"""Pin the lxml behaviours this scraper depends on.

These tests exist so that upgrading lxml (or any dependency that pulls a
different lxml) is a checkable change: if a new release alters how
``document_fromstring`` wraps fragments, how ``itertext()`` yields text
nodes, how ``xpath()`` reports an empty match, or how the XPath string
functions behave, one of these fails and names the exact primitive that
moved.

They are deliberately about lxml's *contract*, not about the site's markup:
the site-specific selectors are covered by ``test_scraper.py`` and the golden
fixtures. What is pinned here is the layer underneath them.

Run with:  python -m unittest discover -s tests
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


from src.scraper import (  # noqa: E402
    _check_error_page,
    _first,
    _has_class_xpath,
    _is_logged_in,
    _spaced_text,
    _stripped_text,
    make_doc,
)


class TestMakeDoc(unittest.TestCase):
    """document_fromstring must wrap fragments the way BeautifulSoup did."""

    def test_a_bare_table_fragment_is_wrapped_in_html_body(self):
        """A fragment whose outermost element is the table must still be
        searchable with .//table. lxml.html.fromstring returns a bare
        fragment root and would fail this; document_fromstring must not."""
        doc = make_doc("<table class='seasonEpisodesList'><tr></tr></table>")
        self.assertIsNotNone(doc)
        self.assertTrue(doc.xpath(".//table"), "fragment was not wrapped; .//table matched nothing")

    def test_a_full_page_parses(self):
        doc = make_doc("<html><body><h1>Title</h1></body></html>")
        self.assertIsNotNone(doc)
        self.assertEqual(_stripped_text(_first(doc, ".//h1")), "Title")

    def test_empty_body_is_none(self):
        self.assertIsNone(make_doc(""))

    def test_whitespace_only_body_is_none(self):
        self.assertIsNone(make_doc("   \n\t  "))


class TestItertext(unittest.TestCase):
    """_stripped_text and _spaced_text depend on itertext() yielding each
    text node separately, so per-node strip() works. If lxml ever merged
    adjacent text nodes, titles would glue into one word."""

    def test_stripped_text_glues_inline_markup(self):
        doc = make_doc("<strong>Hello <em> World </em></strong>")
        self.assertEqual(_stripped_text(_first(doc, ".//strong")), "HelloWorld")

    def test_spaced_text_separates_inline_markup(self):
        doc = make_doc("<h1>Harry Potter<small>Specials</small></h1>")
        self.assertEqual(_spaced_text(_first(doc, ".//h1")), "Harry Potter Specials")

    def test_stripped_text_strips_each_text_node(self):
        """itertext() yields one node per text run separated by child
        elements; each is stripped before joining. A single text run keeps
        its internal whitespace (lxml does not collapse it), which is why
        the readers rely on inline markup, not whitespace, to split titles."""
        doc = make_doc("<p>  A <em> B </em> C  </p>")
        self.assertEqual(_stripped_text(_first(doc, ".//p")), "ABC")


class TestXPathContract(unittest.TestCase):
    """_first depends on xpath() returning a list, empty when nothing matches."""

    def test_first_returns_none_on_no_match(self):
        doc = make_doc("<html><body></body></html>")
        self.assertIsNone(_first(doc, ".//h1"))

    def test_first_returns_the_first_match(self):
        doc = make_doc("<html><body><h1>A</h1><h1>B</h1></body></html>")
        self.assertEqual(_stripped_text(_first(doc, ".//h1")), "A")

    def test_xpath_returns_a_list_not_a_scalar(self):
        doc = make_doc("<html><body><h1>A</h1></body></html>")
        result = doc.xpath(".//h1")
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)


class TestClassPredicate(unittest.TestCase):
    """_has_class_xpath must match a whole class token, never a substring."""

    def _matches(self, class_attr: str, token: str) -> bool:
        doc = make_doc(f"<div class='{class_attr}'></div>")
        return _first(doc, f".//*[{_has_class_xpath(token)}]") is not None

    def test_exact_token_matches(self):
        self.assertTrue(self._matches("seen", "seen"))

    def test_substring_does_not_match(self):
        """contains(@class, 'seen') would match 'unseen'; the predicate must not."""
        self.assertFalse(self._matches("unseen", "seen"))

    def test_token_among_others_matches(self):
        self.assertTrue(self._matches("foo seen bar", "seen"))

    def test_whitespace_insensitive(self):
        self.assertTrue(self._matches("  seen  ", "seen"))


class TestLoginAndErrorDetection(unittest.TestCase):
    """The login and error readers are thin wrappers over xpath(); pin their
    lxml-level behaviour so a parser change cannot silently flip them."""

    def test_logged_in_marker_detected(self):
        doc = make_doc("<div class='avatar'><a href='/user/profil/me'></a></div>")
        self.assertTrue(_is_logged_in(doc))

    def test_logged_out_page_is_not_logged_in(self):
        doc = make_doc("<div class='avatar'><a href='/login'></a></div>")
        self.assertFalse(_is_logged_in(doc))

    def test_error_page_detected_from_title(self):
        self.assertEqual(_check_error_page(make_doc("<title>404 Not Found</title>")), "404")

    def test_real_series_page_is_not_an_error(self):
        doc = make_doc("<div id='stream'><ul><li><a href='/staffel-1'>S1</a></li></ul></div>")
        self.assertIsNone(_check_error_page(doc))


if __name__ == "__main__":
    unittest.main()
