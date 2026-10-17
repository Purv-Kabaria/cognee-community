"""Turning Tally answers and submissions into document text (no network, no database).

The answer shapes follow Tally's documented webhook payload, which lists every
field type: choices, rankings and matrices hold option ids that the question's
``fields`` name, files and signatures hold objects with signed storage URLs.
"""

from fake_tally import question

from cognee_community_connector_tally.tally import (
    NODE_SET_COLUMN,
    _render_answer,
    _submission_row,
)

STATUS = question(
    "q_status",
    "Project status",
    "MULTIPLE_CHOICE",
    options={"opt-a": "Not started", "opt-b": "In progress"},
)
SPORTS = question(
    "q_sports",
    "Sports",
    "CHECKBOXES",
    options={"opt-soccer": "Soccer", "opt-swim": "Swimming", "opt-ski": "Skiing"},
)
FRUIT = question(
    "q_fruit",
    "Rank these",
    "RANKING",
    options={"opt-apple": "Apple", "opt-pear": "Pear", "opt-banana": "Banana"},
)
MATRIX = question(
    "q_matrix",
    "How do we do?",
    "MATRIX",
    options={
        "row-quality": "Quality",
        "row-speed": "Speed",
        "col-neutral": "Neutral",
        "col-happy": "Satisfied",
    },
)
FILE = question("q_file", "Screenshot", "FILE_UPLOAD")
SIGNATURE = question("q_sign", "Sign here", "SIGNATURE")
FEEDBACK = question("q_feedback", "What should we build next?", "TEXTAREA")
SCORE = question("q_score", "How likely are you to recommend us?", "LINEAR_SCALE")

STORAGE_URL = "https://storage.googleapis.com/tally-response-assets/abc/shot.png?signature=XYZ"


def _render(answer, q, formatted=None):
    response = {"questionId": q["id"], "answer": answer}
    if formatted is not None:
        response["formattedAnswer"] = formatted
    return _render_answer(response, q)


# ---------------------------------------------------------------------------
# Answers
# ---------------------------------------------------------------------------


def test_text_answer_is_kept_as_written():
    assert _render("  Dark mode, please  ", FEEDBACK) == "Dark mode, please"


def test_numbers_render_without_a_trailing_zero():
    assert _render(9, SCORE) == "9"
    assert _render(7.0, SCORE) == "7"
    assert _render(4.5, SCORE) == "4.5"


def test_booleans_render_as_yes_or_no():
    assert _render(True, FEEDBACK) == "Yes"
    assert _render(False, FEEDBACK) == "No"


def test_choice_ids_render_as_option_text():
    assert _render(["opt-b"], STATUS) == "In progress"


def test_checkboxes_list_every_ticked_option():
    assert _render(["opt-swim", "opt-ski"], SPORTS) == "Swimming, Skiing"


def test_ranking_keeps_the_respondent_order():
    assert _render(["opt-pear", "opt-apple", "opt-banana"], FRUIT) == "Pear, Apple, Banana"


def test_matrix_names_each_row_and_its_column():
    answer = {"row-quality": ["col-happy"], "row-speed": ["col-neutral"]}
    assert _render(answer, MATRIX) == "Quality: Satisfied; Speed: Neutral"


def test_an_id_the_question_does_not_name_is_kept_verbatim():
    assert _render(["opt-unknown"], STATUS) == "opt-unknown"


def test_file_upload_keeps_file_names_and_never_the_signed_url():
    answer = [
        {"id": "f1", "name": "shot.png", "url": STORAGE_URL, "mimeType": "image/png", "size": 1}
    ]
    rendered = _render(answer, FILE)
    assert rendered == "shot.png"
    assert "http" not in rendered


def test_file_upload_ignores_a_formatted_answer_that_could_hold_the_url():
    answer = [{"id": "f1", "name": "shot.png", "url": STORAGE_URL}]
    assert _render(answer, FILE, formatted=STORAGE_URL) == "shot.png"


def test_signature_is_recorded_as_signed_without_the_image_url():
    answer = [{"id": "s1", "name": "0b1c.png", "url": STORAGE_URL, "mimeType": "image/png"}]
    assert _render(answer, SIGNATURE) == "Signed"


def test_object_answers_drop_link_fields():
    payment = {
        "price": 9,
        "currency": "USD",
        "link": "https://dashboard.stripe.com/payments/pi_123",
    }
    rendered = _render(payment, question("q_pay", "Payment", "PAYMENT"))
    assert rendered == "price: 9; currency: USD"


def test_formatted_answer_wins_when_tally_provides_one():
    assert _render(1500, SCORE, formatted="$1,500.00") == "$1,500.00"


def test_blank_formatted_answer_falls_back_to_the_raw_answer():
    assert _render(1500, SCORE, formatted="  ") == "1500"


def test_empty_answers_render_as_nothing():
    assert _render(None, FEEDBACK) == ""
    assert _render([], SPORTS) == ""
    assert _render("   ", FEEDBACK) == ""


def test_input_link_answers_are_respondent_content_and_stay():
    link = question("q_site", "Website", "INPUT_LINK")
    assert _render("https://example.com", link) == "https://example.com"


# ---------------------------------------------------------------------------
# Submission -> document row
# ---------------------------------------------------------------------------

FORM = {"id": "mexJoq", "name": "Product feedback"}


def _submission(responses, completed=True):
    return {
        "id": "GG6z5L",
        "formId": "mexJoq",
        "respondentId": "jzQdR9",
        "isCompleted": completed,
        "submittedAt": "2026-10-01T09:00:00.000Z",
        "previewUrl": "https://tally.so/preview?accessToken=SECRET",
        "pdfUrl": "https://api.tally.so/pdf?accessToken=SECRET",
        "responses": responses,
    }


def test_row_carries_identity_title_and_a_form_link():
    row = _submission_row(FORM, _submission([]), [FEEDBACK])

    assert row["id"] == "tally:mexJoq:GG6z5L"
    assert row["title"] == "Product feedback (Tally submission GG6z5L)"
    assert row["url"] == "https://tally.so/forms/mexJoq/submissions"
    assert row[NODE_SET_COLUMN] == ["Product feedback"]


def test_row_content_lists_answers_in_form_order():
    responses = [
        {"questionId": "q_score", "answer": 9},
        {"questionId": "q_feedback", "answer": "Dark mode"},
    ]
    row = _submission_row(FORM, _submission(responses), [FEEDBACK, SCORE])

    assert row["content"] == (
        "Form: Product feedback\n"
        "Submitted: 2026-10-01T09:00:00.000Z\n"
        "Respondent: jzQdR9\n"
        "\n"
        "Q: What should we build next?\n"
        "A: Dark mode\n"
        "\n"
        "Q: How likely are you to recommend us?\n"
        "A: 9"
    )


def test_row_never_holds_the_signed_preview_or_pdf_links():
    row = _submission_row(FORM, _submission([{"questionId": "q_score", "answer": 9}]), [SCORE])

    flattened = repr(row)
    assert "accessToken" not in flattened
    assert "previewUrl" not in row
    assert "pdfUrl" not in row


def test_unfinished_submission_is_marked_partial():
    row = _submission_row(
        FORM, _submission([{"questionId": "q_score", "answer": 3}], completed=False), [SCORE]
    )
    assert "Status: partial (not finished)" in row["content"]


def test_blank_answers_and_unknown_questions():
    responses = [
        {"questionId": "q_feedback", "answer": "   "},
        {"questionId": "q_gone", "answer": "kept"},
    ]
    row = _submission_row(FORM, _submission(responses), [FEEDBACK])

    assert "What should we build next?" not in row["content"]
    assert "Q: Unknown question\nA: kept" in row["content"]


def test_untitled_form_still_gets_a_readable_title():
    row = _submission_row({"id": "f1", "name": ""}, _submission([]), [])
    assert row["title"] == "Untitled form (Tally submission GG6z5L)"
    assert row[NODE_SET_COLUMN] == ["Untitled form"]
