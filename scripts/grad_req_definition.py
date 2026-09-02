"""
The school's own graduation requirement model: 25 slots totalling 24.0 credits.

This is NOT what CPS exports. CPS reports at BUCKET level -- one "Science"
figure of 3 credits -- while the school tracks the individual slots those
credits must fill: Biology, Chemistry and Physics specifically.

Reconciled against the CPS export on 2026-09-02: every bucket matches CPS's
stated Required figure exactly, and the slot total of 24.0 matches CPS's
"Total Required Credits" of 24.000. Zero mismatches.

Four buckets need splitting; the other fifteen are already one slot each.

The two CPS buckets not covered here -- "Biology" and "Other Lab Science" -- are
N/A for every student, so CPS is not tracking them at this school.

Loaded to academics.gradReqDefinition by grad_req_definition_update.py.
"""

# (slot, cpsBucket, creditsRequired) in the order they should display.
GRAD_REQUIREMENTS = [
    ("English I",              "English I",            1.0),
    ("English II",             "English II",           1.0),
    ("English III",            "English III",          1.0),
    ("English IV",             "English IV",           1.0),

    ("Algebra",                "Algebra",              1.0),
    ("Geometry",               "Geometry",             1.0),
    ("Adv Alg/Trig",           "Adv Alg/Trig",         1.0),

    ("Science I (Biology)",    "Science",              1.0),
    ("Science II (Chemistry)", "Science",              1.0),
    ("Science III (Physics)",  "Science",              1.0),

    ("World Studies",          "World Studies",        1.0),
    ("U.S. History",           "U.S. History",         1.0),
    ("Civics",                 "Civics",               0.5),
    ("Other Social Science",   "Other Social Science", 0.5),

    ("World Language I",       "World Lang I & II",    1.0),
    ("World Language II",      "World Lang I & II",    1.0),

    ("Fine Arts I",            "Fine Arts",            1.0),
    ("Fine Arts II",           "Fine Arts",            1.0),

    ("Physical Ed I",          "Physical Ed I",        1.0),
    ("Physical Ed II",         "Physical Ed II",       1.0),

    ("Career Ed",              "Career Ed",            1.0),
    ("Computer Science",       "Computer Science",     1.0),

    ("Elective 1",             "Elective",             1.0),
    ("Elective 2",             "Elective",             1.0),
    ("Elective 3",             "Elective",             1.0),
]

TOTAL_CREDITS = 24.0

assert len(GRAD_REQUIREMENTS) == 25, "expected 25 slots"
assert abs(sum(c for _, _, c in GRAD_REQUIREMENTS) - TOTAL_CREDITS) < 0.001, \
    "slot credits must total 24.0"
