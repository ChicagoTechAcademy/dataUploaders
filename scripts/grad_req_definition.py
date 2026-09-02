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

# (slot, cpsBucket, creditsRequired, gradeGenerallyMet, expectedByGrade)
#
# gradeGenerallyMet is recorded verbatim because three of them are not a single
# number: "Any", "9 (shifting to 11)" and "11 or 12".
#
# expectedByGrade is the LATEST grade by which the slot should be done, used to
# decide whether a student is behind. Deliberately the latest, not the earliest:
#   "11 or 12"          -> 12
#   "9 (shifting to 11)" -> 11, the grade it is moving to
#   "Any"                -> None, there is no deadline
# Taking the later grade means a student is only ever flagged as behind when
# they genuinely are. The opposite choice would send counsellors after students
# who are on schedule, which is worse than missing a marginal case.
GRAD_REQUIREMENTS = [
    ("English I",              "English I",            1.0, "9",                  9),
    ("English II",             "English II",           1.0, "10",                10),
    ("English III",            "English III",          1.0, "11",                11),
    ("English IV",             "English IV",           1.0, "12",                12),

    ("Algebra",                "Algebra",              1.0, "9",                  9),
    ("Geometry",               "Geometry",             1.0, "10",                10),
    ("Adv Alg/Trig",           "Adv Alg/Trig",         1.0, "11",                11),

    ("Science I (Biology)",    "Science",              1.0, "9",                  9),
    ("Science II (Chemistry)", "Science",              1.0, "10",                10),
    ("Science III (Physics)",  "Science",              1.0, "11",                11),

    ("World Studies",          "World Studies",        1.0, "11",                11),
    ("U.S. History",           "U.S. History",         1.0, "10",                10),
    ("Civics",                 "Civics",               0.5, "9 (shifting to 11)", 11),
    ("Other Social Science",   "Other Social Science", 0.5, "9 (shifting to 11)", 11),

    ("World Language I",       "World Lang I & II",    1.0, "10",                10),
    ("World Language II",      "World Lang I & II",    1.0, "11",                11),

    ("Fine Arts I",            "Fine Arts",            1.0, "9",                  9),
    ("Fine Arts II",           "Fine Arts",            1.0, "11 or 12",          12),

    ("Physical Ed I",          "Physical Ed I",        1.0, "9",                  9),
    ("Physical Ed II",         "Physical Ed II",       1.0, "10",                10),

    ("Career Ed",              "Career Ed",            1.0, "Any",             None),
    ("Computer Science",       "Computer Science",     1.0, "9",                  9),

    ("Elective 1",             "Elective",             1.0, "Any",             None),
    ("Elective 2",             "Elective",             1.0, "Any",             None),
    ("Elective 3",             "Elective",             1.0, "Any",             None),
]

TOTAL_CREDITS = 24.0

assert len(GRAD_REQUIREMENTS) == 25, "expected 25 slots"
assert abs(sum(r[2] for r in GRAD_REQUIREMENTS) - TOTAL_CREDITS) < 0.001, \
    "slot credits must total 24.0"
