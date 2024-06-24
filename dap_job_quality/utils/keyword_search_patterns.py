keywords = [
    {
        "dimension": "P&B",
        "subcategory": "TOTAL_PACKAGE",
        "patterns": [
            [
                {
                    "LOWER": {
                        "IN": [
                            "pension",
                            "bonus",
                            "salary",
                            "compensation",
                            "pay",
                            "per annum",
                            "overtime",
                        ]
                    }
                }
            ]
        ],
    },
    {
        "dimension": "P&B",
        "subcategory": "LEAVE",
        "patterns": [
            [{"LOWER": {"IN": ["leave", "holiday", "vacation"]}}],
            [{"LOWER": "income"}, {"LOWER": "protection"}],
        ],
    },
    {
        "dimension": "P&B",
        "subcategory": "SPONSORSHIP",
        "patterns": [
            [{"LOWER": "visa"}, {"LOWER": "sponsorship"}],
        ],
    },
    {
        "dimension": "Work-life balance",
        "subcategory": "FLEX_HOURS",
        "patterns": [
            [
                {"LOWER": "flexible"},
                {"LOWER": "working", "OP": "?"},
                {"LOWER": "hours"},
            ],
            [
                {"LOWER": "flexible"},
                {"LOWER": "working", "OP": "?"},
                {"LOWER": "arrangements"},
            ],
            [
                {"LOWER": "compressed"},
                {"IS_PUNCT": True, "OP": "?"},
                {"LOWER": "hours"},
            ],
            [
                {"LOWER": "flexible"},
                {"LOWER": "working", "OP": "?"},
                {"LOWER": "options"},
            ],
            [{"LOWER": "jobshare"}],
            [{"LOWER": "job"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "share"}],
        ],
    },
    {
        "dimension": "Work-life balance",
        "subcategory": "FLEX_LOC",
        "patterns": [[{"LOWER": {"IN": ["remote", "hybrid"]}}]],
    },
    {
        "dimension": "Employment terms",
        "subcategory": "HOURS",
        "patterns": [
            [{"LOWER": "part"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "time"}],
            [{"LOWER": "full"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "time"}],
            [
                {
                    "LOWER": {
                        "IN": [
                            "hours",
                            "shifts",
                            "Monday",
                            "Tuesday",
                            "Wednesday",
                            "Thursday",
                            "Friday",
                        ]
                    }
                }
            ],
        ],
    },
    {
        "dimension": "Employment terms",
        "subcategory": "CONTRACT",
        "patterns": [[{"LOWER": {"IN": ["permanent", "temporary"]}}]],
    },
    {
        "dimension": "Job design and nature of work",
        "subcategory": "L&D",
        "patterns": [
            [
                {
                    "LOWER": {
                        "IN": [
                            "learn",
                            "develop",
                            "train",
                            "learning",
                            "developing",
                            "development",
                            "training",
                        ]
                    }
                }
            ]
        ],
    },
    {
        "dimension": "Job design and nature of work",
        "subcategory": "CAREER",
        "patterns": [
            [{"LOWER": {"IN": ["career", "progress", "progression", "advance"]}}]
        ],
    },
    {
        "dimension": "Pay and benefits",
        "subcategory": "PERKS",
        "patterns": [
            [{"LOWER": "discounts"}],
            [{"LOWER": "medical"}, {"LOWER": "insurance"}],
            [{"LOWER": "gym"}, {"LOWER": "membership"}],
            [{"LOWER": "cycle"}, {"LOWER": "to"}, {"LOWER": "work"}],
            [{"LOWER": "health"}, {"LOWER": "insurance"}],
        ],
    },
]
