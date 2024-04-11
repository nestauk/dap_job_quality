keywords = [
    {
        "dimension": "P&B",
        "subcategory": "TOTAL_PACKAGE",
        "patterns": [[{"LOWER": "pension"}], [{"LOWER": "bonus"}]],
    },
    {
        "dimension": "P&B",
        "subcategory": "LEAVE",
        "patterns": [
            [{"LOWER": {"IN": ["leave", "holidays", "vacation"]}}],
            [{"LOWER": "income"}, {"LOWER": "protection"}],
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
        ],
    },
    {
        "dimension": "Work-life balance",
        "subcategory": "P/T",
        "patterns": [
            [{"LOWER": "part"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "time"}],
            [{"LOWER": "job"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": "share"}],
            [{"LOWER": "jobshare"}],
        ],
    },
    {
        "dimension": "Work-life balance",
        "subcategory": "FLEX_LOC",
        "patterns": [[{"LOWER": {"IN": ["remote", "hybrid"]}}]],
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
]
